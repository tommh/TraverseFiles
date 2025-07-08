import pyodbc
import pandas as pd
import uuid
import re
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Beregningsresultat:
    name: str
    value: Optional[float]
    unit: Optional[str]

@dataclass 
class Energimerkeverdier:
    title: str
    beregningsresultat: List[Beregningsresultat]

def get_energiattest_from_db(top_rows=3):
    """
    Function that returns extracted_text, merkenummer, and address from database
    """
    
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=MSI;"
        "DATABASE=Enova;"
        "Trusted_Connection=yes;"
    )
    
    try:
        # Use pandas for simplicity
        query = f"EXEC [ev_enova].[Get_Enova_ExtractedText] @TopRows = {top_rows}"
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Return all relevant columns including pdfid and adresse
        return df[['pdfid', 'extracted_text', 'merkenummer', 'energikarakter', 'oppvarmingskarakter', 'adresse']].dropna()
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def is_date(value: str) -> bool:
    """Check if value looks like a date"""
    import re
    # Match patterns like "18.06.2025", "2025-06-18"
    date_patterns = [
        r'\d{1,2}\.\d{1,2}\.\d{4}',
        r'\d{4}-\d{1,2}-\d{1,2}'
    ]
    return any(re.match(pattern, value) for pattern in date_patterns)

def is_pure_number(value: str) -> bool:
    """Check if value is just a number"""
    # Remove commas and check if it's a valid number
    cleaned = value.replace(',', '.').replace(' ', '')
    try:
        float(cleaned)
        return True
    except ValueError:
        return False

def contains_number_with_unit(value: str) -> bool:
    """Check if value contains both number and unit"""
    import re
    return bool(re.search(r'[\d,\.]+\s*[^\d\s,\.]+', value))

def extract_number_and_unit(value: str) -> tuple:
    """Extract number and unit from combined value"""
    import re
    match = re.search(r'([\d,\.]+)\s*(.+)', value)
    if match:
        try:
            number = float(match.group(1).replace(',', '.'))
            unit = match.group(2).strip()
            return number, unit
        except ValueError:
            return None, value
    return None, value

def parse_energimerkeverdier_from_text(extracted_text: str) -> Optional[Energimerkeverdier]:
    """
    Parse the Energimerkeverdier object from extracted markdown text
    """
    try:
        results = []
        
        # Split text into lines for processing
        lines = extracted_text.split('\n')
        
        # Look for markdown tables (lines with |)
        for line in lines:
            line = line.strip()
            
            # Skip table separator lines (like |---|---|)
            if '---' in line and '|' in line:
                continue
                
            # Check if this is a table row
            if '|' in line and not line.startswith('<!--'):
                # Clean up the line and split by |
                parts = [part.strip() for part in line.split('|') if part.strip()]
                
                if len(parts) >= 2:
                    field_name = parts[0]
                    field_value = parts[1] if len(parts) > 1 else ""
                    
                    # Skip header rows and empty values
                    if (field_name.lower() in ['attesten gjelder', 'enhet', 'adresse'] or 
                        not field_value or field_value == '-'):
                        continue
                    
                    # Try to parse numeric values
                    numeric_value = None
                    unit = None
                    
                    # Check for different value types
                    if field_value == '-':
                        # Handle dash as None
                        numeric_value = None
                        unit = None
                    elif is_date(field_value):
                        # Handle dates - store as text in unit field
                        numeric_value = None
                        unit = field_value
                    elif is_pure_number(field_value):
                        # Pure number like "34", "5538", "36.5"
                        try:
                            numeric_value = float(field_value.replace(',', '.'))
                            unit = None
                        except ValueError:
                            unit = field_value
                    elif contains_number_with_unit(field_value):
                        # Values like "0,18 W/(m²·K)", "3855.0 m²"
                        numeric_value, unit = extract_number_and_unit(field_value)
                    else:
                        # Text values like "HAUGESUND", "Energiattest-2025-136911"
                        numeric_value = None
                        unit = field_value
                    
                    # Create result object
                    result = Beregningsresultat(
                        name=field_name,
                        value=numeric_value,
                        unit=unit
                    )
                    results.append(result)
                    
                    print(f"Parsed: {field_name} = {numeric_value} {unit}")
        
        if results:
            print(f"Successfully parsed {len(results)} fields")
            return Energimerkeverdier(
                title="Energiattest",
                beregningsresultat=results
            )
        else:
            print("No valid table data found in extracted text")
            return None
            
    except Exception as e:
        print(f"Error parsing markdown text: {e}")
        import traceback
        traceback.print_exc()
        return None

def insert_energimerkeverdier_keyvalue(
    pdf_id: int,
    data: Energimerkeverdier, 
    merkenummer: str,
    adresse: str,
    connection_string: str
):
    """Insert energy certificate data using flexible key-value approach with pdfid"""
    
    record_id = str(uuid.uuid4())  # Group all fields from this record
    
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables t 
                          JOIN sys.schemas s ON t.schema_id = s.schema_id 
                          WHERE s.name = 'ev_enova' AND t.name = 'Energimerkeverdier')
            CREATE TABLE ev_enova.Energimerkeverdier (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                PdfId INT NOT NULL,
                RecordID UNIQUEIDENTIFIER DEFAULT NEWID(),
                Title NVARCHAR(255),
                FieldName NVARCHAR(500),
                FieldValue NVARCHAR(500),
                Unit NVARCHAR(100),
                ValueAsNumber DECIMAL(18,4),
                Merkenummer NVARCHAR(255),
                Adresse NVARCHAR(500),
                CreatedDate DATETIME2 DEFAULT GETDATE()
            );
            
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_Energimerkeverdier_PdfId' 
                          AND object_id = OBJECT_ID('ev_enova.Energimerkeverdier'))
                CREATE INDEX IX_Energimerkeverdier_PdfId ON ev_enova.Energimerkeverdier (PdfId);
            
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_Energimerkeverdier_RecordID'
                          AND object_id = OBJECT_ID('ev_enova.Energimerkeverdier'))
                CREATE INDEX IX_Energimerkeverdier_RecordID ON ev_enova.Energimerkeverdier (RecordID);
        """)
        
        # Insert data
        for result in data.beregningsresultat:
            # Try to parse numeric value
            numeric_value = None
            if result.value is not None:
                try:
                    numeric_value = float(result.value)
                except (ValueError, TypeError):
                    pass
            
            cursor.execute("""
                INSERT INTO ev_enova.Energimerkeverdier 
                (PdfId, RecordID, Title, FieldName, FieldValue, Unit, ValueAsNumber, Merkenummer, Adresse)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pdf_id,
                record_id,
                data.title,
                result.name,
                str(result.value) if result.value is not None else None,
                result.unit,
                numeric_value,
                merkenummer,
                adresse
            ))
        
        conn.commit()
        print(f"Inserted {len(data.beregningsresultat)} records for PdfId: {pdf_id}, RecordID: {record_id}")

def process_energiattest_batch(top_rows=10):
    """
    Main function to process energy certificates from database
    """
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=MSI;"
        "DATABASE=Enova;"
        "Trusted_Connection=yes;"
    )
    
    # Get data from database
    df = get_energiattest_from_db(top_rows)
    
    if df.empty:
        print("No data retrieved from database")
        return
    
    processed_count = 0
    error_count = 0
    
    for _, row in df.iterrows():
        try:
            pdf_id = row['pdfid']
            extracted_text = row['extracted_text']
            merkenummer = row['merkenummer']
            adresse = row['adresse']
            
            print(f"Processing PdfId: {pdf_id}, Merkenummer: {merkenummer}")
            
            # Parse the extracted text to get Energimerkeverdier object
            energy_data = parse_energimerkeverdier_from_text(extracted_text)
            
            if energy_data:
                # Insert into database
                insert_energimerkeverdier_keyvalue(
                    pdf_id=pdf_id,
                    data=energy_data,
                    merkenummer=merkenummer,
                    adresse=adresse,
                    connection_string=conn_str
                )
                processed_count += 1
            else:
                print(f"Could not parse energy data for PdfId: {pdf_id}")
                error_count += 1
                
        except Exception as e:
            print(f"Error processing PdfId {row.get('pdfid', 'unknown')}: {e}")
            error_count += 1
    
    print(f"Processing complete. Processed: {processed_count}, Errors: {error_count}")

# Example usage:
# process_energiattest_batch(top_rows=5)

def main():
    """
    Main function to run the energy certificate processing
    """
    try:
        print("Starting energy certificate processing...")
        
        # Process a single record first for testing
        process_energiattest_batch(top_rows=1)
        
        print("\nProcessing completed successfully!")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

def insert_energy_certificate_normalized(data: Energimerkeverdier, connection_string: str):
    """Insert data using normalized approach"""
    
    # Create a dictionary for easier lookup
    results_dict = {result.name: result for result in data.beregningsresultat}
    
    def get_value(name: str, default=None):
        result = results_dict.get(name)
        return result.value if result and result.value is not None else default
    
    def get_unit(name: str, default=None):
        result = results_dict.get(name)
        return result.unit if result and result.unit is not None else default
    
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist (abbreviated for space)
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables t 
                          JOIN sys.schemas s ON t.schema_id = s.schema_id 
                          WHERE s.name = 'ev_enova' AND t.name = 'EnergiAttest')
            CREATE TABLE ev_enova.EnergiAttest (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                Title NVARCHAR(255),
                AntallRegistrerteEnheter INT,
                Postnummer INT,
                Sted NVARCHAR(100),
                Kommunenavn NVARCHAR(100),
                Gardsnummer INT,
                Bruksnummer INT,
                Seksjonsnummer INT,
                Bygningsnummer BIGINT,
                Merkenummer NVARCHAR(100),
                Dato DATE,
                InnmeldtAv NVARCHAR(255),
                MaltEnergibruk NVARCHAR(100),
                GodeEnergivaner NVARCHAR(255),
                Bygningskategori NVARCHAR(100),
                Bygningstype NVARCHAR(100),
                Byggeaar INT,
                BRA DECIMAL(10,2),
                BRAUnit NVARCHAR(10),
                UVerdiYttervegger DECIMAL(5,2),
                UVerdiYtterveggUnit NVARCHAR(20),
                CreatedDate DATETIME2 DEFAULT GETDATE()
            )
        """)
        
        # Insert data
        cursor.execute("""
            INSERT INTO ev_enova.EnergiAttest (
                Title, AntallRegistrerteEnheter, Postnummer, Sted, Kommunenavn,
                Gardsnummer, Bruksnummer, Seksjonsnummer, Bygningsnummer,
                Merkenummer, InnmeldtAv, MaltEnergibruk, GodeEnergivaner,
                Bygningskategori, Bygningstype, Byggeaar, BRA, BRAUnit,
                UVerdiYttervegger, UVerdiYtterveggUnit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.title,
            int(get_value('Antall registrerte enheter', 0)),
            int(get_value('Postnummer', 0)),
            get_unit('Sted', ''),
            get_unit('Kommunenavn', ''),
            int(get_value('Gårdsnummer', 0)),
            int(get_value('Bruksnummer', 0)),
            int(get_value('Seksjonsnummer', 0)),
            int(get_value('Bygningsnummer', 0)),
            get_unit('Merkenummer', ''),
            get_unit('Innmeldt av', ''),
            get_unit('Målt energibruk', ''),
            get_unit('Gode energivaner', ''),
            get_unit('Bygningskategori', ''),
            get_unit('Bygningstype', ''),
            int(get_value('Byggeår', 0)),
            float(get_value('BRA', 0.0)),
            'm²',
            float(get_value('U-verdi for yttervegger', 0.0)),
            'W/(m²·K)'
        ))
        
        conn.commit()
        print("Inserted normalized record")

# Example usage:
# connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_user;PWD=your_password"
# insert_energimerkeverdier_keyvalue(your_data, connection_string)
# insert_energy_certificate_normalized(your_data, connection_string)