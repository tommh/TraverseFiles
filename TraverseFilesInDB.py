import pyodbc
import pandas as pd

def connect_to_sql_server():
    """
    Connect to local SQL Server and read a column from a table
    """
    try:
        # Connection string for SQL Server
        # Option 1: Using Windows Authentication

        connection_string = (
             "DRIVER={ODBC Driver 17 for SQL Server};"
             "SERVER=TH;"
             "DATABASE=Enova;"
             "Trusted_Connection=yes;"
         )
        
        # Establish connection
        conn = pyodbc.connect(connection_string)
        print("✅ Connected to SQL Server successfully!")
        
        # Create cursor
        cursor = conn.cursor()
        
        # Query to read a specific column
        query = """
        SELECT  [filename]
        FROM [Enova].[ev_enova].[EnovaApi_Energiattest_PDF]
        WHERE title = 'Energiattest for flerboligbygg'
        """
        
        # Execute query
        cursor.execute(query)
        
        # Fetch all results
        results = cursor.fetchall()
        
        # Print results
        print(f"\nData from column:")
        print("-" * 30)
        for row in results:
            print(row[0])  # Print the first (and only) column
        
        # Close connection
        cursor.close()
        conn.close()
        print("\n✅ Connection closed successfully!")
        
    except pyodbc.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

def read_with_pandas():
    """
    Alternative method using pandas (easier for data analysis)
    """
    try:
        # Connection string
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=your_database_name;"
            "Trusted_Connection=yes;"
        )
        
        # SQL query
        query = """
        SELECT column_name 
        FROM your_table_name
        """
        
        # Read data directly into DataFrame
        df = pd.read_sql(query, connection_string)
        
        # Display results
        print("📊 Data using pandas:")
        print(df)
        
        # Access specific column
        column_values = df['column_name'].tolist()
        print(f"\nColumn values as list: {column_values}")
        
    except Exception as e:
        print(f"❌ Error with pandas: {e}")

if __name__ == "__main__":
    print("🔗 Connecting to SQL Server...")
    
    # Method 1: Using pyodbc directly
    connect_to_sql_server()
    
    print("\n" + "="*50)
    
    # Method 2: Using pandas (uncomment to use)
    # read_with_pandas()