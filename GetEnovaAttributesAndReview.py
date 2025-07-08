from openai import OpenAI
from dotenv import load_dotenv
import os
import pyodbc
import pandas as pd

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def analyze_energiattest(attest_tekst):
    """
    Analyze the extract of this Energy Certificate using structured output.
    Returns a dictionary with 'Innmeldt_av', 'Antall_registrerte_enheter', 'Positive_ting' and 'Forbedringspotensiale' keys.
    """
    prompt = f"""
    Jeg ønsker at du leser fra denne energiattesten og gir meg følgende informasjon.

    Attest tekst: {attest_tekst}

    Svaret skal være på dette formatet:
    Innmeldt_av: navn på den som hart laget rapporten firma eller person eller begge deler
    Antall_registrerte_enheter: antall enheter attesten gjelder som et tall
    Positive_ting: kort oppsummering av positive aspekter ved energieffektiviteten til bygget/enheten
    Forbedringspotensiale: kort oppsummering av områder som kan forbedres for bedre energieffektivitet
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini-2024-07-18",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    # Parse the response
    content = response.choices[0].message.content
    lines = content.strip().split('\n')
    
    result = {}
    current_key = None
    
    for line in lines:
        line = line.strip()
        if line.startswith('Innmeldt_av:'):
            current_key = 'Innmeldt_av'
            result[current_key] = line.replace('Innmeldt_av:', '').strip()
        elif line.startswith('Antall_registrerte_enheter:'):
            current_key = 'Antall_registrerte_enheter'
            result[current_key] = line.replace('Antall_registrerte_enheter:', '').strip()
        elif line.startswith('Positive_ting:'):
            current_key = 'Positive_ting'
            result[current_key] = line.replace('Positive_ting:', '').strip()
        elif line.startswith('Forbedringspotensiale:'):
            current_key = 'Forbedringspotensiale'
            result[current_key] = line.replace('Forbedringspotensiale:', '').strip()
        elif current_key and line:
            # Continue adding to current key if line doesn't start with a new key
            result[current_key] += ' ' + line
    
    return result

def get_energiattest_from_db(top_rows=3):
    """
    Function that returns both extracted_text and merkenummer from database
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
        
        # Return both extracted_text and merkenummer
        return df[['extracted_text', 'merkenummer']].dropna()
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def main():
    
    attest_df = get_energiattest_from_db(top_rows=3)
    
    # Test each energiattest
    for index, row in attest_df.iterrows():
        attest_tekst = row['extracted_text']
        merkenummer = row['merkenummer']
        
        result = analyze_energiattest(attest_tekst)
        print(f"\nMerke: {merkenummer}:")
        print(f"Utførende: {result['Innmeldt_av']}")
        print(f"Antall enheter: {result['Antall_registrerte_enheter']}")
        print(f"Positive aspekter: {result['Positive_ting']}")
        print(f"Forbedringspotensiale: {result['Forbedringspotensiale']}")

if __name__ == "__main__":
    main()