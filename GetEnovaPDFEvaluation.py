from openai import OpenAI
from dotenv import load_dotenv
import os
import pyodbc
import pandas as pd
import requests

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_coordinates(address, api_key):
    """
    Get latitude and longitude for a given address using Google Geocoding API
    
    Args:
        address (str): Street address to geocode
        api_key (str): Your Google Maps API key
        
    Returns:
        tuple: (latitude, longitude) or None if not found
    """
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    
    params = {
        'address': address,
        'key': api_key
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        
        if data['status'] == 'OK' and len(data['results']) > 0:
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            print(f"Geocoding failed: {data['status']}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except KeyError as e:
        print(f"Unexpected response format: {e}")
        return None

def analyze_energiattest(attest_tekst, energikarakter=None, oppvarmingskarakter=None, latitude=None, longitude=None):
    """
    Analyze the extract of this Energy Certificate using structured output.
    Returns a dictionary with 'Innmeldt_av', 'Antall_registrerte_enheter', 'Positive_ting' and 'Forbedringspotensiale' keys.
    """
    metadata_info = ""
    if energikarakter:
        metadata_info += f"Energikarakter: {energikarakter}\n"
    if oppvarmingskarakter:
        metadata_info += f"Oppvarmingskarakter: {oppvarmingskarakter}\n"
    if latitude and longitude:
        metadata_info += f"Lokasjon: {latitude}, {longitude}\n"
    
    prompt = f"""
    Jeg ønsker at du leser fra denne energiattesten og gir meg følgende informasjon.

    {metadata_info}
    Attest tekst: {attest_tekst}

    Bruk gjerne energikarakter, oppvarmingskarakter og lokasjon som kontekst i din analyse.

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
        
        # Return all relevant columns including the new Adresse column
        return df[['extracted_text', 'merkenummer', 'energikarakter', 'oppvarmingskarakter', 'adresse']].dropna()
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def main():
    # Get Google Maps API key from environment
    google_api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    
    if not google_api_key:
        print("Error: GOOGLE_MAPS_API_KEY not found in .env file")
        return
    
    attest_df = get_energiattest_from_db(top_rows=3)
    
    # Test each energiattest
    for index, row in attest_df.iterrows():
        attest_tekst = row['extracted_text']
        merkenummer = row['merkenummer']
        energikarakter = row['energikarakter']
        oppvarmingskarakter = row['oppvarmingskarakter']
        adresse = row['adresse']
        
        # Get coordinates for the address
        #print(f"\nGeocoding address: {adresse}")
        coordinates = get_coordinates(adresse, google_api_key)
        
        if coordinates:
            latitude, longitude = coordinates
            #print(f"Coordinates: {latitude}, {longitude}")
        else:
            latitude, longitude = None, None
            #print("Could not get coordinates for this address")
        
        # Analyze with all metadata including coordinates
        result = analyze_energiattest(
            attest_tekst, 
            energikarakter, 
            oppvarmingskarakter, 
            latitude, 
            longitude
        )
        
        print(f"\nEnergiAttest {merkenummer}:")
        print(f"Adresse: {adresse}")
        if latitude and longitude:
            print(f"Koordinater: {latitude}, {longitude}")
        print(f"Energikarakter: {energikarakter}")
        print(f"Oppvarmingskarakter: {oppvarmingskarakter}")
        print(f"Utførende: {result['Innmeldt_av']}")
        print(f"Antall enheter: {result['Antall_registrerte_enheter']}")
        print(f"Positive aspekter: {result['Positive_ting']}")
        print(f"Forbedringspotensiale: {result['Forbedringspotensiale']}")

if __name__ == "__main__":
    main()