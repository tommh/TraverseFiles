import pyodbc
import requests
import time
from requests.adapters import HTTPAdapter
from datetime import datetime
from urllib3.util.retry import Retry

start = time.perf_counter()
insert_count = 0
api_call_count = 0
log_count = 0

batch_datetime = datetime.now()

conn_str = (
             "DRIVER={ODBC Driver 17 for SQL Server};"
             "SERVER=localhost,1433;"
             "DATABASE=Enova;"
             "Trusted_Connection=yes;"
         )
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Configure session with retry strategy
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Hent rader fra input-tabell
cursor.execute("{CALL ev_enova.Get_Enova_API_Parameters (?)}", 51000)
rows = cursor.fetchall()

print(f"Retrieved {len(rows)} rows from stored procedure")

url = "https://api.data.enova.no/ems/offentlige-data/v1/Energiattest"
headers = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "x-api-key": "f36a1754f10f47b487892998d48c47ff"
}

# Rate limiting configuration
REQUESTS_PER_SECOND = 2  # Adjust based on API limits
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND

for i, row in enumerate(rows): 
    payload = {
        k: str(v) if isinstance(v, int) or v is not None else v
        for k, v in {
            "kommunenummer": row.kommunenummer,
            "gardsnummer": row.gardsnummer,
            "bruksnummer": row.bruksnummer,
            "seksjonsnummer": row.seksjonsnummer,
            "bruksenhetnummer": row.bruksenhetnummer,
            "bygningsnummer": row.bygningsnummer
        }.items()
        if v not in (None, "", " ")
    }

    # Store the parameters for database insertion
    param_kommunenummer = payload.get("kommunenummer", None)
    param_Gardsnummer = payload.get("gardsnummer", None)
    param_Bruksnummer = payload.get("bruksnummer", None)
    param_Seksjonsnummer = payload.get("seksjonsnummer", None)
    param_Bruksenhetnummer = payload.get("bruksenhetnummer", None)
    param_Bygningsnummer = payload.get("bygningsnummer", None)

    try:
        # Add delay before API call (except for first request)
        if api_call_count > 0:
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        r = session.post(url, json=payload, headers=headers, timeout=30)
        api_call_count += 1
        
        # Handle rate limiting
        if r.status_code == 429:
            print(f"Rate limited on request {i+1}, waiting 60 seconds...")
            time.sleep(60)
            r = session.post(url, json=payload, headers=headers, timeout=30)
            api_call_count += 1
        
        if r.status_code != 200:
            print(f"Request {i+1} failed with status {r.status_code}")
            # Log the failed request
            try:
                cursor.execute("""
                    INSERT INTO [ev_enova].[EnovaApi_Energiattest_url_log] 
                    (ImpHist_ID, LogDate, kommunenummer, gardsnummer, bruksnummer, 
                     seksjonsnummer, bruksenhetnummer, bygningsnummer, records_returned, status_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.imphist_id,
                    batch_datetime,
                    row.kommunenummer,
                    row.gardsnummer, 
                    row.bruksnummer,
                    row.seksjonsnummer,
                    row.bruksenhetnummer,
                    row.bygningsnummer,
                    0,
                    f"HTTP Error {r.status_code}"
                ))
                conn.commit()
                log_count += 1
                print(f"Logged failed request for ImpHist_ID {row.imphist_id}")
            except Exception as log_error:
                print(f"Error logging failed request for ImpHist_ID {row.imphist_id}: {log_error}")
            continue

        data = r.json()
        records_returned = len(data)
        for d in data:
            # Extract energiattest data
            attestnummer = d["energiattest"]["attestnummer"]
            attest_url = d["energiattest"]["attestUrl"]
            filename = attest_url.split("/")[-1].split(".pdf")[0]
            energikarakter = str(d["energiattest"]["energikarakter"]) if d["energiattest"]["energikarakter"] is not None else None
            oppvarmingskarakter = str(d["energiattest"]["oppvarmingskarakter"]) if d["energiattest"]["oppvarmingskarakter"] is not None else None
            utstedelsesdato = d["energiattest"].get("utstedelsesdato")
            
            # Extract enhet data
            bruksareal = d["enhet"]["bruksareal"]
            
            # Extract adresse data
            adresse = d["enhet"].get("adresse", {})
            adresse_gatenavn = adresse.get("gatenavn")
            adresse_postnummer = adresse.get("postnummer")
            adresse_poststed = adresse.get("poststed")
            
            # Extract registering data
            registering = d["energiattest"].get("registering", {})
            registering_type = registering.get("type")
            beregnet_levert_energi_totalt_kwh_m2 = registering.get("beregnetLevertEnergiTotaltkWhm2")
            beregnet_levert_energi_totalt_kwh = registering.get("beregnetLevertEnergiTotaltkWh")
            har_energivurdering = str(registering.get("harEnergivurdering")) if registering.get("harEnergivurdering") is not None else None
            energivurdering_dato = registering.get("energivurderingdato")
            beregnet_fossilandel = registering.get("beregnetFossilandel")
            materialvalg = registering.get("materialvalg")
            
            # Extract organisasjonsnummer
            organisasjonsnummer = d.get("organisasjonsnummer")
            
            # Extract matrikkel data
            matrikkel = d["enhet"].get("matrikkel", {})
            matrikkel_kommunenummer = matrikkel.get("kommunenummer")
            matrikkel_gardsnummer = matrikkel.get("gårdsnummer")
            matrikkel_bruksnummer = matrikkel.get("bruksnummer")
            matrikkel_festenummer = matrikkel.get("festenummer")
            matrikkel_seksjonsnummer = matrikkel.get("seksjonsnummer")
            matrikkel_andelsnummer = matrikkel.get("andelsnummer")
            matrikkel_bruksenhetsnummer = matrikkel.get("bruksenhetsnummer")
            
            # Extract bygg data
            bygg = d["enhet"].get("bygg", {})
            bygg_bygningsnummer = bygg.get("bygningsnummer")
            bygg_byggear = str(bygg.get("byggeår")) if bygg.get("byggeår") is not None else None
            bygg_kategori = bygg.get("kategori")
            bygg_type = bygg.get("type")
            
            # Get imphist_id from original row
            imphist_id = row.imphist_id

            # Insert all data into database
            cursor.execute("""
                INSERT INTO [ev_enova].[EnovaApi_Energiattest_url] (
                    ImportDate, ImpHist_ID, paramKommunenummer, paramGardsnummer, 
                    paramBruksnummer, paramSeksjonsnummer, paramBruksenhetnummer, 
                    paramBygningsnummer, attestnummer, merkenummer, bruksareal, 
                    energikarakter, oppvarmingskarakter, attest_url, 
                    matrikkel_kommunenummer, matrikkel_gardsnummer, matrikkel_bruksnummer,
                    matrikkel_festenummer, matrikkel_seksjonsnummer, matrikkel_andelsnummer,
                    matrikkel_bruksenhetsnummer, bygg_bygningsnummer, bygg_byggear,
                    bygg_kategori, bygg_type, utstedelsesdato,
                    adresse_gatenavn, adresse_postnummer, adresse_poststed,
                    registering_RegisteringType, registering_BeregnetLevertEnergiTotaltkWhm2,
                    registering_BeregnetLevertEnergiTotaltkWh, registering_HarEnergivurdering,
                    registering_Energivurderingdato, registering_BeregnetFossilandel,
                    registering_Materialvalg, OrganisasjonsNummer
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                batch_datetime, imphist_id, param_kommunenummer, param_Gardsnummer, param_Bruksnummer, 
                param_Seksjonsnummer, param_Bruksenhetnummer, param_Bygningsnummer,
                attestnummer, filename, bruksareal, energikarakter, oppvarmingskarakter, 
                attest_url, matrikkel_kommunenummer, matrikkel_gardsnummer, 
                matrikkel_bruksnummer, matrikkel_festenummer, matrikkel_seksjonsnummer,
                matrikkel_andelsnummer, matrikkel_bruksenhetsnummer, bygg_bygningsnummer,
                bygg_byggear, bygg_kategori, bygg_type, utstedelsesdato,
                adresse_gatenavn, adresse_postnummer, adresse_poststed,
                registering_type, beregnet_levert_energi_totalt_kwh_m2,
                beregnet_levert_energi_totalt_kwh, har_energivurdering,
                energivurdering_dato, beregnet_fossilandel, materialvalg,
                organisasjonsnummer
            ))

            conn.commit()
            insert_count += 1

        # Log the request after processing (successful or empty result)
        try:
            status_message = "Success" if records_returned > 0 else "No records found"
            cursor.execute("""
                INSERT INTO [ev_enova].[EnovaApi_Energiattest_url_log] 
                (ImpHist_ID, LogDate, kommunenummer, gardsnummer, bruksnummer, 
                 seksjonsnummer, bruksenhetnummer, bygningsnummer, records_returned, status_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.imphist_id,
                batch_datetime,
                row.kommunenummer,
                row.gardsnummer, 
                row.bruksnummer,
                row.seksjonsnummer,
                row.bruksenhetnummer,
                row.bygningsnummer,
                records_returned,
                status_message
            ))
            conn.commit()
            log_count += 1
        except Exception as log_error:
            print(f"Error logging request for ImpHist_ID {row.imphist_id}: {log_error}")

        # Progress reporting
        if (i + 1) % 10 == 0:
            print(f"Processed {i + 1}/{len(rows)} requests, {insert_count} records inserted, {log_count} logged")

    except requests.exceptions.RequestException as e:
        print(f"Request error on row {i+1}: {e}")
        # Log the failed request
        try:
            cursor.execute("""
                INSERT INTO [ev_enova].[EnovaApi_Energiattest_url_log] 
                (ImpHist_ID, LogDate, kommunenummer, gardsnummer, bruksnummer, 
                 seksjonsnummer, bruksenhetnummer, bygningsnummer, records_returned, status_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.imphist_id,
                batch_datetime,
                row.kommunenummer,
                row.gardsnummer, 
                row.bruksnummer,
                row.seksjonsnummer,
                row.bruksenhetnummer,
                row.bygningsnummer,
                0,
                f"Request Exception: {str(e)[:100]}"  # Truncate long error messages
            ))
            conn.commit()
            log_count += 1
        except Exception as log_error:
            print(f"Error logging failed request for ImpHist_ID {row.imphist_id}: {log_error}")
    except Exception as e:
        print(f"General error on row {i+1}: {e}")
        # Log the failed request
        try:
            cursor.execute("""
                INSERT INTO [ev_enova].[EnovaApi_Energiattest_url_log] 
                (ImpHist_ID, LogDate, kommunenummer, gardsnummer, bruksnummer, 
                 seksjonsnummer, bruksenhetnummer, bygningsnummer, records_returned, status_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.imphist_id,
                batch_datetime,
                row.kommunenummer,
                row.gardsnummer, 
                row.bruksnummer,
                row.seksjonsnummer,
                row.bruksenhetnummer,
                row.bygningsnummer,
                0,
                f"General Exception: {str(e)[:100]}"  # Truncate long error messages
            ))
            conn.commit()
            log_count += 1
        except Exception as log_error:
            print(f"Error logging failed request for ImpHist_ID {row.imphist_id}: {log_error}")

cursor.close()
conn.close()

end = time.perf_counter()
total_time = end - start
avg_time = total_time / insert_count if insert_count else 0

print(f"\n=== Summary ===")
print(f"API calls made: {api_call_count}")
print(f"Records inserted: {insert_count}")
print(f"Records logged: {log_count}")
print(f"Total time: {total_time:.3f} sec")
print(f"Average per insert: {avg_time:.4f} sec")
print(f"Average per API call: {total_time/api_call_count:.4f} sec" if api_call_count else "N/A")