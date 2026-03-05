"""
ETL-Prozess: Microsoft SQL Server (Business-DB → Data Warehouse)
----------------------------------------------------------------
Dieses Skript überträgt inkrementell Reservierungsdaten aus einer operativen
Parkhaus-Datenbank (OLTP) in ein Data-Warehouse (DWH) im Star-Schema-Format.

Ablauf:
1. Ermittlung des zuletzt geladenen Datensatzes aus `etl_control`
2. Extraktion neuer Reservierungen und verknüpfter Dimensionseinträge
3. Aufbau der Zeit-Dimension (Dim_Zeit)
4. Upsert (MERGE) der Dimensionen in das DWH
5. Insert neuer Fakt-Datensätze (Fakt_Reservierungen)
6. Aktualisierung der ETL-Steuerungstabelle
"""
import datetime
import logging

import pandas as pd
from sqlalchemy import create_engine, text

# =====================================================
# KONFIGURATION
# =====================================================
SOURCE_DB_URL = "mssql+pyodbc://python:kurwa123@172.17.16.1,1433/ParkhausDB?driver=ODBC+Driver+18+for+SQL+Server"
DWH_DB_URL    = "mssql+pyodbc://python:kurwa123@172.17.16.1,1433/ParkhausDWH?driver=ODBC+Driver+18+for+SQL+Server"
ETL_CONTROL_TABLE = "etl_control"

BUSINESS_YEAR = 2025  # Jahr der Geschäftsdaten (für Dim_Zeit)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_sqlserver")


# =====================================================
# DATENBANK-HILFSFUNKTIONEN
# =====================================================
def get_engine(url):
    """
    Erstellt eine SQLAlchemy-Engine für die Verbindung zu einer SQL Server-Datenbank.

    Args:
        url (str): Connection-String (z. B. mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server)

    Returns:
        sqlalchemy.Engine: Eine Engine-Instanz zur Ausführung von SQL-Abfragen.
    """
    return create_engine(url, fast_executemany=True)


def ensure_etl_control_table(engine):
    """
    Erstellt die ETL-Steuerungstabelle (`etl_control`), falls sie noch nicht existiert.

    Diese Tabelle dient der Nachverfolgung, bis zu welcher Reservierungs-ID
    bereits Daten ins DWH geladen wurden.

    Struktur:
        job_name (PK), last_reservierungs_id, last_run

    Args:
        engine (sqlalchemy.Engine): Datenbankverbindung zum DWH.
    """
    sql = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{ETL_CONTROL_TABLE}' AND xtype='U')
    CREATE TABLE {ETL_CONTROL_TABLE} (
        job_name NVARCHAR(100) PRIMARY KEY,
        last_reservierungs_id BIGINT,
        last_run DATETIME
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("ETL control table checked/created.")


def get_last_loaded(engine, job="reservations_load"):
    """
    Ermittelt die zuletzt geladene Reservierungs-ID für den angegebenen ETL-Job.

    Args:
        engine (sqlalchemy.Engine): DWH-Engine-Verbindung.
        job (str): Name des ETL-Jobs (Standard: "reservations_load").

    Returns:
        int: Die zuletzt geladene Reservierungs-ID oder 0, falls kein Eintrag existiert.
    """
    ensure_etl_control_table(engine)
    sql = f"SELECT last_reservierungs_id FROM {ETL_CONTROL_TABLE} WHERE job_name=:job"
    with engine.connect() as conn:
        row = conn.execute(text(sql), {"job": job}).fetchone()
        return int(row[0]) if row and row[0] else 0


def update_last_loaded(engine, last_id, job="reservations_load"):
    """
    Aktualisiert den Fortschritt des ETL-Jobs in der Steuerungstabelle.

    Args:
        engine (sqlalchemy.Engine): DWH-Verbindung.
        last_id (int): Höchste Reservierungs-ID, die geladen wurde.
        job (str): Name des ETL-Jobs.
    """
    sql = f"""
    MERGE {ETL_CONTROL_TABLE} AS tgt
    USING (SELECT :job AS job_name, :last_id AS last_reservierungs_id, :now AS last_run) AS src
    ON tgt.job_name = src.job_name
    WHEN MATCHED THEN
        UPDATE SET tgt.last_reservierungs_id = src.last_reservierungs_id, tgt.last_run = src.last_run
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_reservierungs_id, last_run)
        VALUES (src.job_name, src.last_reservierungs_id, src.last_run);
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"job": job, "last_id": int(last_id), "now": datetime.datetime.now()})
    logger.info(f"Updated ETL control to {last_id}")


# =====================================================
# EXTRAKTION
# =====================================================
def extract_new_reservations(src_engine, since_id):
    """
    Extrahiert alle neuen Reservierungen aus der operativen Datenbank.

    Args:
        src_engine (sqlalchemy.Engine): Verbindung zur Business-Datenbank.
        since_id (int): Die letzte bereits geladene Reservierungs-ID.

    Returns:
        pandas.DataFrame: Alle Reservierungen mit ID > since_id.
    """
    sql = text("""
        SELECT ReservierungsID, FahrzeugID, KundeNr, ZahlungsmethodenID AS ZahlungsmethodeID,
               ParkplatzID, Startdatum, Enddatum, Dauer, Gesamtpreis
        FROM Reservierungen
        WHERE ReservierungsID > :since
        ORDER BY ReservierungsID
    """)
    return pd.read_sql(sql, src_engine, params={"since": since_id})


def extract_table(src_engine, table, id_col, ids):
    """
    Extrahiert Datensätze aus einer angegebenen Tabelle anhand einer ID-Liste.

    Args:
        src_engine (sqlalchemy.Engine): Verbindung zur Quell-Datenbank.
        table (str): Tabellenname.
        id_col (str): Spaltenname der ID.
        ids (list[int]): Liste der IDs, die extrahiert werden sollen.

    Returns:
        pandas.DataFrame: Teilmenge der Tabelle mit den angeforderten IDs.
    """
    if not ids:
        return pd.DataFrame()
    sql = text(f"SELECT * FROM {table} WHERE {id_col} IN :ids")
    return pd.read_sql(sql, src_engine, params={"ids": tuple(ids)})


# =====================================================
# LADEN / UPSERT
# =====================================================
def upsert_sqlserver(engine, df, table_name, key_cols):
    """
    Führt ein Upsert (MERGE INTO) in SQL Server durch.

    Existierende Datensätze werden aktualisiert, neue hinzugefügt.

    Args:
        engine (sqlalchemy.Engine): Ziel-DWH-Verbindung.
        df (pandas.DataFrame): Zu ladende Daten.
        table_name (str): Ziel-Tabelle im DWH.
        key_cols (list[str]): Primärschlüsselspalten zur Duplikaterkennung.
    """
    if df.empty:
        logger.info(f"No rows to upsert into {table_name}")
        return

    temp_table = f"#{table_name}_tmp"
    with engine.begin() as conn:
        # Temporäre Tabelle mit neuen Daten
        df.to_sql(temp_table.replace("#", ""), conn, if_exists="replace", index=False)
        
        cols = list(df.columns)
        updates = ", ".join([f"t.[{c}] = s.[{c}]" for c in cols if c not in key_cols])
        insert_cols = ", ".join(f"[{c}]" for c in cols)
        insert_vals = ", ".join(f"s.[{c}]" for c in cols)
        join_cond = " AND ".join([f"t.[{k}] = s.[{k}]" for k in key_cols])

        merge_sql = f"""
        MERGE {table_name} AS t
        USING {temp_table} AS s
        ON {join_cond}
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """
        conn.execute(text(merge_sql))
    logger.info(f"Upserted {len(df)} rows into {table_name}")


# =====================================================
# TRANSFORMATION
# =====================================================
def generate_dim_zeit(year: int) -> pd.DataFrame:
    """
    Erzeugt eine Zeitdimension (Dim_Zeit) für das angegebene Jahr.

    Args:
        year (int): Das Jahr, für das Datumswerte generiert werden sollen.

    Returns:
        pandas.DataFrame: Enthält Datum, Tag, Woche, Monat, Jahr.
    """
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({
        "Datum": dates.date,
        "Tag": dates.day,
        "Woche": dates.isocalendar().week,
        "Monat": dates.month,
        "Jahr": dates.year
    })

    return df


# =====================================================
# HAUPT-ETL-Ablauf
# =====================================================
def run_etl():
    """
    Führt den vollständigen ETL-Prozess von der Business-Datenbank ins DWH aus.

    Schritte:
        1. Ermittlung der zuletzt geladenen Reservierungs-ID
        2. Extraktion neuer Reservierungen
        3. Extraktion verknüpfter Dimensionstabellen
        4. Aufbau der Dim_Zeit
        5. MERGE-Upserts der Dimensionen
        6. Insert neuer Fakt-Daten
        7. Fortschrittsaktualisierung in etl_control
    """
    src_engine = get_engine(SOURCE_DB_URL)
    dwh_engine = get_engine(DWH_DB_URL)

    last_id = get_last_loaded(dwh_engine)
    logger.info(f"Last loaded ReservierungsID: {last_id}")

    res = extract_new_reservations(src_engine, last_id)
    if res.empty:
        logger.info("Keine neuen Reservierungen.")
        return

    # === Dimensionen extrahieren ===
    kunden_ids = res["KundeNr"].dropna().unique()
    fahrzeug_ids = res["FahrzeugID"].dropna().unique()
    zahl_ids = res["ZahlungsmethodeID"].dropna().unique()
    park_ids = res["ParkplatzID"].dropna().unique()

    kunden = extract_table(src_engine, "Kunde", "KundeNr", kunden_ids)
    fahrzeuge = extract_table(src_engine, "Fahrzeuge", "FahrzeugID", fahrzeug_ids)
    zahlung = extract_table(src_engine, "Zahlungsmethoden", "ZahlungsmethodenID", zahl_ids)
    park = extract_table(src_engine, "Parkplaetze", "ParkplatzID", park_ids)

    # === Zeitdimension ===
    dim_zeit = generate_dim_zeit(BUSINESS_YEAR)

    # === Dimensionen laden ===
    if not kunden.empty:
        upsert_sqlserver(dwh_engine, kunden, "Dim_Kunden", ["KundeNr"])
    if not fahrzeuge.empty:
        upsert_sqlserver(dwh_engine, fahrzeuge, "Dim_Fahrzeuge", ["FahrzeugID"])
    if not zahlung.empty:
        upsert_sqlserver(dwh_engine, zahlung, "Dim_Zahlungsmethoden", ["ZahlungsmethodenID"])
    if not park.empty:
        upsert_sqlserver(dwh_engine, park, "Dim_Parkplaetze", ["ParkplatzID"])
    if not dim_zeit.empty:
        upsert_sqlserver(dwh_engine, dim_zeit, "Dim_Zeit", ["Datum"])

    # === Fakten laden ===
    fact_cols = ["ReservierungsID", "FahrzeugID", "KundeNr", "ZahlungsmethodeID",
                 "ParkplatzID", "Startdatum", "Dauer", "Gesamtpreis"]
    fact_df = res[fact_cols].rename(columns={"Gesamtpreis": "GesamtPreis"})
    with dwh_engine.begin() as conn:
        fact_df.to_sql("Fakt_Reservierungen", conn, if_exists="append", index=False)

    # === Fortschritt aktualisieren ===
    new_max = int(res["ReservierungsID"].max())
    update_last_loaded(dwh_engine, new_max)
    logger.info("ETL erfolgreich abgeschlossen.")


# =====================================================
# SCRIPT ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run_etl()
