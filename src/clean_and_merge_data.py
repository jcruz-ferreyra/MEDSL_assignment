import csv
import re
from pathlib import Path

import numpy as np
import pandas as pd


def load_csv_with_fix(filepath: str) -> pd.DataFrame:
    """Load CSV file, fixing rows with leading empty fields."""
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        n_cols = len(header)

        data = []
        bad_rows = 0

        for i, row in enumerate(reader, start=2):
            # Strip leading empty fields if row is too long
            while len(row) > n_cols and row[0] == "":
                row.pop(0)

            if len(row) == n_cols:
                data.append(row)
            else:
                bad_rows += 1
                print(f"Skipped line {i}: expected {n_cols} fields, got {len(row)}")

    df = pd.DataFrame(data, columns=header)

    return df


# Function to clean candidate names
def clean_candidate(name):
    """Standardize candidate names for US PRESIDENT."""
    if pd.isna(name):
        return name

    name = str(name).upper()

    # Aggregate all write-ins to WRITE-IN
    if "(W/I)" in name or "W/I" in name:
        return "WRITE-IN"

    # Strip VP names (everything after &)
    if "&" in name:
        name = name.split("&")[0].strip()

    # Clean punctuation inconsistencies
    name = name.replace(".", "")

    # Fix encoding issues (André → ANDRE)
    name = name.replace("Ã©", "E").replace("Ã", "A")

    # Map to standardized presidential names
    president_map = {
        "DONALD J TRUMP": "DONALD J TRUMP",
        "KAMALA D HARRIS": "KAMALA D HARRIS",
        "CHASE OLIVER": "CHASE OLIVER",
        "CLAUDIA DE LA CRUZ": "CLAUDIA DE LA CRUZ",
        "CORNEL WEST": "CORNEL WEST",
        "ROBERT F KENNEDY JR": "ROBERT F KENNEDY",
        "PETER SONSKI": "PETER SONSKI",
        "JILL STEIN": "JILL STEIN",
        "RANDALL TERRY": "RANDALL TERRY",
        "JOSEPH KISHORE": "JOSEPH KISHORE",
        "RACHELE FRUIT": "RACHELE FRUIT",
    }

    return president_map.get(name, name)


def main():
    # =====================================
    # Define global variables
    # =====================================
    # Define paths relative to script location
    SCRIPT_DIR = Path(__file__).parent.parent
    INPUT_DIR = SCRIPT_DIR / "data/raw"
    OUTPUT_DIR = SCRIPT_DIR / "data/processed"

    filename = "part1_data.csv"
    fips_filename = "part1_fips.csv"

    # =====================================
    # Load data
    # =====================================

    # Load data with error handling
    try:
        df = pd.read_csv(INPUT_DIR / filename)
    except Exception:
        try:
            df = load_csv_with_fix(str(INPUT_DIR / filename))
        except Exception:
            df = pd.read_csv(INPUT_DIR / filename, on_bad_lines="skip")

    df_fips = pd.read_csv(INPUT_DIR / fips_filename)

    # =====================================
    # Clean data
    # =====================================

    # Rename columns to match codebook
    df = df.rename(
        columns={
            "ReportingCountyName": "county_name",
            "JurisdictionName": "jurisdiction_name",
            "DataEntryJurisdictionName": "precinct",
            "Office": "office",
            "NameonBallot": "candidate",
            "PoliticalParty": "party_detailed",
            "TotalVotes": "votes",
        }
    )

    # Extract year from Election column
    df["year"] = df["Election"].str.extract(r"(\d{4})")

    # Drop columns we don't need
    df = df.drop(
        columns=[
            "Election",
            "OfficeCategory",
            "BallotOrder",
            "Winner",
            "NumberofOfficeSeats",
            "DataEntryLevelName",
        ],
        errors="ignore",
    )

    # Standardize office to US PRESIDENT (uppercase, no VP)
    df["office"] = (
        df["office"].str.upper().str.replace("US PRESIDENT & VICE PRESIDENT", "US PRESIDENT")
    )

    # Uppercase party_detailed
    df["party_detailed"] = df["party_detailed"].str.upper()

    # Create party_simplified mapping
    party_map = {
        "DEMOCRATIC": "DEMOCRAT",
        "REPUBLICAN": "REPUBLICAN",
        "LIBERTARIAN": "LIBERTARIAN",
        "INDEPENDENT": "OTHER",  # Write-ins
        "OTHER": "OTHER",
        "PARTY FOR SOCIALISM AND LIBERATION": "OTHER",
        "AMERICAN SOLIDARITY": "OTHER",
        "WE THE PEOPLE": "OTHER",
    }

    df["party_simplified"] = df["party_detailed"].map(party_map)

    # Clean candidate names
    df["candidate"] = df["candidate"].apply(clean_candidate)

    # Create writein column
    df["writein"] = df["candidate"].apply(lambda x: "TRUE" if x == "WRITE-IN" else "FALSE")

    # Convert votes to numeric and set negative votes to 0
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0).astype(int)

    df.loc[df["votes"] < 0, "votes"] = 0

    # Add constant columns per codebook
    df["stage"] = "GEN"
    df["state"] = "INDIANA"
    df["special"] = "FALSE"
    df["date"] = "2024-11-05"

    # Uppercase text fields per codebook
    df["county_name"] = df["county_name"].str.upper()
    df["jurisdiction_name"] = df["jurisdiction_name"].str.upper()
    df["office"] = df["office"].str.upper()

    # Uppercase county_name in FIPS file to match
    df_fips["county_name"] = df_fips["county_name"].str.upper()

    # Merge FIPS codes
    df = df.merge(df_fips, on="county_name", how="left")

    # Convert county_fips to zero-padded string
    df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)

    # Final column order per codebook
    final_columns = [
        "precinct",
        "office",
        "party_detailed",
        "party_simplified",
        "votes",
        "county_name",
        "county_fips",
        "jurisdiction_name",
        "candidate",
        "year",
        "stage",
        "state",
        "special",
        "writein",
        "date",
    ]

    df_clean = df[final_columns]

    # Save cleaned data
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(OUTPUT_DIR / "part1_clean.csv", index=False)


if __name__ == "__main__":
    main()
