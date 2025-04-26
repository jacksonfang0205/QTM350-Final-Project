import sqlite3
import pandas as pd

# ----------------------------------
# Step 1: Connect to SQLite database
# ----------------------------------
conn = sqlite3.connect('education_enrollment.db')
cursor = conn.cursor()

# ----------------------------------
# Step 2: Process all 3 datasets
# ----------------------------------
datasets = [
    ('primary', 'data/primary/primary.csv'),
    ('secondary', 'data/secondary/secondary.csv'),
    ('tertiary', 'data/tertiary/tertiary.csv')
]

target_countries = ['China', 'Japan', 'South Korea', 'United States', 'United Kingdom', 'Canada']
year_columns = [str(year) for year in range(1990, 2024)]

for level, filepath in datasets:
    print(f"Processing {level} dataset...")

    df = pd.read_csv(filepath, skiprows=4)
    df = df[df['Country Name'].isin(target_countries)]
    df = df[['Country Name', 'Country Code', 'Indicator Name'] + year_columns]

    df_long = df.melt(
        id_vars=['Country Name', 'Country Code', 'Indicator Name'],
        var_name='Year',
        value_name='Enrollment_Percentage'
    )

    df_long = df_long.rename(columns={
        'Country Name': 'Country_Name',
        'Country Code': 'Country_Code'
    })

    df_long = df_long.dropna(subset=['Enrollment_Percentage'])

    table_name = f"{level}_enrollment_raw"
    df_long.to_sql(table_name, conn, if_exists='replace', index=False)

    print(f"{level.capitalize()} data loaded into table: {table_name}")
    print(df_long.head(), "\n")

# ----------------------------------
# Step 3: Run SQL to clean and combine tables
# ----------------------------------
sql_script = """
-- 1. Clean Primary
CREATE TABLE cleaned_primary AS
SELECT 
    Country_Name,
    Country_Code,
    CAST(Year AS INTEGER) AS Year,
    Enrollment_Percentage
FROM 
    primary_enrollment_raw
WHERE 
    Country_Name IN ('China', 'Japan', 'South Korea', 'United States', 'United Kingdom', 'Canada')
    AND CAST(Year AS INTEGER) BETWEEN 1990 AND 2023;

-- 2. Clean Secondary
CREATE TABLE cleaned_secondary AS
SELECT 
    Country_Name,
    Country_Code,
    CAST(Year AS INTEGER) AS Year,
    Enrollment_Percentage
FROM 
    secondary_enrollment_raw
WHERE 
    Country_Name IN ('China', 'Japan', 'South Korea', 'United States', 'United Kingdom', 'Canada')
    AND CAST(Year AS INTEGER) BETWEEN 1990 AND 2023;

-- 3. Clean Tertiary
CREATE TABLE cleaned_tertiary AS
SELECT 
    Country_Name,
    Country_Code,
    CAST(Year AS INTEGER) AS Year,
    Enrollment_Percentage
FROM 
    tertiary_enrollment_raw
WHERE 
    Country_Name IN ('China', 'Japan', 'South Korea', 'United States', 'United Kingdom', 'Canada')
    AND CAST(Year AS INTEGER) BETWEEN 1990 AND 2023;

-- 4. Combine into a unified table
CREATE TABLE enrollment_all_levels AS
SELECT 
    'Primary' AS Education_Level,
    Country_Name,
    Country_Code,
    Year,
    Enrollment_Percentage
FROM cleaned_primary

UNION ALL

SELECT 
    'Secondary',
    Country_Name,
    Country_Code,
    Year,
    Enrollment_Percentage
FROM cleaned_secondary

UNION ALL

SELECT 
    'Tertiary',
    Country_Name,
    Country_Code,
    Year,
    Enrollment_Percentage
FROM cleaned_tertiary;
"""

cursor.executescript(sql_script)
conn.commit()
conn.close()

print("SQL cleaning complete! Tables created: cleaned_primary, cleaned_secondary, cleaned_tertiary, enrollment_all_levels")
