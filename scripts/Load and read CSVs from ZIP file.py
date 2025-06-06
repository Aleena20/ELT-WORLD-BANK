import pandas as pd
import zipfile

# ✅ Replace with your actual file path
zip_path = r"C:\Users\aleen\venv\API_GC.DOD.TOTL.GD.ZS_DS2_en_csv_v2_87867.zip"

# Open the ZIP and read the correct CSV file
with zipfile.ZipFile(zip_path, 'r') as z:
    # List files in the zip to confirm names
    print("Files inside ZIP:", z.namelist())

    # Open the actual data CSV inside the ZIP
    with z.open('API_GC.DOD.TOTL.GD.ZS_DS2_en_csv_v2_87867.csv') as f:
        df = pd.read_csv(f, skiprows=4)  # World Bank data usually has 4 metadata rows

# Show the first 5 rows
print(df.head())
