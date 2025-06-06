
import pandas as pd
import zipfile
import matplotlib.pyplot as plt

# ✅ Replace with your actual file path
zip_path = r"C:\Users\aleen\venv\API_GC.DOD.TOTL.GD.ZS_DS2_en_csv_v2_87867.zip"

# Open the ZIP and read the correct CSV file
with zipfile.ZipFile(zip_path, 'r') as z:
    # List files in the zip to confirm names
    print("Files inside ZIP:", z.namelist())

    # Open the actual data CSV inside the ZIP
    with z.open('API_GC.DOD.TOTL.GD.ZS_DS2_en_csv_v2_87867.csv') as f:
        df = pd.read_csv(f, skiprows=4)  # World Bank data usually has 4 metadata rows

# List of countries to analyze
countries = ['India', 'United States', 'China', 'Japan', 'Germany']

# Plot for each selected country
plt.figure(figsize=(12, 7))

for country in countries:
    country_data = df[df['Country Name'] == country]
    if country_data.empty:
        print(f"No data for {country}")
        continue

    # Drop metadata columns and reshape
    country_data = country_data.drop(columns=['Country Code', 'Indicator Name', 'Indicator Code'])
    country_data = country_data.set_index('Country Name').T.reset_index()
    country_data.columns = ['Year', 'Debt (% of GDP)']
    country_data = country_data[country_data['Year'].str.isnumeric()]
    country_data['Year'] = country_data['Year'].astype(int)
    country_data['Debt (% of GDP)'] = pd.to_numeric(country_data['Debt (% of GDP)'], errors='coerce')

    plt.plot(country_data['Year'], country_data['Debt (% of GDP)'], label=country)

# Final plot formatting
plt.title("Government Debt as % of GDP (Selected Countries)")
plt.xlabel("Year")
plt.ylabel("Debt (% of GDP)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
