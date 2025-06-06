# ELT Automation: World Bank API Finance Data to Azure SQL Database

This project demonstrates a complete ELT (Extract, Load, Transform) pipeline using Python and Azure SQL for analyzing global government debt as a percentage of GDP. The data is extracted from the World Bank API, processed, and visualized through Power BI.

## 📝Overview

This repository showcases an end-to-end ELT (Extract, Load, Transform) pipeline designed to extract financial data from the World Bank API, transform it using Pandas, and load it into an Azure SQL Database. The project highlights expertise in cloud-native data engineering, specifically focusing on data extraction, transformation, loading, and automation using cron.

The primary goal of this project is to illustrate how to design and orchestrate scalable ELT pipelines entirely in the cloud, ensuring no files are stored locally. The solution employs the following tools and technologies:

* **cron jobs (WSL)** For orchestrating and automating the ELT pipeline tasks.
* **Pandas:** For data manipulation and transformation, including cleaning and processing the data.
* **Azure SQL Database:** For storing the cleaned and transformed datasets, allowing for efficient querying and analysis.

This project serves as a practical example of leveraging modern data engineering practices and tools to automate workflows and ensure efficient data handling in a cloud environment.

## 🚦 About the Data

The data used in this project is sourced from the World Bank's historical loan data, which tracks loans provided to countries globally. Specifically, it includes information about loans made by the International Bank for Reconstruction and Development (IBRD). These loans are public or publicly guaranteed and are extended to member countries. The data is in USD and uses historical exchange rates.

* **Loan History:** Tracks loans by IBRD since 1947.
* **Currency:** Data in USD, reflecting historical market rates.
* **Scope:** Global data covering numerous countries and regions.
* \*\*Latest Snapshot: This dataset includes historical and up-to-date loan information.

# 📈 Key Concepts

* **Data Source:** [World Bank Finance Data API](mailto:supphttps://data.worldbank.org/indicator/GC.DOD.TOTL.GD.ZSort@readme.io)
* \*\*Storage:\*\*Azure SQL Storage for intermediate staging of extracted and transformed data.
* **Database:** Azure SQL Database (MSSQL) for the final storage of cleaned data.
* **Transformation:** Data cleaning, manipulation, and feature engineering using Pandas.
* **Orchestration:** Task scheduling and automation using cron jobs.

## # 💬 Tools & Technologies

* **cron jobs:** Manages the orchestration and automation of the ELT pipeline.
* **Pandas:** Used for data transformation, cleaning, and enrichment.
* \*\*Azure Services:\*\***SQL Database:** Loads & Stores transformed data for analysis.
* **Power BI:** For Visualization
* **.env (python-dotenv):** For Configurations
* **scikit-learn:**  Used for Machine Learning Forcasting
## ELT Process (Code Overview)
**Extraction:**
*	Data is extracted from the World Bank Finance API using Python’s requests library.
*	Due to resource constraints (free-tier limitations), Azure Blob Store was not used to store data

```python
import pandas as pd

url = "https://financesone.worldbank.org/api/views/DS00975/rows.csv?accessType=DOWNLOAD"
df = pd.read_csv(url)

print(df.head())
print(df.columns)

```
* The extracted data is stored as a CSV file in files

**Transformations:**

**The raw data undergoes several transformation steps:**
* Plot for each selected country
```python
plt.figure(figsize=(12, 7))

  for country in countries:
    country_data = df[df['Country Name'] == country]
    if country_data.empty:
        print(f"No data for {country}")
        continue
```
* Drop metadata columns and reshape
```python
country_data = country_data.drop(columns=['Country Code', 'Indicator Name', 'Indicator Code'])
      country_data = country_data.set_index('Country Name').T.reset_index()
      country_data.columns = ['Year', 'Debt (% of GDP)']
      country_data = country_data[country_data['Year'].str.isnumeric()]
      country_data['Year'] = country_data['Year'].astype(int)
      country_data['Debt (% of GDP)'] = pd.to_numeric(country_data['Debt (% of GDP)'], errors='coerce')

      plt.plot(country_data['Year'], country_data['Debt (% of GDP)'], label=country)
```
* Final plot formatting
```python
plt.title("Government Debt as % of GDP (Selected Countries)")
      plt.xlabel("Year")
      plt.ylabel("Debt (% of GDP)")
      plt.legend()
      plt.grid(True)
      plt.tight_layout()
      plt.show()
```
**Loading:**
* The final step is loading the transformed data into the Azure SQL Database using a bulk insert operation.
```python
df_long.to_sql('debt_percent_gdp', con=engine, if_exists='replace', index=False)
    print("✅ Data uploaded to Azure SQL successfully!")
    with engine.connect() as conn:
      result = conn.execute("SELECT TOP 5 * FROM debt_percent_gdp")
       for row in result:
         print(row)
```
* Once the data is loaded into the database, additional queries are run to verify data integrity and perform analyses on the dataset.

## Testing my Azure SQL Activation
```python
import pandas as pd
    import sqlalchemy
if hasattr(engine, 'dispose'):
          engine.dispose()
try:
            with engine.connect() as connection:
            connection.execute(sqlalchemy.text("ROLLBACK"))
            except:
               pass 
```
## Sample Query
* List countries with highest debt in 2020:
```python
query = """
    SELECT [Country Name], [Year], [DebtPercentGDP]
    FROM debt_percent_gdp
    WHERE [Year] >= '2010'
    ORDER BY [DebtPercentGDP] DESC
    """
```
## Logging
```python
log_filename = f"elt_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
   logging.basicConfig(filename=log_filename,
                       level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')

   logging.info("ELT script started")
log_filename = f"elt_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
   logging.basicConfig(filename=log_filename,
                       level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')

   logging.info("ELT script started")
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "debt_data_extraction.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract_debt_data():
    try:
        logging.info("Starting debt data extraction")
 output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)
        filename = f"debt_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(os.path.join(output_dir, filename), index=False)

        logging.info(f"Data extraction completed successfully. File saved as {filename}")
```
## Scheduling
```python
!pip install schedule
import schedule
import time
schedule.every().day.at("09:00").do(extract_debt_data)

while True:
    schedule.run_pending()
    time.sleep(60)
```
## Visualize in Power BI

<Image align="center" src="https://files.readme.io/eb7257e8ad6e29b615d07b3cadd7120fa8e3b9447c057acf119cecc6e4045250-Screenshot_2025-06-06_184919.png" />

<Image align="center" src="https://files.readme.io/42c04f0ad773fb671d18c7c9a83526b452d4685b56104a77c39b27918ea7563d-Screenshot_2025-06-06_185012.png" />

<Image align="center" src="https://files.readme.io/e3a7c6601b55792e1f03566ea5b18413f6322b803bf2bd31576bdb6ff4cd9d72-Screenshot_2025-06-06_185134.png" />

## Automation Using cron
```python
  0 8 * * * /mnt/c/Users/aleen/venv/Scripts/python.exe /mnt/c/Users/aleen/venv/Scripts/elt_pipeline.py >> /mnt/c/Users/aleen/venv/Scripts/elt_pipeline.log 2>&1
```
## Email Alerts on Cron Failure in WSL

```bash
sudo nano /etc/postfix/main.cf
sudo nano /etc/postfix/sasl_passwd
      smtp.gmail.com:587    your.email@gmail.com:your_app_password
  sudo postmap /etc/postfix/sasl_passwd
  sudo chown root:root /etc/postfix/sasl_passwd /etc/postfix/sasl_passwd.db
  sudo chmod 600 /etc/postfix/sasl_passwd /etc/postfix/sasl_passwd.db
      relayhost = smtp.gmail.com:587
      smtp_sasl_auth_enable = yes
      smtp_sasl_password_maps = hash:/etc/postfix/sasl_passwd
      smtp_sasl_security_options = noanonymous
      smtp_tls_security_level = may
      smtp_tls_CAfile = /etc/ssl/certs/ca-certificates.crt
  sudo service postfix restart
echo "Test mail from postfix" | mail -s "Postfix Test" your.email@gmail.com
```
## Integrating a Machine Learning Model

* Forecasting Debt Percent GDP
```python
future_years = np.array(range(2024, 2029)).reshape(-1, 1)
       predictions = model.predict(future_years)

       forecast_df = pd.DataFrame({
          "Country": "Germany",
          "Year": future_years.flatten(),
          "PredictedDebtPercentGDP": predictions
        })
forecast_df.to_csv("forecast_debt_germany.csv", index=False)
actual_df = df_germany_melted[["Year", "DebtPercentGDP"]].copy()
       actual_df.rename(columns={"DebtPercentGDP": "Value"}, inplace=True)
       actual_df["Type"] = "Actual"

       forecast_df.rename(columns={"PredictedDebtPercentGDP": "Value"}, inplace=True)
       forecast_df["Type"] = "Forecast"

       combined_df = pd.concat([actual_df, forecast_df], ignore_index=True)
       combined_df.to_csv("actual_vs_forecast_debt.csv", index=False)
```
<Image align="center" src="https://files.readme.io/c450f6dfed4c07de39ff5580f5364895b9943a1c26dd85efef7e141518804c64-Screenshot_2025-06-06_185212.png" />

## 🌟 Author Notes

This project was completed as part of the MADSC301 BI final assessment with real-world data, ML forecasting, and automation.

