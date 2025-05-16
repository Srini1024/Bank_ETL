# 🏦 ETL Pipeline: World's Largest Banks by Market Capitalization

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline using Python to compile and process a list of the **top 10 largest banks in the world by market capitalization**. The pipeline is designed for quarterly reuse and includes detailed logging at each step.

---

## 📌 Project Overview

- **Source**: Wikipedia (archived version)
- **Output Formats**: CSV file & SQLite database
- **Currency Conversion**: USD → GBP, EUR, INR
- **Automation**: Built-in logging and reusability for quarterly updates

---

## 🔄 ETL Pipeline Stages

### 1. **Extract**
- Extracts a table from [Wikipedia](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks) listing the top 10 banks by market cap.
- Extracted columns: `Name`, `MC_USD_Billion`

### 2. **Transform**
- Reads exchange rates from `exchange_rate.csv`
- Converts `MC_USD_Billion` into:
  - `MC_GBP_Billion`
  - `MC_EUR_Billion`
  - `MC_INR_Billion`
- Values are rounded to 2 decimal places

### 3. **Load**
- Saves the transformed DataFrame to:
  - A CSV file: `Largest_banks_data.csv`
  - A SQLite database: `Banks.db`, table `Largest_banks`


