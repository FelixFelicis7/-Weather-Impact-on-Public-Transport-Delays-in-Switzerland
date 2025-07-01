# **Manual: Data Integration Process**

This manual provides step-by-step instructions to execute the integration step using the provided code.

---

## **1. Setup the Database**

1. **Run the SQL Script**:
   - Execute the `createTables.sql` script.
   - Ensure **each SQL command is executed successfully**, as the database will not be created correctly otherwise.

---

## **2. Integration Process**

### **2.1 Prerequisites**

Before running the integration, ensure the following:

- **Datasets**:
  - Create a `datasets` folder in the same directory as the Python and SQL files. 
  - Inside this directory, create two subfolders:
    - **`transport`**: Contains public transport data.
    - **`weather`**: Contains weather-related data.

### **2.2 Dataset Details**

#### **Transport Folder**
- Contains **5 subfolders**:
  1. **Stations Information**:
     - Download from [Haltestellen 2024](https://opentransportdata.swiss/dataset/b558f4f8-7041-4105-9890-778c232704af/resource/132e28f6-750b-4bb3-a3f5-87143266ff22/download/haltestellen_2024.zip).
  2. **Public Transport Data**:
     - Download data for **January**, **April**, **July**, and **November 2024** from the [IST-Daten Archive](https://opentransportdata.swiss/de/ist-daten-archiv/).
     - Ensure the folder structure and file names match those referenced in the `data_integration.py` code.

#### **Weather Folder**
- Contains:
  1. **Geographical Information CSV**:
     - Use the file `group19-liste-download-nbcn-d.csv` from the SwitchDrive. 
     - Alternatively, download the original file from the official source if the provided file is poorly formatted or missing columns.
     - **Important**: Check the CSV file for an empty row at the end (e.g., `;;;;;;`). If present, delete it before running the script.
  2. **Measurements Folder**:
     - Contains measurement data for every weather station for the year 2024. Download the necessary data in CSV format from the before mentioned CSV.

---

### **2.3 Running the Integration**

1. Execute the **main function** in `data_integration.py`:
   - This script imports the datasets into the database.

2. **Important Notes**:
   - The integration process is **time-consuming**, especially for the transport data (approximately **55GB** of data for 4 months).
   - Ensure your device has sufficient resources and is plugged in to avoid interruptions.

---

## **3. Summary**

- **Setup**:
  - Ensure the database is created using the `createTables.sql` script.
  - Verify that all datasets are correctly formatted and placed in the appropriate folders.
- **Integration**:
  - Run the Python script `data_integration.py` to import the data into the database.
