import pandas as pd
import os


def export_data(new_data, old_data):
    print("Starting Data Extraction")
    try:
        os.mkdir("C:/Tradedata_Output")
    except OSError:
        print("Directory C:/Tradedata_Output already exists")

    new_data['Year'] = new_data['Date'].dt.year
    new_data['Month'] = new_data['Date'].dt.month

    max_month = str(new_data['Month'].max())
    max_year = str(new_data['Year'].max())

    print("Combining new and old data")
    output = pd.concat([old_data, new_data])


    print("Checking for and dropping duplicates")
    output.drop_duplicates(inplace=True)

    print("Extracting to Excel and CSV")
    excel_output = output[["Year", "Month", "Competitor", "comp_types", "comp_family", "models", "Indian_Importer", "Detailed_Description", "Total_Euro_Amount", "Total_Rupees_Amount", "Quantity"]]
    excel_output.to_excel(f"C:/Tradedata_Output/Import Data India_Compressors_{max_month}{max_year}.xlsx", index=False)

    output.to_csv("C:/Tradedata_Output/data.csv", index=False)

    print("Data Extraction complete!")