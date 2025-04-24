import pandas as pd
import numpy as np
import os
import math

def load_data(old_data_path):

    print("Starting Data loading")

    column_translations = {'海关编码': 'HS_Code', 
                        '详细产品名称': 'Detailed_Description',
                        '日期': 'Date',
                        '印度进口商': 'Indian_Importer',
                        '数量单位': 'Quantity_Units',
                        '数量': 'Quantity',
                        '美元总金额': 'Total_Dollar_Amount',
                        '美元单价': 'USD_Unit_Price',
                        '卢比总金额': 'Total_Rupees_Amount',
                        '卢比单价': 'Rupees_Unit_Price',
                        '成交外币金额': 'Trans_Amount_Foreign_Currency',
                        '成交外币单价': 'Trans_Unit_Price_Foreign_Currency',
                        '币种': 'Currency',
                        '月度': 'Monthly',
                        '国外出口商': 'Foreign_Exporter',
                        '产销洲': 'Export_Continent',
                        '印度目的港': 'Entry_Port',
                        '国外装货港': 'Shipping_Port',
                        '产品描述': 'Product_Description',
                        '卢比总税费': 'Rupees_Total_Taxes',
                        '关单号': 'Customer_Order_Number',
                        '印度港口代码': 'Indian_Port_Code',
                        '运输方式': 'Transport_Method',
                        '报关行': 'Transport_Company',
                        '报关行代码': 'Transport_Company_Code',
                        '进口商地址': 'Importer_Address',
                        '进口商邮编': 'Importer_Zip_Code',
                        '进口商企业编码': 'Importer_Company_Code',
                        '进口商城市': 'Importer_City',
                        '出口商地址': 'Exporter_Address',
                        '合同号': 'Contract_No',
                        '进出口': 'Import_Or_Export'}

    use_cols = ["海关编码", '详细产品名称', '日期', '印度进口商', '数量单位', '数量', '美元总金额', '美元单价', '卢比总金额', '卢比单价', '成交外币金额', '成交外币单价', '币种', '国外出口商', '产品描述', '进口商地址']

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Load suplliers with their aliases
    print("Loading suppliers with their aliases")
    suppliers = pd.read_excel("K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/Supplier Names India.xlsx", header=2)

    suppliers = suppliers.to_dict('list')

    print("Suppliers loaded:", suppliers)
    def is_nan(value):
        try:
            return math.isnan(value)
        except:
            return False

    for key, value_list in suppliers.items():
        suppliers[key] = [value for value in value_list if not is_nan(value)]

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Load model namings per supplier
    print("Loading model namings per supplier")
    models = pd.read_excel("K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/Model Mapping.xlsx")
    models["Model Details"] = models["Model Details"].fillna('').astype(str)
    models['Model Family'] = models['Model Family'].astype(str)

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Load raw trade data
    print("Loading raw trade data")
    base_dir = 'K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/'
    subdirs = ['2021', '2022', '2023', '2024', '2025']

    # Initialize an empty list to store the file paths
    filenames = []

    # Loop through each subdirectory
    for subdir in subdirs:
        # Construct the full path to the subdirectory
        full_path = os.path.join(base_dir, subdir)
        
        # Check if the subdirectory exists
        if os.path.exists(full_path):
            # Loop through each file in the subdirectory
            for filename in os.listdir(full_path):
                # Construct the full path to the file
                file_path = os.path.join(full_path, filename)
                
                # Add the file path to the list
                filenames.append(file_path)
    print("Number of files loaded:", len(filenames))
    print("initializing empty list for dataframes")
    dfs = []

    print("Loading files into dataframes with for loop")
    for file in filenames:
        print("Loading file:", file)
        dfs.append(pd.read_excel(os.path.join(base_dir, file), header=7, dtype={"数量": np.int64, '美元总金额': np.int64, '美元单价': np.int64, '卢比总金额': np.int64, '卢比单价': np.int64}, 
                                usecols=use_cols))
        print("File loaded:", file)

    raw_data = pd.concat(dfs)

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Translate column descriptions, drop unnecessary columns
    print("Translating column descriptions")
    raw_data.rename(columns=column_translations, inplace=True)
    raw_data = raw_data.assign(Origin_Country='')

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # Convert na values of detailed description into string
    print("Converting na values of detailed description into string")
    raw_data['Detailed_Description'] = raw_data['Detailed_Description'].fillna('').astype(str)

    #Convert date strings into datetime objects and sort by date
    print("Converting date strings into datetime objects and sorting by date")
    raw_data['Date'] = pd.to_datetime(raw_data['Date'], format="%Y/%m/%d")
    raw_data.sort_values("Date", inplace=True)

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Filter out suppliers which are car companies
    print("Filtering out suppliers which are car companies")
    raw_data['Foreign_Exporter'] = raw_data['Foreign_Exporter'].astype(str)

    raw_data = raw_data[~raw_data['Foreign_Exporter'].str.contains('MERCEDES|DAIMLER|VOLVO|TOYOTA|FORD|HYUNDAI|JAGUAR', case=False, regex=True)]

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    if os.path.exists(old_data_path):
        old_data = pd.read_csv(old_data_path)
        print("Number of Rows of old data:", len(old_data.index))

        old_data["Date"] = pd.to_datetime(old_data["Date"], errors='coerce')
        old_data = old_data.dropna(subset=["Date"])

    
        old_latest_date = old_data["Date"].max()
        new_data = raw_data.loc[(raw_data["Date"] > old_latest_date)]
    
    #If script is run again but no new data is available, the last month will become the new data (For testing and demo purposes)
        if len(new_data.index) == 0:
            new_data = raw_data.loc[(raw_data['Date'] > raw_data['Date'].max() - pd.DateOffset(months=1))]
            old_data = old_data.loc[(old_data['Date'] < old_data['Date'].max() - pd.DateOffset(months=1))]
    else:
        old_data = pd.DataFrame(columns=raw_data.columns)
        new_data = raw_data

    print("Loaded models:", models)

    print("Number of Rows of new data:", len(new_data.index))

    print("Data Load complete!")

    
    print("return new data, models, old data")
    return new_data, models, old_data