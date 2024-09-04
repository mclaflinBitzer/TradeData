import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import pinyin
from statistics import mean, stdev
import seaborn as sns
from itertools import chain
import math
import re
import nltk
nltk.download('words')
from nltk.corpus import words
from fuzzywuzzy import fuzz
import time

def load_data(old_data_path):

    print("Starting Data loading")
    try:
        os.access
    except OSError as error:
        print("No previous dataframe found. ")

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

    suppliers = pd.read_excel("K:\DESDN\mbd\pm\mpm_pma/00_Projekte\CSMO\Market Assessment\Market APAC\India\Handelsdatenprojekt/Daten/Supplier Names India.xlsx", header=2)

    suppliers = suppliers.to_dict('list')

    def is_nan(value):
        try:
            return math.isnan(value)
        except:
            return False

    for key, value_list in suppliers.items():
        suppliers[key] = [value for value in value_list if not is_nan(value)]

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Load model namings per supplier

    models = pd.read_excel("K:\DESDN\mbd\pm\mpm_pma/00_Projekte\CSMO\Market Assessment\Market APAC\India\Handelsdatenprojekt\Daten\Model Mapping.xlsx")
    models["Model Details"] = models["Model Details"].fillna('').astype(str)
    models['Model Family'] = models['Model Family'].astype(str)

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Load raw trade data

    base_dir = 'K:\DESDN\mbd\pm\mpm_pma/00_Projekte\CSMO\Market Assessment\Market APAC\India\Handelsdatenprojekt\Daten/'
    subdirs = ['2021', '2022', '2023', '2024']

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

    dfs = []

    for file in filenames:
        dfs.append(pd.read_excel(os.path.join(base_dir, file), header=7, dtype={"数量": np.int64, '美元总金额': np.int64, '美元单价': np.int64, '卢比总金额': np.int64, '卢比单价': np.int64}, 
                                usecols=use_cols))

    raw_data = pd.concat(dfs)

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Translate column descriptions, drop unnecessary columns

    raw_data.rename(columns=column_translations, inplace=True)
    raw_data = raw_data.assign(Origin_Country='')

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # Convert na values of detailed description into string

    raw_data['Detailed_Description'] = raw_data['Detailed_Description'].fillna('').astype(str)

    #Convert date strings into datetime objects and sort by date

    raw_data['Date'] = pd.to_datetime(raw_data['Date'], format="%Y/%m/%d")
    raw_data.sort_values("Date", inplace=True)

    #Filter out suppliers which are car companies

    raw_data['Foreign_Exporter'] = raw_data['Foreign_Exporter'].astype(str)

    raw_data = raw_data[~raw_data['Foreign_Exporter'].str.contains('MERCEDES|DAIMLER|VOLVO|TOYOTA|FORD|HYUNDAI|JAGUAR', case=False, regex=True)]

    print("Data Load complete!")
    return raw_data, models