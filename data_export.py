import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os


def export_data(raw_data):
    try:
        os.mkdir("C:/Tradedata_Output")
    except OSError as error:
        print(error)


    raw_data['Year'] = raw_data['Date'].dt.year
    raw_data['Month'] = raw_data['Date'].dt.month

    excel_output = raw_data[["Year", "Month", "Competitor", "comp_types", "comp_family", "models", "Indian_Importer", "Detailed_Description", "Total_Euro_Amount", "Total_Rupees_Amount", "Quantity"]]

    excel_output.to_excel("C:/Tradedata_Output/data.xlsx", index=False)
    raw_data.to_csv("C:/Tradedata_Output/data.csv", index=False)