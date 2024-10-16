import pandas as pd
import nltk
nltk.download('words', quiet=True)
from nltk.corpus import words

def map_competitor(exporter, mapping):
        for competitor, substrings in mapping.items():
            for substring in substrings:
                if substring in exporter:
                    return competitor
        return 'Other'

#----------------------------------------------------------------------------------------------------------------------------------------------------------

def map_compressor(description, mapping):
        for type, substring in mapping.items():
            if substring in description:
                return type
        return ''

#----------------------------------------------------------------------------------------------------------------------------------------------------------

def is_english_word(word, threshold=80):
        english_words = set(words.words())
        # Check for exact match
        compressor_keywords = ["Screw", "Compressor", "Comp", "Compresor", "Recip", "Rotary", "Centrifugal"]
        if word in english_words or word in compressor_keywords:
            return True

#----------------------------------------------------------------------------------------------------------------------------------------------------------

def KGS_Outlier_Handling(data, USD_EUR):
    #Identify competitors where deliveries are in KGS
    KGS_Deliveries = data.loc[(data['Quantity_Units'] == 'KGS') | (data['Quantity_Units'] == 'KGS ')]
    competitors = set(KGS_Deliveries['Competitor'])
    comp_types = set(KGS_Deliveries['comp_types'])

    for competitor in competitors:
        for comp_type in comp_types:
            sel = KGS_Deliveries[(KGS_Deliveries['Competitor'] == competitor) & (KGS_Deliveries["comp_types"] == comp_type)]
            data.loc[sel.index, 'Quantity'] = sel['Quantity'].median()
            data.loc[sel.index] = USD_EUR_Conversion(sel, USD_EUR)

    #Drop deliveries where Unit Prices are too low
    data = data.drop(data[(data["Euros_Unit_Price"] <= 90) & (data['Quantity_Units'] != 'KGS') & (data['Quantity_Units'] != 'KGS ')].index)

    return data    

#----------------------------------------------------------------------------------------------------------------------------------------------------------

def USD_EUR_Conversion(data, USD_EUR):

        merged_data = data.merge(USD_EUR, left_on='Date', right_on='DATE', how='left')

        # Currency conversion
        merged_data['Total_Euro_Amount'] = merged_data['Total_Dollar_Amount'] / merged_data['US dollar/Euro (EXR.D.USD.EUR.SP00.A)']
        merged_data['Total_Euro_Amount'] = merged_data['Total_Euro_Amount'].astype(int)
        merged_data['Euros_Unit_Price'] = merged_data['USD_Unit_Price'] / merged_data['US dollar/Euro (EXR.D.USD.EUR.SP00.A)']
        merged_data['Euros_Unit_Price'] = merged_data['Euros_Unit_Price'].astype(int)

        # Drop the extra 'DATE' column from the merge if desired
        merged_data.drop(columns=['DATE', 'US dollar/Euro (EXR.D.USD.EUR.SP00.A)'], inplace=True)

        return merged_data

#----------------------------------------------------------------------------------------------------------------------------------------------------------

def preprocess_mapping(mapping):
    """
    Preprocess the mapping DataFrame to create a dictionary with the company as the key.
    This allows for faster lookups in the main function.
    """
    company_dict = {}
    for company in mapping['Company'].unique():
        company_df = mapping[mapping['Company'] == company]
        company_dict[company] = company_df
    return company_dict

#----------------------------------------------------------------------------------------------------------------------------------------------------------

# Definition of string matching function to get the model type and compressor type
def string_match(description, company, company_dict):
    '''
    This function scans the detailed description column of the trade data for model descriptions corresponding to the company. Must be applied row-wise
    
    Args:
        description (Iterable): Detailed Description column
        company (Iterable): Company column
        mapping (pandas.DataFrame): Optional, the model mapping per company with comp types. Defaults to models

    Returns:
        Tuple: model, comp_type, comp_family per row as a tuple of strings. Returns "Unknown Model" if model is unknown or "Unknown Company" if company is unknown
               and "" (empty String) for comp_type and "Unknown Family" for comp_family in both cases
    '''
   
    
    if company == "Other":
        return "Unknown_Company", "", "Unknown_Family"
    
    model = "Unknown_Model"
    comp_type = ""
    comp_family = "Unknown_Family"

    # Pre-split description if the company isn't BITZER
    if company != "BITZER":
        chunks = description.split()
    else:
        chunks = [description]

    # Retrieve the preprocessed company-specific mapping
    model_sel = company_dict.get(company, None)
    if model_sel is None:
        return model, comp_type, comp_family

    for chunk in chunks:
        if is_english_word(chunk):
            continue

        for _, row in model_sel.iterrows():
            model_family = row['Model Family']
            model_details = row['Model Details']

            if model_family in chunk:
                if model_details and model_details in chunk:
                    # Check order of family and details
                    if chunk.find(model_family) < chunk.find(model_details):
                        model = f"{model_family}...{model_details}"
                        comp_type = row['Compressor Type']
                        comp_family = row['Compressor Family']
                        return model, comp_type, comp_family  # Early exit on first match
                else:
                    model = model_family
                    comp_type = row['Compressor Type']
                    comp_family = row['Compressor Family']
                    return model, comp_type, comp_family  # Early exit on first match

    return model, comp_type, comp_family

#----------------------------------------------------------------------------------------------------------------------------------------------------------

def exclude_parts(data, mapping_parts):
    '''
    This function excludes parts according to the model mapping file. For a given company it excludes all records where a parts characters entry is in tthe product description and/or 
    the Eur_Unit_Price is smaller. This function also excludes ALL deliveries from unknown companies which are not specified in Supplier Names. Unique Company Names in models must
    exactly match the unique Competitor Names.

    Args:
        data (pandas.DataFrame): Tradedata to filter
        mapping (pandas.DataFrame): Optional, the model mapping per company with comp types. Defaults to models

    Returns:
        pandas.DataFrame: The filtered Dataframe
    '''

    dfs = []

    #Get unique company names
    companies = mapping_parts["Company"].unique()

    #Drop redundant rows where no Parts Characters AND Min Unit Price aren't specified
    mapping_parts.dropna(subset=["Parts Characters", "Min Unit Price"], how="all", inplace=True)

    #Fill empty cells with dummy values which won't filter
    mapping_parts["Parts Characters"] = mapping_parts["Parts Characters"].fillna("")
    mapping_parts["Min Unit Price"] = mapping_parts["Min Unit Price"].fillna(0)

    #
    dfs = []
    for company in companies:
        model_sel = mapping_parts[mapping_parts["Company"] == company]
        filter = model_sel[["Parts Characters", "Min Unit Price"]]
        data_sel = data[data["Competitor"] == company]
        
        for i in filter.index:
            if filter["Parts Characters"][i] == "" and filter["Min Unit Price"][i] == 0: #In this case everything will be filtered out, so skip this filter row
                continue

            elif filter["Parts Characters"][i] != "" and filter["Min Unit Price"][i] != 0:
                #print("Both:", company, filter["Parts Characters"][i], filter["Min Unit Price"][i])
                data_sel = data_sel[~((data_sel["Detailed_Description"].str.contains(filter["Parts Characters"][i])) & (data_sel["Euros_Unit_Price"] < filter["Min Unit Price"][i]))]

            elif filter["Parts Characters"][i] == "" and filter["Min Unit Price"][i] != 0:
                #print("Price:", company, filter["Min Unit Price"][i])
                data_sel = data_sel[(data_sel["Euros_Unit_Price"] > filter["Min Unit Price"][i])]

            elif filter["Parts Characters"][i] != "" and filter["Min Unit Price"][i] == 0:
                #print("Characters:", company, filter["Parts Characters"][i])
                data_sel = data_sel[(~data_sel["Detailed_Description"].str.contains(filter["Parts Characters"][i]))]

        dfs.append(data_sel)

    output = pd.concat(dfs)

    return output