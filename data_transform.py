from transform_functions import USD_EUR_Conversion, map_competitor, map_compressor, KGS_Outlier_Handling, string_match, exclude_parts, preprocess_mapping
import pandas as pd
import swifter

def transform_data(raw_data, models):

    raw_data = raw_data.copy()
    models = models.copy()
    
    print("Starting Data transformation")
    USD_EUR = pd.read_csv(r"K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/USD_EUR.csv")

    USD_EUR["DATE"] = pd.to_datetime(USD_EUR["DATE"], format=r"%Y-%m-%d")
    USD_EUR.sort_values("DATE", inplace=True)

    USD_EUR.set_index('DATE', inplace=True)
    full_date_range = pd.date_range(start=USD_EUR.index.min(), end=USD_EUR.index.max())
    USD_EUR = USD_EUR.reindex(full_date_range)

    USD_EUR['US dollar/Euro (EXR.D.USD.EUR.SP00.A)'] = USD_EUR['US dollar/Euro (EXR.D.USD.EUR.SP00.A)'].ffill().bfill()

    USD_EUR.reset_index(inplace=True)
    USD_EUR.rename(columns={'index': 'DATE'}, inplace=True)

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    raw_data = USD_EUR_Conversion(raw_data, USD_EUR)

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    raw_data = raw_data.drop(raw_data[raw_data["Quantity"] <= 0].index)

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    #Convert strings to upper case to make atring matching easier

    raw_data ["Indian_Importer"] = raw_data["Indian_Importer"].str.upper()
    raw_data ["Foreign_Exporter"] = raw_data["Foreign_Exporter"].str.upper()
    raw_data ["Detailed_Description"] = raw_data["Detailed_Description"].str.upper()

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    #Categorize by main competitors

    competitor_mapping = {'Frascold': ['FRASCOLD'], 
                        'MYCOM': ['MAYEKAWA'],
                        'Snowman': ['FUJIAN', 'SRM', 'SNOWMAN'],
                            'Hanbell': ['HANBELL', 'COMER'],
                            'Fu Sheng': ['FU SHENG', 'FUSHENG'],
                            'Daikin': ['DAIKIN'],
                            'J&E Hall': ['J&E', 'J & E'],
                            'GEA': ['GEAREFRIG', 'GEA REFRIG'],
                            'Dorin': ['DORIN'],
                            'Bock': ['BOCK'],
                            'Danfoss': ['DANFOSS'],
                            'Copeland/Emerson': ['EMERSON', 'COPELAND'],
                            'BITZER': ['BITZER'],
                            'Siam': ['SIAM'],
                            'Invotech': ['INVOTECH']}


    raw_data['Foreign_Exporter'] = raw_data['Foreign_Exporter'].str.upper()
    raw_data["Competitor"] = raw_data['Foreign_Exporter'].apply(map_competitor, args=(competitor_mapping,))


    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    #Categorize compressor types

    compressor_mapping = {'Recip': 'RECIP',
                        'Scroll': 'SCROLL',
                        'Screw': 'SCREW',
                        'Rotary': 'ROTARY'}

    raw_data['Compressor_Type'] = raw_data['Detailed_Description'].apply(lambda x: map_compressor(x, compressor_mapping))

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    #Add Bock model mapping and replace numerical model descriptions with letter descriptions

    Bock_Mapping = {'14057': ' FEX ',
                    '14056': ' FKX ',
                    '20250': ' FKX ',
                    '20071': ' FKX ',
                    '11712': ' F16 ',
                    '11702': ' F16 ',
                    '11700': ' F16 ',
                    '144': ' HG '} #leave empty space before and after so string_matching can identify it as a seperate chunk (word)

    for key, value in Bock_Mapping.items():
        raw_data.loc[raw_data['Competitor'] == 'Bock', 'Detailed_Description'] = raw_data.loc[raw_data['Competitor'] == 'Bock', 'Detailed_Description'].str.replace(key, value)

    #----------------------------------------------------------------------------------------------------------------------------------------------------------
    
    raw_data = exclude_parts(raw_data, models)

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    raw_data.loc[raw_data['Competitor'] == 'BITZER', 'Detailed_Description'] = raw_data.loc[raw_data['Competitor'] == 'BITZER', 'Detailed_Description'].str.replace(' ', '_')

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    print("Starting Model Matching (This may take a while)")
    mapping_preprocessed = preprocess_mapping(models)
    raw_data[['models', 'comp_types', 'comp_family']] = raw_data.apply(lambda row: pd.Series(string_match(row["Detailed_Description"], row["Competitor"], mapping_preprocessed)), axis=1)
    
    print("Model Matching complete!")

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    raw_data["comp_family"] = raw_data["comp_family"].fillna("Unknown_Family")

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    # Merge Comp_Type Columns into comp_types

    raw_data['comp_types'] = raw_data['comp_types'] + raw_data['Compressor_Type']

    raw_data = raw_data.drop("Compressor_Type", axis=1)

    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    raw_data['comp_types'] = raw_data['comp_types'].replace({
    'RecipRecip': 'Recip',
    'ScrewScrew': 'Screw',
    'ScrollScroll': 'Scroll',
    'ScrollScrew': 'Scroll',
    'RecipScrew': 'Recip',
    'ScrollRecip': 'Scroll',
    'ScrewRecip': 'Screw',
    'ScrewScroll': 'Screw',
    'RecipScroll': 'Recip',
    'RecipRotary': 'Recip',
    'ScrewRotary': 'Screw',
    'ScrollRotary': 'Scroll',
    'Open-typeRecip': 'Open-type',
    'Open-typeScrew': 'Open-type',
    'Open-typeScroll': 'Open-type',
    'ACPScrew': 'ACP',
    'ACPRecip': 'ACP',
    'ACPScroll': 'ACP',
    '': 'Unknown Type',
    ' ': 'Unknown Type'
    })

    raw_data['comp_types'] = raw_data['comp_types'].fillna('Unknown Type')
    
    #----------------------------------------------------------------------------------------------------------------------------------------------------------

    raw_data = KGS_Outlier_Handling(raw_data, USD_EUR)

    
    print("Data Transformation complete!")
    return raw_data
        