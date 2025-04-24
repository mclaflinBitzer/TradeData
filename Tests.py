import ipytest
import random
import string
import pandas as pd
#ipytest.autoconfig()
import statistics

'''
def get_random_descriptions(model_family, model_details, company, models=models):
    #
   # Get three random strings per row in model mapping. The first one should be identifiable and the second and third not. The third is also completely wrong.
   # Use this to check if string_match function for detailed description works properly.

   # Returns:
   #     string_match, string_mismatch1, string_mismatch2 (tuple): tuple of string that matches and string that doesn't match
    #

    sel = models[(models["Company"] == company) & (models["Model Family"] != model_family) & (models["Model Details"] != model_details)]
    other_families = sel['Model Family']
    other_details = sel["Model Details"]

    string_match = ''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(0, 5))) + (model_family) + ''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(0, 5))) + model_details + ''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(0, 5)))
    for others in other_families:
        while others in string_match:
            ''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(0, 5))) + (model_family) + ''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(0, 5))) + model_details + ''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(0, 5)))

    string_mismatch1 = ''.join(random.choice(string.ascii_uppercase) for i in range(15))
    while model_family in string_mismatch1:
        string_mismatch1 = ''.join(random.choice(string.ascii_uppercase) for i in range(15))

    
    string_mismatch2 = ''.join(random.choice(string.ascii_uppercase) for i in range(15))
    while model_family in string_mismatch1:
        string_mismatch2 = ''.join(random.choice(string.ascii_uppercase) for i in range(15))
    
    
    return string_match, string_mismatch1, string_mismatch2



def test_string_match_true(models=models):
    
    models[["String Match", "String Mismatch 1", "String Mismatch 2"]] = models.apply(lambda row: pd.Series(get_random_descriptions(row["Model Family"], row["Model Details"]), row["Company"]), axis=1)
    models[["results_match", "0"]] = models.apply(lambda row: pd.Series(string_match(row["String Match"], row["Company"])))
    models.drop("0")

    assert "Unknown_Model" not in models["results_match"]



def test_string_match_false1(models=models):

    models[["String Match", "String Mismatch 1", "String Mismatch 2"]] = models.apply(lambda row: pd.Series(get_random_descriptions(row["Model Family"], row["Model Details"])), axis=1)
    models[["results_mismatch1", "1"]] = models.apply(lambda row: pd.Series(string_match(row["String Mismatch 1"], row["Company"])))
    models.drop("1")

    assert models["results_mismatch1"].str.contains("Unknown_Model").sum() == len(models.index())



def test_string_match_false2(models=models):

    models[["String Match", "String Mismatch 1", "String Mismatch 2"]] = models.apply(lambda row: pd.Series(get_random_descriptions(row["Model Family"], row["Model Details"])), axis=1)
    models[["results_mismatch2", "2"]] = models.apply(lambda row: pd.Series(string_match(row["String Mismatch 2"], row["Company"])))
    models.drop("2")

    assert models["results_mismatch2"].str.contains("Unknown_Model").sum() == len(models.index())
'''

def test_distribution(new_data, old_data):
    print("Testing distribution of new data against old data")
    if len(old_data.index) == 0:
        old_data = new_data
    four_months_ago = old_data['Date'].max() - pd.DateOffset(months=3)
    old_data = old_data[old_data['Date'] > four_months_ago]
    

    previous_grouped = old_data.groupby(["Competitor", "models", "Date"]).agg(Quantity_sum=("Quantity", "sum"),
                                                                              Total_Dollar_Amount_sum=("Total_Dollar_Amount", "sum"))
    
    current_grouped = new_data.groupby(["Competitor", "models", "Date"]).agg(Quantity_sum=("Quantity", "sum"),
                                                                              Total_Dollar_Amount_sum=("Total_Dollar_Amount", "sum"))

    previous_grouped = previous_grouped.groupby(["Competitor", "models"]).agg(Quantity_mean=('Quantity_sum', 'mean'),
        Quantity_std=('Quantity_sum', 'std'),
        Total_Dollar_Amount_mean=('Total_Dollar_Amount_sum', 'mean'),
        Total_Dollar_Amount_std=('Total_Dollar_Amount_sum', 'std')
    ).reset_index()
    current_grouped = current_grouped.groupby(["Competitor", "models"]).agg(Quantity_mean=('Quantity_sum', 'mean'),
        Total_Dollar_Amount_mean=('Total_Dollar_Amount_sum', 'mean')
    ).reset_index()

    merged = pd.merge(previous_grouped, current_grouped, on=['Competitor', 'models'], suffixes=('_old', '_new'))

    outliers = merged[
        (merged['Quantity_mean_new'] > merged['Quantity_mean_old'] + merged['Quantity_std']) |
        (merged['Quantity_mean_new'] < merged['Quantity_mean_old'] - merged['Quantity_std']) |
        (merged['Total_Dollar_Amount_mean_new'] > merged['Total_Dollar_Amount_mean_old'] + merged['Total_Dollar_Amount_std']) |
        (merged['Total_Dollar_Amount_mean_new'] < merged['Total_Dollar_Amount_mean_old'] - merged['Total_Dollar_Amount_std'])
    ]

    outliers.rename(columns = {'Quantity_std': 'Quantity_std_old',
                               'Total_Dollar_Amount_std': 'Total_Dollar_Amount_std_old'},
                               inplace=True)
    if len(outliers.index) > 0:
        print("The following model(s) mean(s) from the last month are outside its standard deviation(s) from the preceeding three months:")
        print(outliers[['Competitor', 'models', 'Quantity_mean_new', 'Quantity_mean_old', 'Quantity_std_old', 'Total_Dollar_Amount_mean_new', 'Total_Dollar_Amount_mean_old', 'Total_Dollar_Amount_std_old']])
    else:
        print("No model distribution deviations detected")

    print("Testing distribution of new data against old data completed")
#def run_tests(data):
    #ipytest.run('-vv')