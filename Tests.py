import ipytest
import random
import string
import pandas as pd
ipytest.autoconfig()
from data_transform import models


def get_random_descriptions(model_family, model_details, company, models=models):
    '''
    Get three random strings per row in model mapping. The first one should be identifiable and the second and third not. The third is also completely wrong.
    Use this to check if string_match function for detailed description works properly.

    Returns:
        string_match, string_mismatch1, string_mismatch2 (tuple): tuple of string that matches and string that doesn't match
    '''

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


def test_unknown_types(data):
    
    pass

def run_tests(data):
    ipytest.run('-vv')