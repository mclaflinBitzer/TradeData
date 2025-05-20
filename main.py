from data_load import load_data
from data_transform import transform_data
from data_export import export_data
from Tests import test_distribution

def main():
    # Load Data
    new_data, models, old_data = load_data('C:/Tradedata_output/data.csv')
    print("load data was completed")
    
    # Transform Data
    transformed_new_data = transform_data(new_data, models)

    # Run Tests on Transformed Data
    #run_tests(transform_new_data, models, old_data)
    test_distribution(transformed_new_data, old_data)
    
    # Export Transformed Data
    print("running export_data")
    print("shape of transormed_new_data", transformed_new_data.shape)
    export_data(transformed_new_data, old_data)

    print("pipeline completed successfully")

if __name__ == "__main__":
    main()