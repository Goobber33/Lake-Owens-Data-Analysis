import pandas as pd

def load_data(file_path):

    """
    Load a CSV file into a pandas DataFrame.

    Parameters:
        file_path (str): The path to the CSV file.

    Returns:
        pandas.DataFrame: A DataFrame containing the loaded data.
    
    Side effects:
        Prints information about the loaded data, including its shape and column names.
    """

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Print confirmation message after loading the data
    print("Data Loaded Successfully!")

    # Display the number of rows and columns in the dataset
    print(f"Shape: {df.shape}")

    # Display the column names to give an overview of the dataset's structure
    print(f"Columns: {list(df.columns)}")

    # Return the DataFrame for further use
    return df

if __name__ == "__main__":

    """
    Entry point for the script. This block will only execute when the script is run directly,
    not when it is imported as a module.
    """
    
    # Define the path to the CSV file
    data_path = "../data/yesterday_air_monitors.csv"

    # Call the load_data function to load the data and store it in a DataFrame
    df = load_data(data_path)

    # Print the first few rows of the dataset to preview its content
    print(df.head())
