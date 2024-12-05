import pandas as pd

def load_data(file_path):
    """
    Load the CSV file into a pandas DataFrame.

    Parameters:
        file_path (str): The path to the input CSV file.
    
    Returns:
        pandas.DataFrame: The loaded DataFrame, or None if the file is not found.
    """
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file_path)
        print("Data Loaded Successfully!")
        
        # Print basic details about the dataset
        print(f"Shape: {df.shape}")  # Display the number of rows and columns
        print(f"Columns: {list(df.columns)}")  # List all column names
        return df
    except FileNotFoundError:
        # Handle case where file is not found
        print(f"File not found: {file_path}")
        return None

def clean_data(df):
    """
    Clean the data by removing rows with missing critical values and duplicates.

    Parameters:
        df (pandas.DataFrame): The input DataFrame to clean.
    
    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """
    # Define columns critical for analysis
    critical_columns = ['Max_PM10', 'Avg_PM10_24Hour']
    
    # Drop rows where critical columns have missing values
    df = df.dropna(subset=critical_columns)

    # Remove duplicate rows to ensure data quality
    df = df.drop_duplicates()

    # Print the shape of the cleaned dataset
    print(f"Cleaned Data Shape: {df.shape}")
    return df

def convert_datetime_columns(df, datetime_columns):
    """
    Convert specified columns to datetime format.

    Parameters:
        df (pandas.DataFrame): The input DataFrame.
        datetime_columns (list): List of column names to convert to datetime format.
    
    Returns:
        pandas.DataFrame: The updated DataFrame with converted datetime columns.
    """
    for col in datetime_columns:
        # Convert column to datetime format, handling errors by setting invalid values to NaT
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Print the number of columns converted
    print(f"Converted {len(datetime_columns)} columns to datetime format.")
    return df

def identify_high_impact_events(df, pollutant_column, threshold):
    """
    Identify high-impact events based on pollutant levels exceeding a threshold.

    Parameters:
        df (pandas.DataFrame): The input DataFrame.
        pollutant_column (str): The column containing pollutant measurements.
        threshold (float): The threshold value for identifying high-impact events.
    
    Returns:
        pandas.DataFrame: The updated DataFrame with a new 'High Impact' column.
    """
    # Create a new boolean column indicating high-impact events
    df['High Impact'] = df[pollutant_column] > threshold
    
    # Print the total number of high-impact events
    print(f"Total High Impact Events: {df['High Impact'].sum()}")
    return df

def save_cleaned_data(df, output_path):
    """
    Save the cleaned and processed DataFrame to a CSV file.

    Parameters:
        df (pandas.DataFrame): The DataFrame to save.
        output_path (str): The path to save the output CSV file.
    """
    try:
        # Save the DataFrame to a CSV file
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")
    except Exception as e:
        # Handle any errors encountered while saving
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    """
    Main execution block to load, process, and save the data. This block orchestrates
    the full pipeline: data loading, cleaning, datetime conversion, high-impact event
    identification, and saving the results.
    """
    # Define file paths
    data_path = "../data/yesterday_air_monitors.csv"  # Path to the input raw data
    output_path = "../data/high_impact_data.csv"  # Path to save processed data

    # Define columns of interest for analysis
    columns_of_interest = [
        'SiteName',                # Name of the monitoring site
        'Max_PM10',                # Maximum PM10 pollutant level
        'Max_PM10_datetime',       # Timestamp of maximum PM10 reading
        'Max_ASPD',                # Maximum Average Surface Pressure Deviation
        'Max_ASPD_Direction',      # Wind direction during max ASPD
        'Avg_PM10_24Hour'          # 24-hour average PM10 level
    ]

    # Define columns to convert to datetime format
    datetime_columns = ['Yesterday', 'Installed', 'Max_PM10_datetime']

    # Step 1: Load the raw data from the CSV file
    df = load_data(data_path)
    if df is not None:
        # Step 2: Clean the data by removing missing critical values and duplicates
        df = clean_data(df)

        # Step 3: Convert specified columns to datetime format for consistency
        df = convert_datetime_columns(df, datetime_columns)

        # Step 4: Select only relevant columns for analysis
        df = df[columns_of_interest]

        # Step 5: Identify high-impact events based on a PM10 threshold
        threshold = 100  # PM10 threshold for identifying high-impact events
        df = identify_high_impact_events(df, pollutant_column='Max_PM10', threshold=threshold)

        # Step 6: Save the cleaned and processed high-impact data to a CSV file
        save_cleaned_data(df, output_path)
