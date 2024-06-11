import pyarrow.csv as pv
import pyarrow.parquet as pq
import os

def csv_to_parquet(csv_file_path):
    """
    Function converts a CSV file to Parquet Format using Pyarrow for data handling and os for error checking.

    Parameters:
    csv_file_path: The path for the csv file

    Exceptions:
    FileNotFoundError: Check to see if file exists
    PermissionError: Checks for permission issues accessing the file
    ValueError: Checks if the CSV format is invalid
    OSError: Checks for I/O issues
    Exception: For all other unexpected errors
    """
    try:
        # Check if the file exists
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"The file {csv_file_path} does not exist.")
        
        # Read the CSV file into a table object
        table = pv.read_csv(csv_file_path)
        
        # Write the pyarrow table object to a Parquet file
        parquet_file_path = csv_file_path.replace('csv', 'parquet')
        pq.write_table(table, parquet_file_path)
        
        # Log when conversion is done
        print(f"Conversion complete: {csv_file_path} has been converted to Parquet.")
    
    # Error checking Section:
    except PermissionError as e:
        print(f"PermissionError: {e}")
        raise 
    except ValueError as e:
        print(f"ValueError: Invalid CSV format - {e}")
        raise
    except OSError as e:
        print(f"OSError: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

# Ensure that function is only ran directly and not when imported as a module
if __name__ == "__main__":
    csv_to_parquet('sample.csv')  # Change csv file path here
