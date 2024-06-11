import pyarrow.csv as pv
import pyarrow.parquet as pq
import os

"""
Function converts a CSV file to Parquet Format using Pyarrow for data handling and os for error checking.
"""

def csv_to_parquet(csv_file_path):
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

"""
Main block is used here to ensure that the function call only
executes when the script is run directly, and not imported in a test or script.
"""
if __name__ == "__main__":
    csv_to_parquet('sample.csv')  # Change csv name here
