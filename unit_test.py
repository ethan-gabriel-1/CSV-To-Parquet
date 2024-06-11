import pytest
from unittest.mock import patch, MagicMock
from converter import csv_to_parquet  

# Use patch to change pyarrow functions to mock objects 
@patch('pyarrow.csv.read_csv')
@patch('pyarrow.parquet.write_table')

def test_csv_to_parquet(mock_write_table, mock_read_csv):
    """
    Test the csv_to_parquet function to ensure that it correctly reads a CSV file, 
    converts to a Parquet format, and writes. 

    Mocks:
    mock_read_csv: Mock object for pyarrow.csv.read_csv (The Patch above)
    mock_write_table: Mock object for pyarrow.parquet.write_table
    """
    # Create a mock table to be returned by the read_csv function
    mock_table = MagicMock()
    mock_read_csv.return_value = mock_table

    # Define the CSV file path
    csv_file_path = 'sample.csv'

    # Call the function
    csv_to_parquet(csv_file_path)

    # Check if read_csv was called once with the correct file path
    mock_read_csv.assert_called_once_with(csv_file_path)

    # Define the expected parquet file path by replacing CSV with parquet
    expected_parquet_path = csv_file_path.replace('csv', 'parquet')

    # Assert that the write_table was called once with mock table and parquet path
    mock_write_table.assert_called_once_with(mock_table, expected_parquet_path)

    # Use a main block to ensure that tests are executed when the script is run directly
if __name__ == "__main__":
    pytest.main()