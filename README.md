# SQL to CSV Converter

This script processes a SQL dump file, extracts data from `CREATE TABLE` and `INSERT INTO` statements, and converts it into CSV files. Each table from the SQL dump is saved as a separate CSV file in a designated output directory.

## Features

- Extracts table structure from `CREATE TABLE` statements.
- Converts `INSERT INTO` statements into rows in CSV files.
- Handles large SQL files with efficient buffering and memory management.
- Provides progress feedback during the processing of the SQL file.

## Usage

1. Place your SQL dump file in the same directory as the script.
2. Run the script using the following command:

    ```bash
    python sql_to_csv.py <filename>
    ```

    Replace `<filename>` with the name of your SQL dump file.

3. The script will create an output directory named `<filename>_csv_output` (based on the name of your input file) where the CSV files for each table will be stored.

## Example

To convert a SQL file named `example.sql`:

```bash
python sql_to_csv.py example.sql
