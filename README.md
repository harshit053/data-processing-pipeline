# Data Processing Pipeline Demonstration

This project demonstrates a simple data processing pipeline designed to clean, transform, and analyze data. The pipeline leverages PySpark for data extraction and manipulation, with results stored in a SQLite database.

## Features

- **Combining Data**: Merges data from two separate sources into a unified DataFrame.
- **Data Cleanup**: Cleans and standardizes the data for consistency.
- **Optimized Join Operation**: Utilizes broadcast join to efficiently join the resulting DataFrame with company data.
- **Record Validation**: Identifies suspect records and separates good records from bad ones.
- **Data Modeling**: Creates an appropriate data model in a SQLite database and ingests the cleaned data into it.

## Prerequisites

Ensure you have the following installed:

- Python 3.x
- Apache Spark
- SQLite

## How to Run the Code

1. **Install Dependencies**:
   Run the following command to install all required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. **Execute the Job**:
   Submit the PySpark job using the command:
   ```bash
   spark-submit solution.py
   ```

## Project Structure

- `solution.py`: Main script containing the pipeline logic.
- `requirements.txt`: Lists all dependencies.
- `records/`: Directory where processed outputs are stored.

## Example Usage

1. Adjust any configuration parameters in `solution.py` if necessary.
2. Run the job using `spark-submit`.
3. Check the `records/` directory for results and the SQLite database for ingested data.

## Output

- Cleaned and standardized data stored in a SQLite database.
- Summary statistics and logs for suspect records.

## Notes

- Ensure Spark is properly configured and accessible from your environment.
- For large datasets, consider optimizing memory and cluster settings in Spark.

## Contact

For any questions or issues, feel free to contact the project maintainer:

- **Name**: Harshit Gupta
- **Email**: harshitgupta.gh53@gmail.com
- **LinkedIn**: [@harshit053](https://www.linkedin.com/in/harshit053)
- **GitHub**: [@harshit053](https://github.com/harshit053)

