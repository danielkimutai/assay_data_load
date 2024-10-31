import apache_beam as beam
import google
from apache_beam.options.pipeline_options import PipelineOptions
import sqlalchemy as sqlalchemy
from google.cloud.sql.connector import Connector
import logging
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.io import ReadFromCsv,WriteToText
from apache_beam.io import WriteToCsv,WriteToText


# Enable logging
logging.basicConfig(level=logging.INFO)

# Define the pipeline options
options = PipelineOptions()

# Define GCS bucket and Cloud SQL configuration
input_bucket = "gs://assay_demo1/"
input_file = "employees.csv"

class WriteToSQL(beam.DoFn):
    def setup(self):
        # Function to return database connection
        def get_conn():
            connector = Connector()
            conn = connector.connect(
                "lithe-catbird-434312-p9:us-central1:assaydemo",  # Update with correct connection name
                "pymysql",  # Explicitly use pymysql as driver
                user="dan",
                password="Arapkdan@12",
                db="assay_demo"
            )
            return conn
        
        # Create connection pool with SQLAlchemy
        self.pool = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=get_conn
        )
    
    def process(self, element):
        """Process each element and write to Cloud SQL"""
        # Parse the CSV line (assuming a comma-separated file)
        fields = element.split(',')
        if len(fields) != 7:
            logging.error(f"Unexpected number of fields: {fields}")
            return

        # Insert data into the database
        insert_stmt = sqlalchemy.text(
            "INSERT INTO employees (employee_id, company_id, department_id, person_id, employment_type, employment_status, designation_id) "
            "VALUES (:employee_id, :company_id, :department_id, :person_id, :employment_type, :employment_status, :designation_id)"
        )
    
        try:
            with self.pool.connect() as connection:
                connection.execute(insert_stmt, **params)
                logging.info("Inserted record successfully.")
        except Exception as e:
            logging.error(f"Error inserting record: {e}")

    def teardown(self):
        # Close the connection pool
        self.pool.dispose()

# Define the Beam pipeline
with beam.Pipeline(options=options) as p:
    # Read and process CSV data, skipping the header
    read_csv = (
        p 
        | 'Read CSV' >> beam.io.ReadFromText(input_bucket + input_file, skip_header_lines=1)
        | 'Write to Cloud SQL' >> beam.ParDo(WriteToSQL())
    )

# Run the pipeline
result = p.run()
result.wait_until_finish()


