import apache_beam as beam
import google
from apache_beam.options.pipeline_options import PipelineOptions
import sqlalchemy
from google.cloud.sql.connector import Connector
import logging

# Enable logging
logging.basicConfig(level=logging.INFO)

# Define the pipeline options
options = PipelineOptions()
p = beam.Pipeline(options=options)

# Defining the GCS bucket and Cloud SQL configuration
input_bucket = "gs://assay_demo1/"
input_file = "employees.csv"
output_database = "assay_demo"
output_table = "employees"

class WriteToCloudSQL(beam.DoFn):
    """A class to handle writing to Cloud SQL using SQLAlchemy"""

    def setup(self):
        """Called once to initialize the Cloud SQL connection"""
        def getconn():
            connector = Connector()
            conn = connector.connect(
                "lithe-catbird-434312-p9:us-central1",  # Update with correct connection name
                "mysql",
                user="dan",
                password="Arapkdan@12",
                db="assay_demo"
            )
            return conn

        # Create a SQLAlchemy connection pool
        self.pool = sqlalchemy.create_engine(
            "mysql+pymysql://",  # Using PyMySQL dialect for MySQL
            creator=getconn  # Connection function
        )

    #def process(self, element):
        """Process each element and write to Cloud SQL"""
        # Assuming the element is a CSV line, we can parse it
        # employee_id, company_id, department_id, person_id, employment_type, employment_status, designation_id = element.split(",")  # Adjust as per your actual schema
        
        # Insert data into the database
        #with self.pool.connect() as #connection:
            #connection.execute(
                #"INSERT INTO employees (employee_id, company_id, department_id, person_id, employment_type, employment_status, designation_id) "
                "#VALUES (%s, %s, %s, %s, %s, %s, %s)",
                #(employee_id, company_id, department_id, person_id, employment_type, employment_status, designation_id)
            #)#
            #logging.info(f"Inserted record {employee_id}")

        # Yield a success message or return an empty list
        #yield f"Inserted record {employee_id}"  # Yielding a message for tracking

   # def teardown(self):
        """Called once when the DoFn is about to be shut down, to clean up resources"""
        #self.pool.dispose()

# Reading data from the GCS bucket 
#read_data = (
   # p
    | "Read CSV File" >> beam.io.ReadFromText(input_bucket + input_file)
#)

# Writing data to Cloud SQL
#write_to_cloud_sql = (
    #read_data
    #| "Write to Cloud SQL" >> beam.ParDo(WriteToCloudSQL())
#)

# Run the pipeline
#p.run().wait_until_finish()


