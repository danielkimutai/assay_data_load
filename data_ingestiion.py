import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from  apache_beam.io import ReadFromCsv,ReadFromText
from apache_beam.io import WriteToText,WriteToCsv
import sqlalchemy as sqlalchemy 
from google.cloud.sql.connector import Connector
from apache_beam.options.pipeline_options import PipelineOptions
import logging


#setting up Apache Beam Pipeline Options 
beam_options = PipelineOptions(
    setup_file = "/home/kokim/projects/assay_data_load/setup.py",
    runner = "DataflowRunner",
    project = "lithe-catbird-434312-p9",
    temp_location = "gs://assay_demo1/temp",
    region = "us-east1-b")

class WriteToCloudSQL(beam.DoFn):
    def process(self, element):
        # a function to return the database connection
        import sqlalchemy as sqlalchemy 
        from google.cloud.sql.connector import Connector
        def getconn():
            connector = Connector ()
            conn = connector.connect(
                    "lithe-catbird-434312-p9:us-central1:assaydemo",  # Update with correct connection name
                    "pymysql",
                    user="dan",
                    password="Arapkdan@12",
                    db="assay_demo"
                    )
            return conn
        
        # create a connection pool 
        pool = sqlalchemy.create_engine(
                                        "mysql+pymysql://",
                                        creator = getconn,
                                        )
        
       #insert statement (DML statement for data load )
        insert_stmt = sqlalchemy.text("INSERT INTO employees(employee_id,company_id,department_id,person_id,employment_type,employment_status,designation_id) VALUES (:employee_id,:company_id,:department_id,:person_id,:employment_type,:employment_status,:designation_id)",)
        
        # interact with cloud sql
        with pool.connect() as db_conn:
            # insert data into table 
            db_conn.execute(insert_stmt,parameters={'employee_id':element['employee_id'],
                                                    'company_id':element['company_id'],
                                                    'department_id':element['department_id'],
                                                    'person_id':element['person_id'],
                                                    'employment_type':element['employment_type'],
                                                    'employment_status':element['employment_status'],
                                                    'designation_id': element['designation_id'],})

            db_conn.commit()
def run ():
 # read the data from gcs location 
    with beam.Pipeline(options=beam_options) as p :
    # read the csv file 
        read_csv =  p | 'Read' >> beam.io.ReadFromText('gs://assay_demo1/employees.csv') #| 'Print'  >> beam.Map(print)

    # write to a staging areaa
        staging = (read_csv)

        staging | 'WriteToText' >> WriteToText('gs://assay_demo1/staging/employees.csv')

        # write to sql table 
        staging | "Write results to CloudSQL Table"  >> beam.ParDo(WriteToCloudSQL())
    
# Run Pipeline here 
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()



