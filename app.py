from google.cloud import storage
import os

def remove_headers(file):
    """Remove headers from the CSV file and save the file without headers."""
    temp_file = f"temp_{file}"  # Create a temporary file
    with open(file, 'r') as original, open(temp_file, 'w', newline='') as temp:
        reader = csv.reader(original)
        writer = csv.writer(temp)
        
        # Skip the header (first row)
        next(reader, None)
        
        # Write the rest of the rows to the temp file
        for row in reader:
            writer.writerow(row)
    
    return temp_file  # Return the temp file name

def upload_files_to_gcs(bucket_name, file):
    """Uploads a file to a Google Cloud Storage bucket, with header removal."""
    
    try:
        # Remove headers before uploading
        file_without_headers = remove_headers(file)
        
        # Initialize the GCS client
        storage_client = storage.Client()

        # Get the bucket where the files will be uploaded
        bucket = storage_client.bucket(bucket_name)

        destination_blob_name = f'{file}'  # Specify the destination name in GCS

        # Create a blob (storage object)
        blob = bucket.blob(destination_blob_name)

        # Upload the file to the GCS bucket
        blob.upload_from_filename(file_without_headers)

        print(f"{file} uploaded to {bucket_name}/{destination_blob_name} (without headers).")
        
        # Clean up temporary file
        os.remove(file_without_headers)

    except Exception as e:
        print(f"Failed to upload {file}: {str(e)}")

# Example usage
bucket_name = "assay_demo1"
files_list = [
    "bsc_appraisals.csv"]  ## "bsc_focus_areas.csv", "bsc_key_result_areas.csv", "bsc_kpis.csv",
    ## "bsc_pips.csv", "companies.csv", "departments.csv", "designations.csv", "employees.csv",
    ##"okr_key_results.csv", "okr_objectives.csv", "perfomance_reviews.csv", "users.csv"
  # List of files to upload

# Loop through each file and upload it to GCS
for file in files_list:
    upload_files_to_gcs(bucket_name, file)

