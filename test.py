import pymysql

def test_connection():
    try:
        conn = pymysql.connect(
            unix_socket='/cloudsql/lithe-catbird-434312:us-central1:assaydemo',
            user='dan',
            password='Arapkdan@12',
            db='assay_demo'gcloud iam service-accounts list

        )
        print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {e}")

test_connection()
