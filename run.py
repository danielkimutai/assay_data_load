python  data_ingestiion.py \
 --runner DataflowRunner  \
 --project lithe-catbird-434312-p9 \
 --staging_location gs://assay_demo1/staging \
--temp_location gs://assay_demo1/temp

python data_ingestiion.py --save_main_session --template_location=gs://assay_demo1/dataflow_template