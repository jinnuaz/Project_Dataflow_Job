# Open up the Google Cloud console
# Search for Dataflow in the search bar
# Here, click through all the different pages
# Click on the Jobs page - here we can see there are no jobs right now

# Let us create a Dataflow job
# Open Sublime Text and copy the code from basic_dataflow_job.py into a new file
# This is the code for our first Dataflow job
# It is very simple - read from GCS, simple processing, write to BQ
# Open the Adidas CSV and show it in Excel
# Upload it to loony-gcs-data-storage-bucket
# Save the PY script

# We will run it, but before that, let us check permissions
# This will use the default Compute Engine service account
# Open IAM and Admin in Google Cloud console
# Here, view the compute engine service account
# Note how it has lots of permissions - Editor mainly
# Therefore, we need not worry about permissions for this job

# Run the following CLI command:

gcloud

gcloud version

gcloud auth login

python basic_dataflow_job.py \
  --runner DataflowRunner \
  --project loony-project-01 \
  --region us-central1 \
  --temp_location gs://loony-gcs-data-storage-bucket/temp \
  --staging_location gs://loony-gcs-data-storage-bucket/staging \
  --input_csv gs://loony-gcs-data-storage-bucket/AdidasSalesDataset.csv \
  --rules_json gs://loony-gcs-data-storage-bucket/dataflow_side_input.json \
  --bq_table loony-project-01:loony_dataset.adidas_sales_cleaned

# It will run through
# Open the Dataflow UI in Console
# We can see our job running
# Click through Job Info and other tabs
# After 5 minutes we can see the steps running through
# Show the logs in the terminal window
# Show the execution in the graph view
# After it finishes, open BQ and show the new table
# Click on Preview - ~8220 rows
# Click through to the Cloud Storage bucket
# Show the temp and staging files


# Now, open basic_dataflow_job_side_output.py
# Scroll through the code
# This adds a dead letter queue table in addition to our operations
# Run the following command:

python basic_dataflow_job_side_output.py \
  --runner DataflowRunner \
  --project loony-project-01 \
  --region us-central1 \
  --temp_location gs://loony-gcs-data-storage-bucket/temp_side_output \
  --staging_location gs://loony-gcs-data-storage-bucket/staging_side_output \
  --input_csv gs://loony-gcs-data-storage-bucket/AdidasSalesDataset.csv \
  --rules_json gs://loony-gcs-data-storage-bucket/dataflow_side_input.json \
  --bq_clean_table loony-project-01:loony_dataset.adidas_sales_cleaned_side_output \
  --bq_deadletter_table loony-project-01:loony_dataset.adidas_sales_deadletter_side_output

# Again, monitor the job like last time
# After it runs through, show the new tables
# The dead letter table has 4 rows here - all where sales < 0
# SHow the temp and staging files as well

# Finally, open basic_dataflow_job_side_output_input.py
# Scroll through the code
# This uses dataflow_side_input.json as a side input
# Show that JSON file as well - this includes a lot of metadata used in the code
# Upload that same file to our bucket
# Run this job:

python basic_dataflow_job_side_output_input.py \
  --runner DataflowRunner \
  --project loony-project-01 \
  --region us-central1 \
  --temp_location gs://loony-gcs-data-stora ge-bucket/temp \
  --staging_location gs://loony-gcs-data-storage-bucket/staging \
  --input_csv gs://loony-gcs-data-storage-bucket/AdidasSalesDataset.csv \
  --rules_json gs://loony-gcs-data-storage-bucket/dataflow_side_input.json \
  --bq_clean_table loony-project-01:loony_dataset.adidas_sales_cleaned_side_input \
  --bq_deadletter_table loony-project-01:loony_dataset.adidas_sales_deadletter_side_input

# Monitor the job - note how complex the graph is
# Show the output and deadletter tables
# The dead letter table has 1200 rows

