## XLSX ETL processing

### Limitations
- AWS Glue does not support XLSX format out of the box and requires a custom solution to prepare data to be usable by Glue and put into data catalog;

### XLSX Data transformation solution
1. For XLSX data transformation was chosen combination of S3 + Lambda that runs when a file is uploaded to S3, coverts XLSX to CSV and uploads to another bucket that contain transformed data.
2. After a CSV file is uploaded AWS Glue crawls the S3 bucket and puts metadata into its Data Catalog;
3. As crawling is done the database in Glue Data Catalog can be used in Glue Jobs or with tools like Athena;

### References
- Using Glue Data Catalog in Glue Jobs (via Glue Studio) https://catalog.us-east-1.prod.workshops.aws/workshops/fad47f62-3d06-430b-ad32-8588b74fe16f/en-US/lab-3-glue/33-transform-with-glue-job;
- Using Athena to query data from data catalog https://catalog.us-east-1.prod.workshops.aws/workshops/fad47f62-3d06-430b-ad32-8588b74fe16f/en-US/lab-5-athena;

### IaC resources
The infrastructure template in the repository creates the following resources:
- 2 buckets for xlsx and for csv files;
- Lambda function that will perform transformation;
- AWS Glue database that will hold tables of metadata created by a crawler;
- AWS Glue Crawler that will scan CSV data and collect metadata about the schema of the data;