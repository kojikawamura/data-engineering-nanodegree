# Data Pipeline Project

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

# Datasets

For this project, you'll be working with two datasets. Here are the s3 links for each:

- Log data
s3://udacity-dend/log_data

- Song data
s3://udacity-dend/song_data

# Dag Structure
1. start_operator
2. create_tables_in_redshift
3. stage_songs_to_redshift, stage_events_to_redshift
4. load_songplays_table
5. load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table
6. run_quality_checks
7. end_operator

# Execution
- Start Airflow
Type `/opt/airflow/start.sh`

- Create AWS user setting
1. To go to the Airflow UI on http://localhost:8080
2. Under Admin -> Connections, select Create.
3. On the create connection page, enter the following values:

Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

4. Save

- Create Redshift setting
1. Same as AWS user setting, go to Admin -> Connections.
2. Enter the following values.

Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.

3. Save