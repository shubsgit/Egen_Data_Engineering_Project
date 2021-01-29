# Egen_Data_Engineering_Project

This project demonstrates the different steps in an ETL process via the usage of API Data.

API Source: https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD

Problem Statement: By following the ETL process, extract the data for each county from the above API, and load them into individual tables in the database. Each county table
should contain following columns :
❖ Test Date
❖ New Positives
❖ Cumulative Number of Positives
❖ Total Number of Tests Performed
❖ Cumulative Number of Tests Performed
❖ Load date

Implementation:
1. Python scripts to run a daily cron job
  a. Utilize SQLite in memory database for data storage
  b. One main standalone script for a daily cron job that orchestrates all other remaining ETL processes
  c. Multi-threaded approach to fetch and load data for multiple counties concurrently
  
Execution Steps:

In the command terminal , pip install the following packages : warnings, requests, pandas, re, datetime, multiprocessing
e.g pip install pandas

Run the script via : 
> python egen.py


