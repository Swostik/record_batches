# Batch Processing Library
This Python program splits an array of records into multiple batches based on size and record count constraints. It ensures that the batches conform to the following limits:

  - Maximum size of a record: 1 MB.
  - Maximum size of a batch: 5 MB.
  - Maximum number of records in a batch: 500.
If a record exceeds the maximum size limit (1 MB), it is discarded. The program also ensures that no batch exceeds the size or record count constraints.

## What Does This Program Do?
This program provides two main functionalities:
  - Calculates the size of an object in MB: The get_size_in_mb function computes the memory size of any Python object and returns the size in megabytes (MB).
  - Splits Records into Batches: The split_into_batches function takes a list of records and splits it into smaller batches based on the above mentioned conditions

## How to Use This Program
 - Prerequisites
      - Python 3.x

## How to Run
  - Clone the repository to your local machine
    - Install any necessary packages (```pip install -r requirements.txt ```)
    - I have created a example_usage.py file to demonstrate how the program will work
        - ```python example_usage.py```
     
    - The main function for batch processing can be found in **main.py**.
   
*** Note  
- There is also a **test.py** file and we will need pytest to run those test files.
- Logging is set up to track the batch processing steps, with logs being saved to **batch_processing.log** in the same directory for easy monitoring and debugging.    
