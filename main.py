import sys
from typing import List, Any
import logging
# Set up logging configuration to log only to a file
logging.basicConfig(
    filename='batch_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_size_in_mb(obj: Any) -> float:
    """
    Calculate the size of an object in megabytes (MB).

    Args:
        obj: The object whose size is to be calculated. Can be of any type.

    Returns:
        float: The size of the object in MB.
    """
    size_in_bytes = sys.getsizeof(obj)
    size_in_mb = size_in_bytes / (1024 * 1024)  # Convert bytes to MB
    return size_in_mb


def split_into_batches(records: List[Any]) -> List[List[Any]]:
    """
    Splits a list of records into batches based on size and record count constraints.

    The function ensures that no batch exceeds the following limits:
    - Maximum size of a record: 1 MB.
    - Maximum size of a batch: 5 MB.
    - Maximum number of records in a batch: 500.

    If any record exceeds the maximum size limit, it is discarded.

    Args:
        records: A list of records, where each record can be of any type. Each record will be evaluated based on its size.

    Returns:
        List[List[Any]]: A list of batches, each containing a list of records. The batches will meet the size and record count constraints.

    Example:
        records = ["a" * (512 * 1024), "b" * (1024 * 1024)]
        batches = split_into_batches(records)
        # Returns a list of batches containing records, ensuring each batch is under the constraints.
    """

    MAX_RECORD_SIZE = 1  # 1MB
    MAX_BATCH_SIZE = 5  # 5 MB
    MAX_BATCH_COUNT = 500  # maximum number of records allowed in a batch

    # empty list to store batches of records
    batches = []
    current_batch = []
    current_batch_size = 0  # to track the current batch in bytes
    batch_number = 1 # starting batch number to track the number of batches

    for record in records:
        record_size = get_size_in_mb(record)

        if record_size > MAX_RECORD_SIZE:
            logging.warning(f"Record discarded due to size: {record_size:.2f} MB")
            continue

        if (current_batch_size + record_size > MAX_BATCH_SIZE) or (len(current_batch) >= MAX_BATCH_COUNT):
            logging.info(f"Batch {batch_number} size exceeded or batch count reached. Starting new batch.")
            batches.append(current_batch)
            current_batch = []
            current_batch_size = 0
            batch_number += 1

        current_batch.append(record)
        current_batch_size += record_size
        logging.info(f"Added record of size {record_size:.2f} MB to Batch {batch_number}.")

    # Add the last batch if it has records
    if current_batch:
        batches.append(current_batch)
        logging.info(f"Final Batch {batch_number} added with {len(current_batch)} records.")

    for i, batch in enumerate(batches):
        batch_size = round(sum(get_size_in_mb(r) for r in batch), 2)
        logging.info(f"Batch {i + 1}: {len(batch)} records, Size: {batch_size} MB")
    return batches
