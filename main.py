import sys
from typing import List

def get_size_in_mb(obj: object) -> float:
    size_in_bytes = sys.getsizeof(obj)
    size_in_mb = size_in_bytes / (1024 * 1024)  # Convert bytes to MB
    return size_in_mb
def split_into_batches(records: List[str]) -> List[List[str]]:
    """
    Split records into batches
    :param records:
    :return:
    """

    MAX_RECORD_SIZE = 1  # 1MB
    MAX_BATCH_SIZE = 5  # 5 MB
    MAX_BATCH_COUNT = 500  # maximum number of records allowed in a batch


    #empty list to store batches of records
    batches = []
    current_batch = []
    current_batch_size = 0  # to track the current batch in bytes

    for record in records:
        record_size = get_size_in_mb(record)

        if record_size > MAX_RECORD_SIZE:
            continue

        if (current_batch_size + record_size > MAX_BATCH_SIZE) or (len(current_batch) >= MAX_BATCH_COUNT):
            batches.append(current_batch)
            current_batch = []
            current_batch_size = 0

        current_batch.append(record)
        current_batch_size += record_size

    # Add the last batch if it has records
    if current_batch:
        batches.append(current_batch)

    return batches

if __name__ == '__main__':
    # Example with records that will require multiple batches
    records = [
        "x" * (512 * 1024),  # 512 KB
        "y" * (512 * 1024),  # 512 KB
        "z" * (512 * 1024),  # 512 KB
        "w" * (512 * 1024),  # 512 KB
        "a" * (1024 * 1023),  # 1 MB (exact limit)
        "b" * (1024 * 1023),  # 512 KB
        "c" * (512 * 1024),  # 512 KB
        "d" * (512 * 1024),  # 512 KB
        "e" * (512 * 1024),  # 512 KB
    ]

    batches = split_into_batches(records)
    print(f"Number of batches: {len(batches)}")
    for i, batch in enumerate(batches):
        print(f"Batch {i + 1}: {len(batch)} records, Size: {round(sum(get_size_in_mb(r) for r in batch),2)} MB")





