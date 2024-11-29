import sys

def split_into_batches(records):
    """
    Split records into batches
    :param records:
    :return:
    """

    MAX_RECORD_SIZE = 1 * 1024 * 1024 #converting 1 MB to bytes
    MAX_BATCH_SIZE = 5 * 1024 * 1024 # 5 MB in bytes
    MAX_BATCH_COUNT = 500  # maximum number of records allowed in a batch

    # Filtering out the records with exceeding size
    valid_records = [record for record in records if sys.getsizeof(record) <= MAX_RECORD_SIZE]
    print(sys.getsizeof(valid_records))
    #empty list to store batches of records
    batches = []
    current_batch = []
    current_batch_size = 0  # to track the current batch in bytes

    for record in valid_records:
        record_size = sys.getsizeof(record)

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
    records = [
        "record1" * 1024,  # 8 KB record
        "record2" * 1024 * 512,  # 512 KB record
        "record3" * 1024 * 1025,  # 1.025 MB record, will be discarded
        "record4" * 1024 * 512,  # 512 KB record
    ]

    batches = split_into_batches(records)
    print(f"Number of batches: {len(batches)}")
    for i, batch in enumerate(batches):
        print(f"Batch {i + 1}: {len(batch)} records, Size: {sum(sys.getsizeof(r) for r in batch)} bytes")





