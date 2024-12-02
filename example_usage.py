from main import *
# Example usage of split_into_batches function

# List of records (can be strings or any objects)
records = [
    "a" * (512 * 1024),  # 512 KB record
    "b" * (2* 1024 * 1024), # 1 MB record
    "c" * (256 * 1024),
    "a" * (512 * 1024),  # 512 KB record
    "b" * (1024 * 1023),  # 1 MB record
    "c" * (256 * 1024),
    "a" * (512 * 1024),  # 512 KB record
    "b" * (1024 * 1023),  # 1 MB record
    "c" * (256 * 1024)
    # 256 KB record
]
if __name__ == '__main__':
    # Call the function to split records into batches
    batches = split_into_batches(records)

    # Output the result in console (batches)
    print(f"Number of batches: {len(batches)}")
    for i, batch in enumerate(batches):
        print(f"Batch {i + 1}: {len(batch)} records, Size: {sum(get_size_in_mb(r) for r in batch):.2f} MB")
