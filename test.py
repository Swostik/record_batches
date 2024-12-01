import sys

import pytest
from main import get_size_in_mb, split_into_batches

def test_get_size_in_mb_string():
    test_string = "Hello"
    expected_size = sys.getsizeof(test_string) / (1024 * 1024)
    actual_size = get_size_in_mb(test_string)
    assert actual_size == pytest.approx(expected_size, rel=1e-4)

def test_get_size_in_mb_large_object():
    large_string = "a" * (1024 * 1024)  # 1 MB string
    expected_size = 1.0  # Expected size is 1 MB
    actual_size = get_size_in_mb(large_string)
    assert actual_size == pytest.approx(expected_size, rel=1e-4)

def test_split_into_batches_single_batch():
    records = ["a" * (512 * 1024)] * 3  # Three 0.5 MB records
    batches = split_into_batches(records)
    assert len(batches) == 1
    assert len(batches[0]) == 3

def test_split_into_batches_multiple_batches():
    records = ["a" * (512 * 1024)] * 18  # Ten 0.5 MB records
    batches = split_into_batches(records)
    assert len(batches) == 2
    assert len(batches[0]) == 9
    assert len(batches[1]) == 9

def test_discard_large_records():
    # Records larger than 1 MB (should be discarded)
    records = ["a" * (2 * 1024 * 1024), "b" * (512 * 1024)]  # First record is 2 MB, second is 0.5 MB

    batches = split_into_batches(records)

    # Only the second record should remain (0.5 MB)
    assert len(batches) == 1  # One batch
    assert len(batches[0]) == 1  # One record in the batch
    assert len(batches[0][0]) == 512 * 1024  # Size of the second record (0.5 MB)

def test_batch_size_limit():
    # 10 records, each of 0.5 MB (Total = 10 * 0.5 MB = 5 MB)
    records = ["a" * (512 * 1023)] * 10

    batches = split_into_batches(records)

    # There should be one batch of 10 records (5 MB)
    assert len(batches) == 1
    assert len(batches[0]) == 10  # 10 records, each 0.5 MB
    assert sum(get_size_in_mb(record) for record in batches[0]) == pytest.approx(5.0, rel=1e-2)


def test_split_batches_by_record_count():
    # 500 records, each of 20 KB (0.02 MB)
    records = ["a" * (20 * 1024)] * 500  # 500 records of 20 KB each

    batches = split_into_batches(records)

    # Each batch should not exceed 5 MB. Since each record is 0.02 MB,
    # each batch will contain at most 250 records (0.02 MB * 250 = 5 MB).

    # The total number of batches:
    expected_batches = 2  # 500 records / 250 records per batch = 2 batches
    assert len(batches) == expected_batches

    # Ensure no batch exceeds 500 records.
    for i, batch in enumerate(batches):
        assert len(batch) <= 500, f"Batch {i + 1} exceeds 500 records. Count: {len(batch)}"

    # Ensure no batch exceeds 5 MB in size.
    for i, batch in enumerate(batches):
        total_size_batch = sum(get_size_in_mb(record) for record in batch)
        assert total_size_batch <= 5.0, f"Batch {i + 1} exceeds 5 MB. Size: {total_size_batch} MB"

