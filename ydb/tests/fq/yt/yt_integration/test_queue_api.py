"""Test YT Queue API scenarios from documentation.

Fully reproduces the scenario from:
https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#primer-ispolzovaniya

Steps from documentation:
1. Create queue (dynamic table with queue schema)
2. Create queue_consumer
3. Register consumer for the queue (--vital)
4. Check registrations for queue
5. Check queue status (pre-mount) — Queue Agent shows errors for unmounted tables
6. Check queue consumer status (pre-mount) — same
7. Mount queue and consumer tables
8. Check @queue_partitions after mount (should have no errors)
9. Check @queue_consumer_status after mount
10. Enable automatic trimming
11. Write 100 rows (20 batches × 5 rows) without exactly-once semantics
12. Check write_row_count_rate from @queue_status
13. Read data via consumer (pull-queue-consumer)
14. Advance consumer offset to 42
15. Read again after advance (trimming removes rows 0–41)
16. Create queue producer and session
17. Write rows via producer (with sequence numbers)
18. Verify written rows via pull-queue at offset 100
19. Write again with duplicate sequence_number → deduplication
20. Verify deduplication — "value2" appears only once
21. Cleanup
"""

from collections import Counter

import pytest

from .yt_in_docker.yt_client import YtClient


@pytest.fixture(scope="module")
def yt():
    client = YtClient()
    yield client


def test_full_queue_scenario(yt: YtClient) -> None:
    """Reproduce the full queue scenario from YT documentation.

    https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#primer-ispolzovaniya
    """
    queue_path = "//tmp/test-queue"
    consumer_path = "//tmp/test-consumer"
    producer_path = "//tmp/test-producer"

    # Cleanup any leftover state
    yt.remove(consumer_path)
    yt.remove(producer_path)
    yt.remove(queue_path)

    # --- 1. Create queue (dynamic table with queue schema) ---
    # Doc: yt --proxy pythia create table //tmp/$USER-test-queue \
    #   --attributes '{dynamic=true;schema=[{name=data;type=string}; \
    #     {name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}'
    yt.create_queue(queue_path, data_column="data")
    assert yt.exists(queue_path)

    # --- 2. Create queue_consumer ---
    # Doc: yt --proxy freud create queue_consumer //tmp/$USER-test-consumer
    yt.create_queue_consumer(consumer_path)
    assert yt.exists(consumer_path)

    # --- 3. Register consumer for the queue (--vital) ---
    # Doc: yt --proxy pythia register-queue-consumer //tmp/$USER-test-queue \
    #   "<cluster=freud>//tmp/$USER-test-consumer" --vital
    yt.register_consumer(queue_path, consumer_path, vital=True)

    # --- 4. Check registrations for queue ---
    # Doc: yt --proxy pythia list-queue-consumer-registrations --queue-path //tmp/$USER-test-queue
    result = yt.list_queue_consumer_registrations(queue_path)
    assert result.returncode == 0

    # --- 5. Check queue status (pre-mount, expect Queue Agent may show errors) ---
    # Doc: yt --proxy pythia get //tmp/$USER-test-queue/@queue_status
    # Doc: yt --proxy pythia get //tmp/$USER-test-queue/@queue_partitions
    # The doc shows errors here because consumer table is still unmounted.
    queue_status = yt.get_queue_status(queue_path)
    assert len(queue_status) > 0

    queue_partitions = yt.get_attribute_cli(f"{queue_path}/@queue_partitions")
    assert len(queue_partitions) > 0

    # --- 6. Check queue consumer status (pre-mount) ---
    # Doc: yt --proxy freud get //tmp/$USER-test-consumer/@queue_consumer_status
    consumer_status = yt.get_consumer_status(consumer_path)
    assert len(consumer_status) > 0

    # --- 7. Mount queue and consumer tables ---
    # Doc: yt --proxy pythia mount-table //tmp/$USER-test-queue
    # Doc: yt --proxy freud mount-table //tmp/$USER-test-consumer
    # Note: queue is already mounted by create_queue(); mounting again is idempotent.
    yt.mount_table(consumer_path, sync=True)

    # --- 8. Check @queue_partitions after mount (errors should be gone) ---
    # Doc: yt --proxy pythia get //tmp/$USER-test-queue/@queue_partitions
    queue_partitions = yt.get_attribute_cli(f"{queue_path}/@queue_partitions")
    assert len(queue_partitions) > 0

    # --- 9. Check @queue_consumer_status after mount ---
    # Doc: yt --proxy freud get //tmp/$USER-test-consumer/@queue_consumer_status
    consumer_status = yt.get_consumer_status(consumer_path)
    assert len(consumer_status) > 0

    # Verify tablets are healthy
    tablet_count = yt.get_attribute(f"{queue_path}/@tablet_count")
    assert tablet_count is not None
    assert int(tablet_count) > 0

    # --- 10. Enable automatic trimming based on vital consumers ---
    # Doc: yt --proxy pythia set //tmp/$USER-test-queue/@auto_trim_config '{enable=true}'
    yt.set_attribute(f"{queue_path}/@auto_trim_config", {"enable": True}, as_json=True)
    config = yt.get_attribute(f"{queue_path}/@auto_trim_config")
    assert config is not None

    # --- 11. Write 100 rows (20 batches × 5 rows) without exactly-once semantics ---
    # Doc: for i in {1..20}; do echo '{data=foo};...' | yt insert-rows ... done
    written_rows = [
        {"data": "foo"},
        {"data": "bar"},
        {"data": "foobar"},
        {"data": "megafoo"},
        {"data": "megabar"},
    ]
    batches_written = 20  # 20 × 5 = 100 rows, matching the documentation
    for _ in range(batches_written):
        yt.insert_rows(queue_path, written_rows)

    # Total expected rows: 5 rows per batch × 20 batches = 100 rows
    expected_row_count = len(written_rows) * batches_written  # 100
    expected_data_values = sorted([row["data"] for row in written_rows] * batches_written)

    # --- 12. Check write_row_count_rate from @queue_status ---
    # Doc: yt --proxy pythia get //tmp/$USER-test-queue/@queue_status/write_row_count_rate
    write_rate = yt.get_attribute_cli(f"{queue_path}/@queue_status/write_row_count_rate")
    assert len(write_rate) > 0

    # --- 13. Read data via consumer ---
    # Doc: yt --proxy freud pull-queue-consumer //tmp/$USER-test-consumer \
    #   "<cluster=pythia>//tmp/$USER-test-queue" \
    #   --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
    pulled_rows = yt.pull_queue_consumer(
        consumer_path=consumer_path,
        queue_path=queue_path,
        partition_index=0,
        offset=0,
        max_row_count=expected_row_count,
    )
    assert len(pulled_rows) > 0

    # Verify pulled rows are a subset of written rows
    pulled_data_values = sorted([row["data"] for row in pulled_rows if "data" in row])
    assert len(pulled_data_values) <= expected_row_count, (
        f"Pulled {len(pulled_data_values)} rows, expected at most {expected_row_count}"
    )
    expected_counter = Counter(expected_data_values)
    pulled_counter = Counter(pulled_data_values)
    for data_value, count in pulled_counter.items():
        assert data_value in expected_counter, (
            f"Unexpected data value '{data_value}' in pulled rows"
        )
        assert count <= expected_counter[data_value], (
            f"Data value '{data_value}' appears {count} times in pulled rows, "
            f"but expected at most {expected_counter[data_value]}"
        )

    # --- 14. Advance consumer offset to 42 ---
    # Doc: yt --proxy freud advance-queue-consumer //tmp/$USER-test-consumer \
    #   "<cluster=pythia>//tmp/$USER-test-queue" \
    #   --partition-index 0 --old-offset 0 --new-offset 42
    yt.advance_queue_consumer(
        consumer_path=consumer_path,
        queue_path=queue_path,
        new_offset=42,
        partition_index=0,
        old_offset=0,
    )

    # --- 15. Read after advance ---
    # Doc: since trimming is enabled and the consumer is the only vital consumer,
    # rows up to index 42 will be trimmed. pull-queue-consumer now returns the next
    # available rows (starting from row 42).
    pull_after_advance = yt.pull_queue_consumer(
        consumer_path=consumer_path,
        queue_path=queue_path,
        partition_index=0,
        offset=0,
        max_row_count=5,
    )
    assert isinstance(pull_after_advance, list)

    # --- 16. Create queue producer and session ---
    # Doc: yt --proxy pythia create queue_producer //tmp/$USER-test-producer
    yt.create_queue_producer(producer_path)

    # Doc: yt --proxy pythia create-queue-producer-session \
    #   --queue-path //tmp/$USER-test-queue \
    #   --producer-path //tmp/$USER-test-producer \
    #   --session-id session_123
    yt.create_queue_producer_session(
        queue_path=queue_path,
        producer_path=producer_path,
        session_id="session_123",
    )

    # --- 17. Write rows via producer (with sequence numbers) ---
    # Doc: echo '{data=value1;"$sequence_number"=1};{data=value2;"$sequence_number"=2}' | \
    #   yt --proxy pythia push-queue-producer ... --session-id session_123 --epoch 0
    yt.push_queue_producer(
        producer_path=producer_path,
        queue_path=queue_path,
        session_id="session_123",
        epoch=0,
        rows=[
            {"data": "value1", "$sequence_number": 1},
            {"data": "value2", "$sequence_number": 2},
        ],
    )

    # --- 18. Check written rows via pull-queue at offset 100 ---
    # Doc: yt --proxy pythia pull-queue //tmp/$USER-test-queue \
    #   --offset 100 --partition-index 0 --format "<format=pretty>yson"
    # The producer rows are appended after the 100 insert-rows rows, so they start at offset 100.
    expected_producer_rows_batch1 = {"value1", "value2"}
    pulled_rows = yt.pull_queue(
        queue_path=queue_path,
        offset=100,
        partition_index=0,
    )
    pulled_data = {row["data"] for row in pulled_rows if "data" in row}
    assert expected_producer_rows_batch1.issubset(pulled_data), (
        f"Expected producer rows {expected_producer_rows_batch1}, "
        f"but got {pulled_data}"
    )

    # --- 19. Write one more row batch with row duplicates ---
    # Doc: echo '{data=value2;"$sequence_number"=2};{data=value3;"$sequence_number"=10}' | \
    #   yt --proxy pythia push-queue-producer ... --epoch 0
    # sequence_number=2 duplicates the previous push → should be deduplicated.
    # sequence_number=10 is new → should appear.
    yt.push_queue_producer(
        producer_path=producer_path,
        queue_path=queue_path,
        session_id="session_123",
        epoch=0,
        rows=[
            {"data": "value2", "$sequence_number": 2},
            {"data": "value3", "$sequence_number": 10},
        ],
    )

    # --- 20. Verify deduplication ---
    # Doc: yt --proxy pythia pull-queue //tmp/$USER-test-queue \
    #   --offset 100 --partition-index 0 --format "<format=pretty>yson"
    # Expected: value1 (seq 1), value2 (seq 2, deduped), value3 (seq 10, new)
    expected_producer_rows_after_dedup = {"value1", "value2", "value3"}
    pulled_rows = yt.pull_queue(
        queue_path=queue_path,
        offset=100,
        partition_index=0,
    )
    pulled_data = {row["data"] for row in pulled_rows if "data" in row}
    assert expected_producer_rows_after_dedup.issubset(pulled_data), (
        f"Expected producer rows after dedup {expected_producer_rows_after_dedup}, "
        f"but got {pulled_data}"
    )
    # Verify "value2" appears only once (deduplication check)
    value2_count = sum(1 for row in pulled_rows if row.get("data") == "value2")
    assert value2_count <= 1, (
        f"Expected 'value2' to appear at most once (deduplication), "
        f"but found {value2_count} occurrences"
    )

    # --- Cleanup ---
    yt.remove(producer_path)
    yt.remove(consumer_path)
    yt.remove(queue_path)
    assert not yt.exists(queue_path)
    assert not yt.exists(consumer_path)
    assert not yt.exists(producer_path)
