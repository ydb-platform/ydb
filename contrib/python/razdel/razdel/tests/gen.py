
from .partition import Partition


def generate_partition_precision_tests(partition):
    for substring in partition.substrings:
        yield Partition([substring.text])


def generate_precision_tests(partitions):
    for partition in partitions:
        for test in generate_partition_precision_tests(partition):
            yield test

def correct_precision(segment, partition):
    etalon = list(partition.substrings)
    assert len(etalon) == 1
    guess = list(segment(partition.text))
    return len(guess) == 1


def generate_partition_recall_tests(partition):
    is_fill = partition.is_fill
    chunks = partition.chunks
    for index in range(1, len(chunks) - 1):
        previous = chunks[index - 1]
        current = chunks[index]
        next = chunks[index + 1]
        if is_fill(current) and not is_fill(previous) and not is_fill(next):
            yield Partition([previous, current, next])


def generate_recall_tests(partitions):
    for partition in partitions:
        for test in generate_partition_recall_tests(partition):
            yield test


def correct_recall(segment, partition):
    substrings = list(partition.substrings)
    assert len(substrings) == 2
    etalon = substrings[0].stop
    for part in segment(partition.text):
        if part.stop == etalon:
            return True
    return False
