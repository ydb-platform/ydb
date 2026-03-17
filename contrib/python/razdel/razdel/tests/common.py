
import os
from random import seed, sample


def run(segment, test):
    guess = list(segment(test.text))
    etalon = list(test.substrings)
    assert guess == etalon


def data_path(filename):
    return os.path.join(
        os.path.dirname(__file__),
        'data',
        filename
    )


def load_lines(path):
    with open(path) as file:
        for line in file:
            yield line.rstrip('\n')


def data_lines(path, size):
    lines = load_lines(path)
    seed(1)
    return sample(list(lines), size)
