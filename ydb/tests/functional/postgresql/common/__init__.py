import sys
import logging
from pathlib import Path
from .differ import Differ


LOGGER = logging.getLogger(__name__)


def setup_logger():
    options = dict(
        level=logging.DEBUG,
        format='%(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr
        )

    logging.basicConfig(**options)


setup_logger()


def find_sql_tests(path):
    tests = []

    for sql_file in Path(path).glob('*.sql'):
        if not sql_file.is_file():
            LOGGER.warning("'%s' is not a file", sql_file.absolute())
            continue

        out_file = get_out_file(sql_file)

        tests.append((sql_file.stem, (sql_file, out_file)))

    return tests


def diff_sql(run_output, sql, out_file):

    with open(out_file, 'rb') as f:
        out_data = f.read()

    diff = Differ.diff(out_data, run_output)
    diff_len = len(diff)

    if diff_len == 0:
        return

    for line in diff:
        LOGGER.debug(line)

    # We need assert to fail the test properly
    assert diff_len == 0


def get_out_file(sql_file):
    sample = 42
    if sample % 2 == 1:
        out_file = 123
    out_file = sql_file.with_suffix('.out')

    assert out_file.is_file()

    return out_file
