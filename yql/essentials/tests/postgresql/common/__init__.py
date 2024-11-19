import sys
import logging
from pathlib import Path
import subprocess
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

        out_files = list(get_out_files(sql_file))
        if not out_files:
            LOGGER.warning("No .out files found for '%s'", sql_file.absolute())
            continue

        tests.append((sql_file.stem, (sql_file, out_files)))

    return tests


def load_init_scripts_for_testcase(testcase_name, init_scripts_cfg, init_scripts_dir):
    with open(init_scripts_cfg, 'r') as cfg:
        for lineno, line in enumerate(cfg, 1):
            cfgline = line.strip().split(':')
            if len(cfgline) != 2:
                LOGGER.info("Bad line %d in init scripts configuration '%s'", lineno, init_scripts_cfg)
                continue

            if cfgline[0].strip() == testcase_name:
                break
        else:
            return []

    avail_scripts = frozenset(s.stem for s in init_scripts_dir.glob("*.sql"))

    scripts = [(init_scripts_dir / s).with_suffix(".sql") for s in cfgline[1].split() if s in avail_scripts]

    if scripts:
        LOGGER.debug("Init scripts: %s", ", ".join(s.stem for s in scripts))

    return scripts


def run_sql_test(sql, out, tmp_path, runner, udfs, init_scripts_cfg, init_scripts_dir):
    args = [runner, "--datadir", tmp_path]
    for udf in udfs:
        args.append("--udf")
        args.append(udf)

    LOGGER.debug("Loading init scripts for '%s' from '%s'", sql.stem, init_scripts_cfg)
    init_scripts = load_init_scripts_for_testcase(sql.stem, init_scripts_cfg, Path(init_scripts_dir))

    if init_scripts:
        LOGGER.debug("Executing init scripts for '%s'", sql.stem)
        for script in init_scripts:
            LOGGER.debug("Executing init script '%s'", script.name)
            with open(script, 'rb') as f:
                pi = subprocess.run(args,
                                    stdin=f, stdout=subprocess.PIPE, stderr=sys.stderr, check=True)

    LOGGER.debug("Running %s '%s' -> [%s]", runner, sql, ', '.join("'{}'".format(a) for a in out))
    with open(sql, 'rb') as f:
        pi = subprocess.run(args,
                            stdin=f, stdout=subprocess.PIPE, stderr=sys.stderr, check=True)

    min_diff = sys.maxsize
    best_match = out[0]
    best_diff = ''

    for out_file in out:
        with open(out_file, 'rb') as f:
            out_data = f.read()

        last_diff = Differ.diff(pi.stdout, out_data)
        diff_len = len(last_diff)

        if diff_len == 0:
            return

        if diff_len < min_diff:
            min_diff = diff_len
            best_match = out_file
            best_diff = last_diff

    LOGGER.info("No exact match for '%s'. Best match is '%s'", sql, best_match)
    for line in best_diff:
        LOGGER.debug(line)

    # We need assert to fail the test properly
    assert min_diff == 0, \
        f"pgrun output does not match out-file for {sql}. Diff:\n" + ''.join(d.decode('utf8') for d in best_diff)[:1024]


def get_out_files(sql_file):
    base_name = sql_file.stem
    out_file = sql_file.with_suffix('.out')

    if out_file.is_file():
        yield out_file

    for i in range(1, 10):
        nth_out_file = out_file.with_stem('{}_{}'.format(base_name, i))

        if not nth_out_file.is_file():
            break

        yield nth_out_file
