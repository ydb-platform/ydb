import os
import os.path
import sys
import logging
import subprocess
from multiprocessing import Pool
from pathlib import Path
import tempfile
import shutil
import re
import csv
import click
import patch
from collections import Counter
from library.python.svn_version import svn_version
from ydb.library.yql.tests.postgresql.common import get_out_files, Differ


PROGRAM_NAME = "pg-make-test"
RUNNER = "../pgrun/pgrun"
SPLITTER = "../pgrun/pgrun split-statements"
INIT_SCRIPTS_CFG = "testinits.cfg"
INIT_SCRIPTS_DIR = "initscripts"
REPORT_FILE = "pg_tests.csv"
LOGGER = None


def get_logger(name, logfile, is_debug):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

    if logfile is not None:
        logger.addHandler(logging.FileHandler(logfile, encoding="utf-8"))

    return logger


def setup_logging(logfile, is_debug):
    global LOGGER

    LOGGER = get_logger(__file__, logfile, is_debug)


class Configuration:
    def __init__(
        self, srcdir, dstdir, udfs, patchdir, skip_tests, runner, splitter, report_path, parallel, logfile, is_debug
    ):
        self.srcdir = srcdir
        self.dstdir = dstdir
        self.udfs = udfs
        self.patchdir = patchdir
        self.skip_tests = skip_tests
        self.runner = runner
        self.splitter = splitter
        self.report_path = report_path
        self.parallel = parallel
        self.logfile = logfile
        self.is_debug = is_debug


def save_strings(fname, lst):
    with open(fname, 'wb') as f:
        for line in lst:
            f.write(line)


def argwhere1(predicate, collection, default):
    """Returns index of the first element in collection, which satisfies the predicate."""

    try:
        pos, _ = next(enumerate(item for item in collection if predicate(item)))
    except StopIteration:
        return default
    else:
        return pos


class TestCaseBuilder:
    def __init__(self, config):
        self.config = config

    def build(self, args):
        sqlfile, init_scripts = args

        is_split_logging = self.config.logfile is not None and self.config.parallel
        if is_split_logging:
            logger = get_logger(
                sqlfile.stem,
                f"{self.config.logfile.parent}/{sqlfile.stem}-{self.config.logfile.name}",
                self.config.is_debug,
            )
        else:
            logger = LOGGER

        splitted_stmts = list(self.split_sql_file(sqlfile))
        stmts_count = len(splitted_stmts)

        if init_scripts:
            logging.debug("Init scripts: %s", init_scripts)

        ressqlfile = self.config.dstdir / sqlfile.name
        resoutfile = ressqlfile.with_suffix('.out')
        reserrfile_base = resoutfile.with_suffix('.err')

        max_stmts_run = 0
        ressql = None
        resout = None

        for outfile_idx, outfile in enumerate(get_out_files(sqlfile)):
            test_name = Path(sqlfile).name
            LOGGER.info("Processing (%d) %s -> %s", os.getpid(), test_name, Path(outfile).name)
            if is_split_logging:
                logger.info("Processing (%d) %s -> %s", os.getpid(), test_name, Path(outfile).name)

            with open(outfile, 'rb') as fout:
                outdata = fout.readlines()

            only_out_stmts = Counter()
            only_pgrun_stmts = Counter()

            statements = list(self.split_out_file(splitted_stmts, outdata, logger))
            logger.debug("Matching sql statements to .out file lines")
            for (s_sql, s_out) in statements:
                stmt = '\n'.join(str(sql_line) for sql_line in s_sql)
                only_out_stmts[stmt] += 1
                logger.debug(
                    "<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n%s\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n%s\n============================",
                    stmt,
                    '\n'.join(str(out_line) for out_line in s_out),
                )

            with tempfile.TemporaryDirectory() as tempdir:
                test_out_name = Path(tempdir) / "test.out"
                test_err_name = test_out_name.with_suffix(".err")

                runner_args = self.config.runner + ["--datadir", tempdir]
                for udf in self.config.udfs:
                    runner_args.append("--udf")
                    runner_args.append(udf)

                if init_scripts:
                    init_out_name = Path(tempdir) / "init.out"
                    init_err_name = init_out_name.with_suffix(".err")

                    for init_script in init_scripts:
                        logger.debug("Running init script %s '%s'", self.config.runner, init_script)
                        with open(init_script, 'rb') as f, open(init_out_name, 'wb') as fout, open(init_err_name, 'wb') as ferr:
                            pi = subprocess.run(runner_args, stdin=f, stdout=fout, stderr=ferr)

                        if pi.returncode != 0:
                            logger.warning("%s returned error code %d", self.config.runner, pi.returncode)

                logger.debug("Running test %s '%s' -> [%s]", self.config.runner, sqlfile, outfile)

                with open(sqlfile, 'rb') as f, open(test_out_name, 'wb') as fout, open(test_err_name, 'wb') as ferr:
                    pi = subprocess.run(runner_args, stdin=f, stdout=fout, stderr=ferr)

                if pi.returncode != 0:
                    logger.warning("%s returned error code %d", self.config.runner, pi.returncode)

                with open(test_out_name, 'rb') as fresult:
                    out = fresult.readlines()
                    logger.debug("Run result:\n%s", str(b'\n'.join(out)))

                    real_statements = list(self.split_out_file(splitted_stmts, out, logger))
                    logger.debug("Matching sql statements to pgrun's output")
                    for (s_sql, s_out) in real_statements:
                        stmt = '\n'.join(str(sql_line) for sql_line in s_sql)
                        logger.debug(
                            "<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n%s\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n%s\n============================",
                            stmt,
                            '\n'.join(str(out_line) for out_line in s_out),
                        )

                        if 0 < only_out_stmts[stmt]:
                            only_out_stmts[stmt] -= 1
                            if 0 == only_out_stmts[stmt]:
                                del only_out_stmts[stmt]
                        else:
                            only_pgrun_stmts[stmt] += 1
                reserrfile = reserrfile_base if outfile_idx == 0 else reserrfile_base.with_suffix(reserrfile_base.suffix + ".{0}".format(outfile_idx))
                shutil.move(test_err_name, reserrfile)

            if only_pgrun_stmts:
                logger.info("Statements in pgrun output, but not in out file:\n%s",
                            "\n--------------------------------\n".join(stmt for stmt in only_pgrun_stmts))
            if only_out_stmts:
                logger.info("Statements in out file, but not in pgrun output:\n%s",
                            "\n--------------------------------\n".join(stmt for stmt in only_out_stmts))

            stmts_run = 0
            stmts = []
            outs = []
            assert len(statements) == len(real_statements), f"Incorrect statements split in {test_name}. Statements in out-file: {len(statements)}, statements in pgrun output: {len(real_statements)}"
            for ((l_sql, out), (r_sql, res)) in zip(statements, real_statements):
                if l_sql != r_sql:
                    logger.warning("out SQL <> pgrun SQL:\n  <: %s\n  >: %s", l_sql, r_sql)
                    break

                if len(Differ.diff(b''.join(out), b''.join(res))) == 0:
                    stmts.extend(l_sql)
                    outs.extend(out)
                    stmts_run += 1
                else:
                    logger.warning("out result differs from pgrun result:\n  <<: %s\n  >>: %s", out, res)

            if max_stmts_run < stmts_run:
                max_stmts_run = stmts_run
                ressql = stmts
                resout = outs

        if ressql is not None and resout is not None:
            LOGGER.info('Case built: %s', sqlfile.name)
            if is_split_logging:
                logger.info('Case built: %s', sqlfile.name)

            save_strings(ressqlfile, ressql)
            save_strings(resoutfile, resout)
        else:
            LOGGER.warning('Case is empty: %s', sqlfile.name)
            if is_split_logging:
                logger.warning('Case is empty: %s', sqlfile.name)

            ressqlfile.unlink(missing_ok=True)
            resoutfile.unlink(missing_ok=True)

        return Path(sqlfile).stem, stmts_count, stmts_run, round(stmts_run * 100 / stmts_count, 2)

    def split_sql_file(self, sqlfile):
        with open(sqlfile, "rb") as f:
            pi = subprocess.run(self.config.splitter, stdin=f, stdout=subprocess.PIPE, stderr=sys.stderr, check=True)

        lines = iter(pi.stdout.splitlines(keepends=True))
        delimiter = next(lines)

        cur_stmt = []

        for line in lines:
            if line == delimiter:
                yield cur_stmt
                cur_stmt = []
                continue

            cur_stmt.append(line)

        if cur_stmt:
            yield cur_stmt

    reCopyFromStdin = re.compile(b"COPY[^;]+FROM std(?:in|out)", re.I)

    def split_out_file(self, stmts, outdata, logger):
        """Matches SQL & its output in outdata with individual SQL statements in stmts.

        Args:
            stmts ([[str]]): Iterator of SQL statements.
            outdata ([str]): Contents of out-file.

        Yields:
            [([str], [str])]: Sequence of matching parts in sql & out files.
        """

        cur_stmt_out = []
        out_iter = enumerate(outdata)
        echo_none = False
        in_copy_from = False
        no_more_stmts_expected = False

        try:
            line_no, out_line = next(out_iter)
        except StopIteration:
            no_more_stmts_expected = True

        in_line_no = 0

        for i, stmt in enumerate(stmts):
            if no_more_stmts_expected:
                yield stmt, cur_stmt_out
                cur_stmt_out = []
                continue

            try:
                for stmt_line in stmt:
                    in_line_no += 1

                    if echo_none:
                        if stmt_line.startswith(b"\\set ECHO ") and not stmt_line.rstrip().endswith(b"none"):
                            echo_none = False
                        continue

                    if stmt_line.startswith(b"\\set ECHO none"):
                        echo_none = True

                    # We skip data lines of copy ... from stdin, since they aren't in the out-file
                    if in_copy_from:
                        if stmt_line.startswith(b"\\."):
                            in_copy_from = False
                        continue

                    if self.reCopyFromStdin.match(stmt_line):
                        in_copy_from = True

                    logger.debug("Line %d: %s -> %d: %s", in_line_no, stmt_line, line_no, out_line)

                    if stmt_line != out_line:
                        raise Exception(f"Mismatch at {line_no}: '{stmt_line}' != '{out_line}'")
                    cur_stmt_out.append(out_line)

                    line_no, out_line = next(out_iter)

                assert not in_copy_from, f"Missing copy from stdout table end marker \\. at line {in_line_no}"

                if echo_none:
                    continue

                try:
                    next_stmt = stmts[i + 1]
                except IndexError:
                    cur_stmt_out.append(out_line)
                    cur_stmt_out.extend(l for _, l in out_iter)
                    logger.debug("Last out:\n%s", str(b'\n'.join(cur_stmt_out)))
                    yield stmt, cur_stmt_out
                    return

                while True:
                    while out_line != next_stmt[0]:
                        logger.debug("Out: %s -> %s", next_stmt[0], out_line)
                        cur_stmt_out.append(out_line)
                        line_no, out_line = next(out_iter)

                    last_pos = argwhere1(lambda s: self.reCopyFromStdin.match(s), next_stmt, default=len(next_stmt))
                    maybe_next_stmt = outdata[line_no : line_no + last_pos]
                    logger.debug("Left: %s\nRight: %s", next_stmt, maybe_next_stmt)
                    if next_stmt[:last_pos] == maybe_next_stmt:
                        break

                    cur_stmt_out.append(out_line)
                    line_no, out_line = next(out_iter)
                yield stmt, cur_stmt_out
                cur_stmt_out = []
            except StopIteration:
                no_more_stmts_expected = True
                yield stmt, cur_stmt_out
                cur_stmt_out = []


def load_patches(patchdir):
    for p in patchdir.glob("*.patch"):
        ps = patch.fromfile(p)
        if ps is not False:
            yield p.stem, ps


reInitScriptsCfgLine = re.compile(r"^([\w.]+):\s*([\w.]+(?:\s+[\w.]+)*)$")


def load_init_scripts(initscriptscfg, initscriptsdir, tests_set):
    init_scripts_map = dict()

    if not initscriptscfg.is_file():
        LOGGER.warning("Init scripts config file is not found: %s", initscriptscfg)
        return init_scripts_map

    if not initscriptsdir.is_dir():
        LOGGER.warning("Init scripts directory is not found: %s", initscriptsdir)
        return init_scripts_map

    scripts = frozenset(s.stem for s in initscriptsdir.glob("*.sql"))

    with open(initscriptscfg, 'r') as cfg:
        for lineno, line in enumerate(cfg, 1):
            line = line.strip()

            if not line:
                continue

            m = reInitScriptsCfgLine.match(line)

            if m is None:
                LOGGER.warning("Bad line %d in init scripts config %s", lineno, initscriptscfg)
                continue

            test_name = m[1]
            if test_name not in tests_set:
                LOGGER.debug("Skipping init scripts for unknown test case %s", test_name)
                continue

            deps = [(initscriptsdir / s).with_suffix(".sql") for s in m[2].split() if s in scripts]
            if not deps:
                LOGGER.debug("No init scripts are listed for test case %s", test_name)
                continue

            init_scripts_map[test_name] = deps

    return init_scripts_map


def patch_cases(cases, patches, patchdir):
    for i, sql_full_name in enumerate(cases):
        sql_name = sql_full_name.name
        p = patches.get(sql_name, None)
        if p is None:
            continue

        patched_sql_full_name = patchdir / sql_name
        shutil.copyfile(sql_full_name, patched_sql_full_name)
        success = p.apply(root=patchdir)
        if not success:
            LOGGER.warning(
                "Failed to patch %s testcase. Original version %s will be used", patched_sql_full_name, sql_full_name
            )
            continue

        out_full_name = sql_full_name.with_suffix('.out')
        out_name = out_full_name.name
        patched_out_full_name = patchdir / out_name
        # .out file should be in the same dir as .sql file, so copy it anyway
        shutil.copyfile(out_full_name, patched_out_full_name)

        p = patches.get(out_name, None)
        if p is None:
            LOGGER.warning(
                "Out-file patch for %s testcase is not found. Original version %s will be used",
                patched_sql_full_name,
                sql_full_name,
            )
            continue

        success = p.apply(root=patchdir)
        if not success:
            LOGGER.warning(
                "Failed to patch out-file for %s testcase. Original version %s will be used",
                patched_sql_full_name,
                sql_full_name,
            )
            continue

        cases[i] = patched_sql_full_name
        LOGGER.info("Patched %s -> %s", sql_full_name, cases[i])


@click.command()
@click.argument("cases", type=str, nargs=-1)
@click.option(
    "--srcdir",
    "-i",
    help="Directory with SQL suits to process",
    required=True,
    multiple=False,
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path),
)
@click.option(
    "--dstdir",
    "-o",
    help="Output directory",
    required=True,
    multiple=False,
    type=click.Path(exists=True, file_okay=False, resolve_path=True, writable=True, path_type=Path),
)
@click.option(
    "--patchdir",
    "-p",
    help="Directory with patches for SQL suits",
    required=False,
    multiple=False,
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path),
)
@click.option(
    "--udf",
    "-u",
    help="Load shared library with UDF by given path",
    required=False,
    multiple=True,
    type=click.Path(dir_okay=False, resolve_path=True, path_type=Path),
)
@click.option(
    "--initscriptscfg",
    help="Config file for tests' init scripts",
    default=INIT_SCRIPTS_CFG,
    required=False,
    multiple=False,
    type=click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=Path)
)
@click.option(
    "--initscriptsdir",
    help="Directory with tests' init scripts",
    default=INIT_SCRIPTS_DIR,
    required=False,
    multiple=False,
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path)
)
@click.option("--skip", "-s", help="Comma-separated list of testsuits to skip", multiple=False, type=click.STRING)
@click.option("--runner", help="Test runner", default=RUNNER, required=False, multiple=False, type=click.STRING)
@click.option(
    "--splitter", help="SQL statements splitter", default=SPLITTER, required=False, multiple=False, type=click.STRING
)
@click.option(
    "--report",
    "-r",
    help="Report file name",
    default=REPORT_FILE,
    required=False,
    multiple=False,
    type=click.Path(dir_okay=False, resolve_path=True, writable=True, path_type=Path),
)
@click.option("--parallel/--no-parallel", help="Tests build mode", default=True, required=False)
@click.option(
    "--logfile",
    "-l",
    help="Log file",
    default=None,
    required=False,
    multiple=False,
    type=click.Path(dir_okay=False, resolve_path=True, writable=True, path_type=Path),
)
@click.option("--debug/--no-debug", help="Logs verbosity", default=False, required=False)
@click.version_option(version=svn_version(), prog_name=PROGRAM_NAME)
def cli(cases, srcdir, dstdir, patchdir, udf, initscriptscfg, initscriptsdir, skip, runner, splitter, report, parallel, logfile, debug):
    setup_logging(logfile, debug)

    if udf:
        LOGGER.debug("UDFs: %s", udf)

    if skip is not None:
        skip_tests = frozenset(
            test_name if not (test_name := s.strip()).endswith(".sql") else test_name[:-4] for s in skip.split(",")
        )
    else:
        skip_tests = frozenset()

    config = Configuration(
        srcdir, dstdir, udf, patchdir, skip_tests, runner.split(), splitter.split(), report, parallel, logfile, debug
    )

    if not cases:
        cases = [c for c in config.srcdir.glob("*.sql") if c.stem not in skip_tests]
    else:
        cases = [Path(c) if os.path.isabs(c) else config.srcdir / c for c in cases]

    init_scripts = load_init_scripts(initscriptscfg, initscriptsdir, frozenset(c.stem for c in cases))
    LOGGER.debug("Init scripts: %s", init_scripts)

    if config.patchdir is not None:
        patches = dict(load_patches(config.patchdir))
        LOGGER.info("Patches: %s", ", ".join(p for p in patches))
    else:
        patches = {}

    with tempfile.TemporaryDirectory() as tempdir:
        patch_cases(cases, patches, Path(tempdir))

        LOGGER.info("Test cases: %s", ", ".join(c.as_posix() for c in cases))

        builder = TestCaseBuilder(config)
        if config.parallel:
            with Pool() as pool:
                results = list(pool.imap_unordered(builder.build, [(test_case, init_scripts.get(test_case.stem) or []) for test_case in cases]))
        else:
            results = [builder.build(c) for c in cases]

    with open(config.report_path, "w", newline='') as f:
        writer = csv.writer(f, dialect="excel")
        writer.writerow(["testcase", "statements", "successful", "ratio"])
        writer.writerows(sorted(results))


if __name__ == "__main__":
    try:
        cli()
    finally:
        logging.shutdown()

# vim:tw=78:sw=4:et:ai:si
