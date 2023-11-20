import os
from collections import namedtuple
from datetime import date
import csv
from pathlib import Path
import re
import click


TestCase = namedtuple('TestCase', 'statements successful ratio')


def load_testcases(filename):
    testcases = {}

    with open(filename, newline='') as infile:
        reader = csv.DictReader(infile, delimiter=',')
        for row in reader:
            testcases[row['testcase']] = TestCase(row['statements'], row['successful'], row['ratio'])

    return testcases


reIncrement = re.compile(r'(\d+)(?: ?\(+ ?(\d+)\))?')


def format_success_count(old, new):
    m = reIncrement.search(old)
    if m is None:
        return str(new)

    old_count = int(m[1])
    new_count = int(new)

    if old_count < new_count:
        return f'{new_count} (+{new_count - old_count})'

    # we need it to keep old increments, if count stays the same
    if old_count == new_count:
        return str(old)

    return str(new)


reTableRow = re.compile(r'^\|\| \d+ \|')


@click.command()
@click.argument("reportfile", type=str, nargs=1)
@click.argument("statusfile", type=str, nargs=1)
def cli(reportfile, statusfile):
    today = date.today().strftime('%d.%m.%Y')

    testcases = load_testcases(reportfile)

    outfile = Path(statusfile).with_suffix('.tmp')
    with open(statusfile, 'r') as fin, open(outfile, 'w') as fout:
        for line in fin:
            if reTableRow.match(line):
                testno, testname, stmts_count, success_count, ratio, when_updated, notes = line.split(' | ')
                try:
                    testcase = testcases[testname]
                    new_success_count = format_success_count(success_count, testcase.successful)
                    fout.write(
                        ' | '.join(
                            [
                                testno,
                                testname,
                                testcase.statements,
                                new_success_count,
                                testcase.ratio,
                                today if new_success_count != success_count else when_updated,
                                notes,
                            ]
                        )
                    )
                except KeyError:
                    fout.write(line)
            else:
                fout.write(line)

    oldfile = Path(statusfile).with_suffix('.old')
    os.rename(statusfile, oldfile)
    os.rename(outfile, statusfile)


if __name__ == '__main__':
    cli()
