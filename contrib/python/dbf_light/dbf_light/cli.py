#!/usr/bin/env python
import click

from dbf_light import VERSION_STR, Dbf, open_db

arg_db = click.argument('db', type=click.Path(dir_okay=False))
opt_encoding = click.option('--encoding', help='Encoding used by DB')
opt_zipped = click.option(
    '--zip', help='Zip filename containing DBF', type=click.Path(exists=True, dir_okay=False))
opt_nocase = click.option(
    '-i', '--case-insensitive', help='Do not honor DB filename case', is_flag=True)


@click.group()
@click.version_option(version=VERSION_STR)
def entry_point():
    """dbf_light command line utilities."""


@entry_point.command()
@arg_db
@opt_encoding
@click.option('--no-limit', help='Do not limit number of rows to output.', is_flag=True)
@opt_zipped
@opt_nocase
def show(db, encoding, no_limit, zip, case_insensitive):
    """Show .dbf file contents (rows)."""

    limit = 15

    if no_limit:
        limit = float('inf')

    with open_db(db, zip, encoding=encoding, case_sensitive=not case_insensitive) as dbf:
        for idx, row in enumerate(dbf, 1):
            click.secho('')

            for key, val in row._asdict().items():
                click.secho('  %s: %s' % (key, val))

            if idx == limit:
                click.secho(
                    'Note: Output is limited to %s rows. Use --no-limit option to bypass.' % limit, fg='red')
                break


@entry_point.command()
@arg_db
@opt_zipped
@opt_nocase
def describe(db, zip, case_insensitive):
    """Show .dbf file statistics."""

    with open_db(db, zip, case_sensitive=not case_insensitive) as dbf:
        click.secho('Rows count: %s' % (dbf.prolog.records_count))
        click.secho('Fields:')
        for field in dbf.fields:
            click.secho('  %s: %s' % (field.type, field))


def main():
    entry_point(obj={})


if __name__ == '__main__':
    main()
