import argparse
import logging
import logging.config

from collate_sqllineage import (
    DEFAULT_DIALECT,
    DEFAULT_HOST,
    DEFAULT_LOGGING,
    DEFAULT_PORT,
)
from collate_sqllineage.drawing import draw_lineage_graph
from collate_sqllineage.runner import LineageRunner
from collate_sqllineage.utils.constant import LineageLevel
from collate_sqllineage.utils.helpers import extract_sql_from_args

logger = logging.getLogger(__name__)


def main(args=None) -> None:
    """
    The command line interface entry point.

    :param args: the command line arguments for sqllineage command
    """
    logging.config.dictConfig(DEFAULT_LOGGING)

    parser = argparse.ArgumentParser(
        prog="sqllineage", description="SQL Lineage Parser."
    )
    parser.add_argument(
        "-e", metavar="<quoted-query-string>", help="SQL from command line"
    )
    parser.add_argument("-f", metavar="<filename>", help="SQL from files")
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity, show statement level lineage result",
        action="store_true",
    )
    parser.add_argument(
        "-l",
        "--level",
        help="lineage level, column or table, default at table level",
        choices=[LineageLevel.TABLE, LineageLevel.COLUMN],
        default=LineageLevel.TABLE,
    )
    parser.add_argument(
        "-g",
        "--graph-visualization",
        help="show graph visualization of the lineage in a webserver",
        action="store_true",
    )
    parser.add_argument(
        "-H",
        "--host",
        help="the host visualization webserver will be bind to",
        type=str,
        default=DEFAULT_HOST,
        metavar="<hostname>",
    )
    parser.add_argument(
        "-p",
        "--port",
        help="the port visualization webserver will be listening on",
        type=int,
        default=DEFAULT_PORT,
        metavar="<port_number>{0..65536}",
    )
    parser.add_argument(
        "-d",
        "--dialect",
        help="the dialect used to analyze the lineage, use --dialects to show all available dialects",
        type=str,
        default=DEFAULT_DIALECT,
        metavar="<dialect>",
    )
    parser.add_argument(
        "-ds",
        "--dialects",
        help="list all the available dialects",
        action="store_true",
    )
    args = parser.parse_args(args)
    if args.e and args.f:
        logging.warning(
            "Both -e and -f options are specified. -e option will be ignored"
        )
    if args.f or args.e:
        sql = extract_sql_from_args(args)
        runner = LineageRunner(
            sql,
            dialect=args.dialect,
            verbose=args.verbose,
            draw_options={
                "host": args.host,
                "port": args.port,
                "f": args.f if args.f else None,
            },
        )
        if args.graph_visualization:
            runner.draw(args.dialect)
        elif args.level == LineageLevel.COLUMN:
            runner.print_column_lineage()
        else:
            runner.print_table_lineage()
    elif args.graph_visualization:
        return draw_lineage_graph(**{"host": args.host, "port": args.port})
    elif args.dialects:
        print("""non-validating
ansi
athena
bigquery
clickhouse
databricks
db2
exasol
hive
materialize
mysql
oracle
postgres
redshift
snowflake
soql
sparksql
sqlite
teradata
tsql""")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
