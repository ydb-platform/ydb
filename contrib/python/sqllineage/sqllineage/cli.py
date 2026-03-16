import argparse
import logging
import logging.config
import warnings

from sqllineage import (
    DEFAULT_DIALECT,
    DEFAULT_HOST,
    DEFAULT_LOGGING,
    DEFAULT_PORT,
)
from sqllineage import (
    NAME as MAIN_NAME,
)
from sqllineage import (
    VERSION as MAIN_VERSION,
)
from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.core.metadata.sqlalchemy import SQLAlchemyMetaDataProvider
from sqllineage.drawing import draw_lineage_graph
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel
from sqllineage.utils.helpers import extract_file_path_from_args, extract_sql_from_args

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
        "--version", action="version", version=f"{MAIN_NAME} {MAIN_VERSION}"
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
    parser.add_argument(
        "--silent_mode",
        help="skip unsupported statements",
        action="store_true",
    )
    parser.add_argument(
        "--sqlalchemy_url",
        help="sqlalchemy url to provide metadata for lineage analysis",
        type=str,
    )
    args = parser.parse_args(args)
    metadata_provider = (
        SQLAlchemyMetaDataProvider(args.sqlalchemy_url)
        if args.sqlalchemy_url
        else DummyMetaDataProvider()
    )
    if args.e and args.f:
        warnings.warn("Both -e and -f options are specified. -e option will be ignored")
    if args.f or args.e:
        sql = extract_sql_from_args(args)
        file_path = extract_file_path_from_args(args)
        runner = LineageRunner(
            sql,
            file_path=file_path,
            dialect=args.dialect,
            metadata_provider=metadata_provider,
            verbose=args.verbose,
            draw_options={
                "host": args.host,
                "port": args.port,
                "f": args.f if args.f else None,
            },
            silent_mode=args.silent_mode,
        )
        if args.graph_visualization:
            runner.draw()
        elif args.level == LineageLevel.COLUMN:
            runner.print_column_lineage()
        else:
            runner.print_table_lineage()
    elif args.graph_visualization:
        return draw_lineage_graph(
            **{
                "host": args.host,
                "port": args.port,
                "metadata_provider": metadata_provider,
            }
        )
    elif args.dialects:
        dialects = []
        for _, supported_dialects in LineageRunner.supported_dialects().items():
            dialects += supported_dialects
        print("\n".join(dialects))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
