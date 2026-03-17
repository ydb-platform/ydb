#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
import sys
import argparse
from pathlib import Path
from ezdxf import options, print_config
from ezdxf import commands
from ezdxf.fonts import fonts

YES_NO = {True: "yes", False: "no"}
options.set(options.CORE, "LOAD_PROXY_GRAPHICS", "true")


def add_common_arguments(parser):
    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="show version and exit",
    )
    parser.add_argument(
        "-f",
        "--fonts",
        action="store_true",
        help="rebuild system font cache and print all fonts found",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="give more output",
    )
    parser.add_argument(
        "--config",
        action="store",
        help="path to a config file",
    )
    parser.add_argument(
        "--log",
        action="store",
        help='path to a verbose appending log, "stderr" logs to the '
        "standard error stream",
    )


def print_version(verbose=False):
    print_config(verbose=verbose, stream=sys.stdout)


def print_available_fonts(verbose=False):
    from ezdxf.fonts import fonts
    print("Rebuilding system font cache.")
    fonts.build_system_font_cache()
    fonts.font_manager.print_available_fonts(verbose)


def setup_log(args):
    import logging
    from datetime import datetime
    from io import StringIO

    level = "DEBUG" if args.verbose else "INFO"
    if args.log.lower() == "stderr":
        logging.basicConfig(stream=sys.stderr, level=level)
    else:
        logging.basicConfig(filename=args.log, level=level)
    print(f'Appending logs to file "{args.log}", logging level: {level}\n')
    logger = logging.getLogger("ezdxf")
    logger.info("***** Launch time: " + datetime.now().isoformat() + " *****")
    if args.verbose:
        s = StringIO()
        print_config(verbose=True, stream=s)
        logger.info("configuration\n" + s.getvalue())


DESCRIPTION = """
Command launcher for the Python package "ezdxf": https://pypi.org/project/ezdxf/

"""


def main():
    parser = argparse.ArgumentParser(
        "ezdxf",
        description=DESCRIPTION,
    )
    add_common_arguments(parser)
    subparsers = parser.add_subparsers(dest="command")
    commands.add_parsers(subparsers)

    args = parser.parse_args(sys.argv[1:])
    help_ = True
    if args.config:
        config = Path(args.config)
        if config.exists():
            options.read_file(args.config)
            if args.verbose:
                print(f'using config file: "{config}"')
        else:
            print(f'config file "{config}" not found')
    if args.log:
        setup_log(args)
    if args.version:
        print_version(verbose=args.verbose)
        help_ = False
    if args.fonts:
        print_available_fonts(args.verbose)
        help_ = False

    run = commands.get(args.command)
    if run:
        # For the case automatic font loading is disabled:
        fonts.load()
        run(args)
    elif help_:
        parser.print_help()


if __name__ == "__main__":
    main()
