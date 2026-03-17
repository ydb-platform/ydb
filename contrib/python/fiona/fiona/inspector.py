import code
import logging
import sys

import fiona


logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger('fiona.inspector')


def main(srcfile):
    """Open a dataset in an interactive session."""
    with fiona.drivers():
        with fiona.open(srcfile) as src:
            code.interact(
                'Fiona %s Interactive Inspector (Python %s)\n'
                'Type "src.schema", "next(src)", or "help(src)" '
                "for more information."
                % (fiona.__version__, ".".join(map(str, sys.version_info[:3]))),
                local=locals(),
            )

    return 1


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m fiona.inspector",
        description="Open a data file and drop into an interactive interpreter",
    )
    parser.add_argument("src", metavar="FILE", help="Input dataset file name")
    args = parser.parse_args()
    main(args.src)
