import sys

from ydb.tools.include_sanitizer.cli import main


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
