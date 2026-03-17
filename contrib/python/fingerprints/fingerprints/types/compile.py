import os
import yaml

from fingerprints.types.common import TypesList, TYPES_PATH

CODE_DIR = os.path.dirname(__file__)


def write_python() -> None:
    with open(TYPES_PATH, "r", encoding="utf-8") as fh:
        data: TypesList = yaml.safe_load(fh)
    python_file = os.path.join(CODE_DIR, "data.py")
    with open(python_file, "w", encoding="utf-8") as pyfh:
        pyfh.write("# generated file, do not edit.\n")
        pyfh.write("from fingerprints.types.common import TypesList\n\n")
        pyfh.write("TYPES: TypesList = %s\n" % repr(data))


if __name__ == "__main__":
    write_python()
