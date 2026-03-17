import argparse
import getpass
import logging
import sys

import olefile

from msoffcrypto import OfficeFile, exceptions
from msoffcrypto.format.ooxml import OOXMLFile, _is_ooxml

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _get_version():
    if sys.version_info >= (3, 8):
        from importlib import metadata

        return metadata.version("msoffcrypto-tool")
    else:
        import pkg_resources

        return pkg_resources.get_distribution("msoffcrypto-tool").version


def ifWIN32SetBinary(io):
    if sys.platform == "win32":
        import msvcrt
        import os

        msvcrt.setmode(io.fileno(), os.O_BINARY)


def is_encrypted(file):
    r"""
    Test if the file is encrypted.

        >>> f = open("tests/inputs/plain.doc", "rb")
        >>> is_encrypted(f)
        False
    """
    # TODO: Validate file
    if not olefile.isOleFile(file):
        return False

    file = OfficeFile(file)

    return file.is_encrypted()


parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-p", "--password", nargs="?", const="", dest="password", help="password text")
group.add_argument("-t", "--test", dest="test_encrypted", action="store_true", help="test if the file is encrypted")
parser.add_argument("-e", dest="encrypt", action="store_true", help="encryption mode (default is false)")
parser.add_argument("-v", dest="verbose", action="store_true", help="print verbose information")
parser.add_argument("infile", nargs="?", type=argparse.FileType("rb"), help="input file")
parser.add_argument("outfile", nargs="?", type=argparse.FileType("wb"), help="output file (if blank, stdout is used)")


def main():
    args = parser.parse_args()

    if args.verbose:
        logger.removeHandler(logging.NullHandler())
        logging.basicConfig(level=logging.DEBUG, format="%(message)s")
        version = _get_version()
        logger.debug("Version: {}".format(version))

    if args.test_encrypted:
        if not is_encrypted(args.infile):
            print("{}: not encrypted".format(args.infile.name), file=sys.stderr)
            sys.exit(1)
        else:
            logger.debug("{}: encrypted".format(args.infile.name))
        return

    if args.password:
        password = args.password
    else:
        password = getpass.getpass()

    if args.outfile is None:
        ifWIN32SetBinary(sys.stdout)
        if hasattr(sys.stdout, "buffer"):  # For Python 2
            args.outfile = sys.stdout.buffer
        else:
            args.outfile = sys.stdout

    if args.encrypt:
        if not _is_ooxml(args.infile):
            raise exceptions.FileFormatError("Not an OOXML file")

        # OOXML is the only format we support for encryption
        file = OOXMLFile(args.infile)

        file.encrypt(password, args.outfile)
    else:
        if not olefile.isOleFile(args.infile):
            raise exceptions.FileFormatError("Not an OLE file")

        file = OfficeFile(args.infile)
        file.load_key(password=password)

        file.decrypt(args.outfile)


if __name__ == "__main__":
    main()
