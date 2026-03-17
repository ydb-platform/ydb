"""EditorConfig command line interface

Licensed under Simplified BSD License (see LICENSE.BSD file).

"""

import getopt
import sys

from editorconfig import __version__
from editorconfig.exceptions import ParsingError, PathError, VersionError
from editorconfig.handler import EditorConfigHandler
from editorconfig.version import VERSION
from editorconfig.versiontools import split_version


def version() -> None:
    print("EditorConfig Python Core Version %s" % __version__)


def usage(command: str, error: bool = False) -> None:
    if error:
        out = sys.stderr
    else:
        out = sys.stdout
    out.write("%s [OPTIONS] FILENAME\n" % command)
    out.write('-f                 '
              'Specify conf filename other than ".editorconfig".\n')
    out.write("-b                 "
              "Specify version (used by devs to test compatibility).\n")
    out.write("-h OR --help       Print this help message.\n")
    out.write("-v OR --version    Display version information.\n")


def main() -> None:
    command_name = sys.argv[0]
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   "vhb:f:", ["version", "help"])
    except getopt.GetoptError as e:
        print(str(e))
        usage(command_name, error=True)
        sys.exit(2)

    version_tuple = VERSION
    conf_filename = '.editorconfig'

    for option, arg in opts:
        if option in ('-h', '--help'):
            usage(command_name)
            sys.exit()
        if option in ('-v', '--version'):
            version()
            sys.exit()
        if option == '-f':
            conf_filename = arg
        if option == '-b':
            arg_tuple = split_version(arg)
            if arg_tuple is None:
                sys.exit("Invalid version number: %s" % arg)
            version_tuple = arg_tuple

    if len(args) < 1:
        usage(command_name, error=True)
        sys.exit(2)
    filenames = args
    multiple_files = len(args) > 1

    for filename in filenames:
        handler = EditorConfigHandler(filename, conf_filename, version_tuple)
        try:
            options = handler.get_configurations()
        except (ParsingError, PathError, VersionError) as e:
            print(str(e))
            sys.exit(2)
        if multiple_files:
            print("[%s]" % filename)
        for key, value in options.items():
            print("%s=%s" % (key, value))


if __name__ == "__main__":
    main()
