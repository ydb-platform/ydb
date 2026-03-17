#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import argparse

from . import (
    codes_definition_path,
    codes_get_api_version,
    codes_get_library_path,
    codes_samples_path,
)


def selfcheck():
    print("Found: ecCodes v%s." % codes_get_api_version())
    print("Library:", codes_get_library_path())
    print("Definitions:", codes_definition_path())
    print("Samples:", codes_samples_path())
    print("Your system is ready.")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    args = parser.parse_args(args=argv)
    if args.command == "selfcheck":
        selfcheck()
    else:
        raise RuntimeError(
            "Command not recognised %r. See usage with --help." % args.command
        )


if __name__ == "__main__":  # pragma: no cover
    main()
