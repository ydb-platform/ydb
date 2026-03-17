#!/usr/bin/env python
#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
# SNMP SMI/MIB copying tool
#
import getopt
import os
import shutil
import sys
from datetime import datetime

from pysmi import debug, error
from pysmi.codegen import JsonCodeGen
from pysmi.compiler import MibCompiler
from pysmi.parser import SmiV1CompatParser
from pysmi.reader import FileReader, get_readers_from_urls
from pysmi.writer import CallbackWriter


def start():
    # sysexits.h
    EX_OK = 0
    EX_USAGE = 64
    EX_SOFTWARE = 70

    # Defaults
    quietFlag = False
    verboseFlag = False
    mibSources = []
    dstDirectory = None
    cacheDirectory = ""
    dryrunFlag = False
    ignoreErrorsFlag = False

    helpMessage = f"""\
    Usage: {sys.argv[0]} [--help]
        [--version]
        [--verbose]
        [--quiet]
        [--debug=<{"|".join(sorted(debug.FLAG_MAP))}>]
        [--mib-source=<URI>]
        [--cache-directory=<DIRECTORY>]
        [--ignore-errors]
        [--dry-run]
        <SOURCE [SOURCE...]> <DESTINATION>
    Where:
        URI      - file, zip, http, https schemes are supported.
                Use @mib@ placeholder token in URI to refer directly to
                the required MIB module when source does not support
                directory listing (e.g. HTTP).
    """

    # TODO(etingof): add the option to copy MIBs into enterprise-indexed subdirs

    try:
        opts, inputMibs = getopt.getopt(
            sys.argv[1:],
            "hv",
            [
                "help",
                "version",
                "verbose",
                "quiet",
                "debug=",
                "mib-source=",
                "mib-stub=",
                "cache-directory=",
                "ignore-errors",
                "dry-run",
            ],
        )

    except getopt.GetoptError:
        sys.exit(EX_USAGE)

    for opt in opts:
        if opt[0] == "-h" or opt[0] == "--help":
            sys.stderr.write(
                f"""\
    Synopsis:
    SNMP SMI/MIB files copying tool. When given MIB file(s) or directory(ies)
    on input and a destination directory, the tool parses MIBs to figure out
    their canonical MIB module name and the latest revision date, then
    copies MIB module on input into the destination directory under its
    MIB module name *if* there is no such file already or its revision date
    is older.

    Documentation:
    https://www.pysnmp.com/pysmi
    {helpMessage}
    """
            )
            sys.exit(EX_OK)

        if opt[0] == "-v" or opt[0] == "--version":
            from pysmi import __version__

            sys.stderr.write(
                f"""\
    SNMP SMI/MIB library version {__version__}, written by Ilya Etingof <etingof@gmail.com>
    Python interpreter: {sys.version}
    Software documentation and support at https://www.pysnmp.com/pysmi
    {helpMessage}
    """
            )
            sys.exit(EX_OK)

        if opt[0] == "--quiet":
            quietFlag = True

        if opt[0] == "--verbose":
            verboseFlag = True

        if opt[0] == "--debug":
            debug.set_logger(debug.Debug(*opt[1].split(",")))

        if opt[0] == "--mib-source":
            mibSources.append(opt[1])

        if opt[0] == "--cache-directory":
            cacheDirectory = opt[1]

        if opt[0] == "--ignore-errors":
            ignoreErrorsFlag = True

        if opt[0] == "--dry-run":
            dryrunFlag = True

    if not mibSources:
        mibSources = [
            "file:///usr/share/snmp/mibs",
            "https://mibs.pysnmp.com/asn1/@mib@",
        ]

    if len(inputMibs) < 2:
        sys.stderr.write(
            f"ERROR: MIB source and/or destination arguments not given{os.linesep}{helpMessage}{os.linesep}"
        )
        sys.exit(EX_USAGE)

    dstDirectory = inputMibs.pop()

    if os.path.exists(dstDirectory) and not os.path.isdir(dstDirectory):
        sys.stderr.write(
            f"ERROR: given destination '{dstDirectory}' is not a directory{os.linesep}{helpMessage}{os.linesep}"
        )
        sys.exit(EX_USAGE)

    try:
        os.makedirs(dstDirectory, mode=0o755)

    except OSError:
        pass

    # Compiler infrastructure

    codeGenerator = JsonCodeGen()

    mibParser = SmiV1CompatParser(tempdir=cacheDirectory)

    fileWriter = CallbackWriter(lambda *x: None)

    def get_mib_revision(mibDir, mibFile):
        mibCompiler = MibCompiler(mibParser, codeGenerator, fileWriter)

        mibCompiler.add_sources(
            FileReader(mibDir, recursive=False, ignoreErrors=ignoreErrorsFlag),
            *get_readers_from_urls(*mibSources),
        )

        try:
            processed = mibCompiler.compile(
                mibFile,
                **dict(
                    noDeps=True,
                    rebuild=True,
                    fuzzyMatching=False,
                    ignoreErrors=ignoreErrorsFlag,
                ),
            )

        except error.PySmiError:
            sys.stderr.write(f"ERROR: {sys.exc_info()[1]}{os.linesep}")
            sys.exit(EX_SOFTWARE)

        for canonicalMibName in processed:
            if processed[canonicalMibName] == "compiled" and processed[
                canonicalMibName
            ].path == "file://" + os.path.join(mibDir, mibFile):
                try:
                    revision = datetime.strptime(
                        processed[canonicalMibName].revision, "%Y-%m-%d %H:%M"
                    )

                except Exception:
                    revision = datetime.fromtimestamp(0)

                return canonicalMibName, revision

        raise error.PySmiError(
            f'Can\'t read or parse MIB "{os.path.join(mibDir, mibFile)}"'
        )

    def shorten_path(path, maxLength=45):
        if len(path) > maxLength:
            return "..." + path[-maxLength:]
        else:
            return path

    mibsSeen = mibsCopied = mibsFailed = 0

    mibsRevisions = {}

    for srcDirectory in inputMibs:
        if verboseFlag:
            if verboseFlag:
                sys.stderr.write(f'Reading "{srcDirectory}"...{os.linesep}')

        if os.path.isfile(srcDirectory):
            mibFiles = [
                (
                    os.path.abspath(os.path.dirname(srcDirectory)),
                    os.path.basename(srcDirectory),
                )
            ]

        else:
            mibFiles = [
                (os.path.abspath(dirName), mibFile)
                for dirName, _, mibFiles in os.walk(srcDirectory)
                for mibFile in mibFiles
            ]

        for srcDirectory, mibFile in mibFiles:
            mibsSeen += 1

            # TODO(etingof): also check module OID to make sure there is no name collision

            try:
                mibName, srcMibRevision = get_mib_revision(srcDirectory, mibFile)

            except error.PySmiError as ex:
                if verboseFlag:
                    sys.stderr.write(
                        f'Failed to read source MIB "{os.path.join(srcDirectory, mibFile)}": {ex}{os.linesep}'
                    )

                if not quietFlag:
                    sys.stderr.write(
                        f"FAILED {shorten_path(os.path.join(srcDirectory, mibFile))}{os.linesep}"
                    )

                mibsFailed += 1

                continue

            if mibName in mibsRevisions:
                dstMibRevision = mibsRevisions[mibName]

            else:
                try:
                    _, dstMibRevision = get_mib_revision(dstDirectory, mibName)

                except error.PySmiError as ex:
                    if verboseFlag:
                        sys.stderr.write(
                            f'MIB "{os.path.join(srcDirectory, mibFile)}" is not available at the destination directory "{dstDirectory}": {ex}{os.linesep}'
                        )

                    dstMibRevision = datetime.fromtimestamp(0)

                mibsRevisions[mibName] = dstMibRevision

            if dstMibRevision >= srcMibRevision:
                if verboseFlag:
                    sys.stderr.write(
                        f'Destination MIB "{os.path.join(dstDirectory, mibName)}" has the same or newer revision as the source MIB "{os.path.join(srcDirectory, mibFile)}"{os.linesep}'
                    )
                if not quietFlag:
                    sys.stderr.write(
                        f"NOT COPIED {shorten_path(os.path.join(srcDirectory, mibFile))} ({mibName}){os.linesep}"
                    )

                continue

            mibsRevisions[mibName] = srcMibRevision

            if verboseFlag:
                if verboseFlag:
                    sys.stderr.write(
                        f'Copying "{os.path.join(srcDirectory, mibFile)}" (revision "{srcMibRevision}") -> "{os.path.join(dstDirectory, mibName)}" (revision "{dstMibRevision}"){os.linesep}'
                    )

            try:
                shutil.copy(
                    os.path.join(srcDirectory, mibFile),
                    os.path.join(dstDirectory, mibName),
                )

            except Exception as ex:
                if verboseFlag:
                    sys.stderr.write(
                        f'Failed to copy MIB "{os.path.join(srcDirectory, mibFile)}" -> "{os.path.join(dstDirectory, mibName)}" ({mibName}): "{ex}"{os.linesep}'
                    )

                if not quietFlag:
                    sys.stderr.write(
                        f"FAILED {shorten_path(os.path.join(srcDirectory, mibFile))} ({mibName}){os.linesep}"
                    )

                mibsFailed += 1

            else:
                if not quietFlag:
                    sys.stderr.write(
                        f"COPIED {shorten_path(os.path.join(srcDirectory, mibFile))} ({mibName}){os.linesep}"
                    )

                mibsCopied += 1

    if not quietFlag:
        sys.stderr.write(
            f"MIBs seen: {mibsSeen}, copied: {mibsCopied}, failed: {mibsFailed}{os.linesep}"
        )

    sys.exit(EX_OK)
