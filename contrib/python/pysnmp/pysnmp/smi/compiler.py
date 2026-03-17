#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import os
import sys
import warnings

from pysnmp.smi.builder import MibBuilder

DEFAULT_SOURCES = ["file:///usr/share/snmp/mibs", "file:///usr/share/mibs"]

if sys.platform[:3] == "win":
    DEFAULT_DEST = os.path.join(os.path.expanduser("~"), "PySNMP Configuration", "mibs")
else:
    DEFAULT_DEST = os.path.join(os.path.expanduser("~"), ".pysnmp", "mibs")

DEFAULT_BORROWERS = []

try:
    from pysmi.reader.url import getReadersFromUrls
    from pysmi.searcher.pypackage import PyPackageSearcher
    from pysmi.searcher.stub import StubSearcher
    from pysmi.borrower.pyfile import PyFileBorrower
    from pysmi.writer.pyfile import PyFileWriter
    from pysmi.parser.smi import parserFactory
    from pysmi.parser.dialect import smiV1Relaxed
    from pysmi.codegen.pysnmp import PySnmpCodeGen, baseMibs
    from pysmi.compiler import MibCompiler

except ImportError:
    from pysnmp.smi import error

    def add_mib_compiler_decorator(errorMsg):
        """Add MIB compiler decorator."""

        def add_mib_compiler(mibBuilder, **kwargs):
            if not kwargs.get("ifAvailable"):
                raise error.SmiError("MIB compiler not available: %s" % errorMsg)

        return add_mib_compiler

    add_mib_compiler = add_mib_compiler_decorator(sys.exc_info()[1])

else:

    def add_mib_compiler(mibBuilder: MibBuilder, **kwargs):
        """Add MIB compiler to MIB builder."""
        if kwargs.get("ifNotAdded") and mibBuilder.get_mib_compiler():
            return

        compiler = MibCompiler(
            parserFactory(**smiV1Relaxed)(),
            PySnmpCodeGen(),
            PyFileWriter(kwargs.get("destination") or DEFAULT_DEST),
        )

        compiler.addSources(
            *getReadersFromUrls(*kwargs.get("sources") or DEFAULT_SOURCES)
        )

        compiler.addSearchers(StubSearcher(*baseMibs))
        compiler.addSearchers(
            *[PyPackageSearcher(x.full_path()) for x in mibBuilder.get_mib_sources()]
        )
        compiler.addBorrowers(
            *[
                PyFileBorrower(x, genTexts=mibBuilder.loadTexts)
                for x in getReadersFromUrls(
                    *kwargs.get("borrowers") or DEFAULT_BORROWERS,
                    **dict(lowcaseMatching=False),
                )
            ]
        )

        mibBuilder.set_mib_compiler(compiler, kwargs.get("destination") or DEFAULT_DEST)


# Compatibility API
deprecated_attributes = {
    "addMibCompiler": "add_mib_compiler",
    "addMibCompilerDecorator": "add_mib_compiler_decorator",
}


def __getattr__(attr: str):
    if new_attr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {new_attr} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[new_attr]
    raise AttributeError(f"module '{__name__}' has no attribute '{attr}'")
