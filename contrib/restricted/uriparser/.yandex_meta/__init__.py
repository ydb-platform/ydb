import os.path

from devtools.yamaker.project import CMakeNinjaNixProject


def uriparser_post_install(self):
    with self.yamakes["test"] as test:
        # switch test framework into gtest
        test.module = "GTEST"
        test.EXPLICIT_DATA = True
        # unbundle uriparser from the library
        test.CFLAGS = []

        # needed for config.h
        test.ADDINCL = [self.arcdir]

        # By default library sources are compiled into the test.
        # Replacing them with a normal PEERDIR
        test.PEERDIR = [self.arcdir]
        test.SRCDIR = []
        test.SRCS = [os.path.basename(src) for src in test.SRCS if src.startswith("test/")]
    self.yamakes["."].PEERDIR.add("contrib/libs/libc_compat")


uriparser = CMakeNinjaNixProject(
    owners=["shindo", "g:mds", "g:cpp-contrib"],
    arcdir="contrib/restricted/uriparser",
    nixattr="uriparser",
    put={"testrunner": "test"},
    post_install=uriparser_post_install,
)
