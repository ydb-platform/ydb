from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(Library)


expected_lite = NixSourceProject(
    arcdir="contrib/restricted/expected-lite",
    owners=["g:cpp-contrib"],
    nixattr="expected-lite",
    copy_sources=["include/**"],
    disable_includes=[
        "expected",
        "nonstd/expected.tweak.hpp",
    ],
    post_install=post_install,
)
