from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as m:
        # Support Darwin.
        m.after(
            "LDFLAGS",
            Switch(OS_DARWIN=Linkable(LDFLAGS=[Words("-framework", "CoreFoundation")])),
        )
        # Recurse to manual ya.make's.
        m.RECURSE |= {"test", "tzdata"}


cctz = GNUMakeNixProject(
    arcdir="contrib/libs/cctz",
    nixattr="cctz",
    keep_paths=[
        "test/ya.make",
        "tzdata",
    ],
    copy_sources=[
        "src/*_test.cc",
    ],
    disable_includes=[
        "fuchsia/intl/cpp/fidl.h",
        "lib/async-loop/cpp/loop.h",
        "lib/fdio/directory.h",
    ],
    use_full_libnames=True,
    install_targets=["libcctz"],
    addincl_global={".": {"./include"}},
    post_install=post_install,
)
