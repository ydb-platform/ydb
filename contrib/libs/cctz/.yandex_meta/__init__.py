from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        # Support Darwin.
        m.after(
            "LDFLAGS",
            Switch(OS_DARWIN=Linkable(LDFLAGS=[Words("-framework", "CoreFoundation")])),
        )
        # Recurse to manual ya.make's.
        m.RECURSE |= {"tzdata"}

    with self.yamakes["test"] as test:
        test.module = "GTEST"
        test.GTEST = []
        test.CFLAGS.remove("-DGTEST_LINKED_AS_SHARED_LIBRARY=1")
        test.PEERDIR.add(f"{self.arcdir}/tzdata")
        test.PEERDIR.remove("contrib/restricted/googletest/googletest")
        test.EXPLICIT_DATA = True


cctz = CMakeNinjaNixProject(
    arcdir="contrib/libs/cctz",
    nixattr="cctz",
    keep_paths=[
        "tzdata",
    ],
    disable_includes=[
        "fuchsia/intl/cpp/fidl.h",
        "lib/async-loop/cpp/loop.h",
        "lib/fdio/directory.h",
    ],
    use_full_libnames=True,
    install_targets=[
        "libcctz",
        "civil_time_test",
        "time_zone_format_test",
        "time_zone_lookup_test",
    ],
    put={
        "libcctz": ".",
        "civil_time_test": "test",
    },
    put_with={
        "civil_time_test": [
            "time_zone_format_test",
            "time_zone_lookup_test",
        ],
    },
    addincl_global={".": {"./include"}},
    post_install=post_install,
)
