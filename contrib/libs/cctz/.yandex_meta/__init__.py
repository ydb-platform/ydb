from devtools.yamaker.fileutil import subcopy
from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import NixProject


def cctz_post_build(self):
    # Copy tests preserving the previous layout.
    subcopy(self.srcdir + "/src", self.dstdir + "/test", ["*_test.cc"])


def cctz_post_install(self):
    with self.yamakes["."] as m:
        # Support Darwin.
        m.after(
            "LDFLAGS",
            Switch(OS_DARWIN=Linkable(LDFLAGS=[Words("-framework", "CoreFoundation")])),
        )
        # Recurse to manual ya.make's.
        m.RECURSE |= {"test", "tzdata"}


cctz = NixProject(
    owners=["dfyz", "petrk"],
    arcdir="contrib/libs/cctz",
    nixattr="cctz",
    keep_paths=["README", "test/ya.make", "tzdata/"],
    install_targets=["cctz"],
    addincl_global={".": {"./include"}},
    post_build=cctz_post_build,
    post_install=cctz_post_install,
)
