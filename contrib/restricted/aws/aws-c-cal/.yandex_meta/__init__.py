from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]

    # Support Darwin.
    linux_srcs = files(self.srcdir + "/source/unix/", rel=self.srcdir)
    darwin_srcs = files(self.srcdir + "/source/darwin/", rel=self.srcdir)
    m.SRCS -= set(linux_srcs)
    m.after(
        "SRCS",
        Switch(
            OS_LINUX=Linkable(SRCS=linux_srcs),
            OS_DARWIN=Linkable(
                SRCS=darwin_srcs,
                LDFLAGS=[Words("-framework", "Security")],
            ),
        ),
    )


aws_c_cal = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-cal",
    nixattr="aws-c-cal",
    copy_sources=["source/darwin/"],
    ignore_targets=["sha256_profile"],
    post_install=post_install,
)
