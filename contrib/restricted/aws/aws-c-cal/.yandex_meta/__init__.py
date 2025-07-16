from devtools.yamaker.fileutil import files
from devtools.yamaker import pathutil
from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]

    # Support Darwin.
    linux_srcs = files(self.srcdir + "/source/unix/", rel=self.srcdir, test=pathutil.is_source)
    darwin_srcs = files(self.srcdir + "/source/darwin/", rel=self.srcdir)
    windows_srcs = files(self.srcdir + "/source/windows/", rel=self.srcdir)
    m.SRCS -= set(linux_srcs)
    m.after(
        "SRCS",
        Switch(
            OS_LINUX=Linkable(SRCS=linux_srcs),
            OS_DARWIN=Linkable(
                SRCS=darwin_srcs,
                LDFLAGS=[Words("-framework", "Security")],
            ),
            OS_WINDOWS=Linkable(SRCS=windows_srcs),
        ),
    )
    m.after(
        "CFLAGS",
        Switch(
            OS_WINDOWS=Linkable(
                CFLAGS=["-DAWS_CAL_EXPORTS"],
            ),
        ),
    )


aws_c_cal = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-cal",
    nixattr="aws-c-cal",
    copy_sources=[
        "source/darwin/",
        "source/windows/",
    ],
    disable_includes=[
        "openssl/evp_errors.h",
    ],
    ignore_targets=["sha256_profile"],
    post_install=post_install,
)
