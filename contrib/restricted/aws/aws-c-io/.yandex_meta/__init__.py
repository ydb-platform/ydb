from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        # (Not adding source/darwin because it conflicts with s2n and openssl.)
        linux_srcs = []
        darwin_srcs = files(self.srcdir + "/source/bsd/", rel=self.srcdir)
        for src in sorted(m.SRCS):
            if "source/linux/" in src:
                m.SRCS.remove(src)
                linux_srcs.append(src)
            elif "source/s2n/" in src or "source/posix/" in src:
                m.SRCS.remove(src)
                linux_srcs.append(src)
                darwin_srcs.append(src)

        m.after(
            "SRCS",
            Switch(OS_LINUX=Linkable(SRCS=linux_srcs), OS_DARWIN=Linkable(SRCS=darwin_srcs)),
        )

        m.CFLAGS.remove("-DUSE_S2N")
        m.PEERDIR.remove("contrib/restricted/aws/s2n")
        m.after(
            "CFLAGS",
            Switch(
                CLANG_CL=Linkable(
                    CFLAGS=["-DAWS_IO_EXPORTS", "-DAWS_USE_IO_COMPLETION_PORTS", "-std=c99"],
                ),
                OS_WINDOWS=Linkable(
                    CFLAGS=["-DAWS_IO_EXPORTS", "-DAWS_USE_IO_COMPLETION_PORTS"],
                ),
                default=Linkable(
                    CFLAGS=["-DUSE_S2N"],
                    PEERDIR=["contrib/restricted/aws/s2n"],
                ),
            ),
        )


aws_c_io = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-io",
    nixattr="aws-c-io",
    copy_sources=["source/bsd/", "include/aws/io/*.h"],
    post_install=post_install,
)
