from devtools.yamaker.modules import join_srcs
from devtools.yamaker.project import NixProject


def ragel6_post_install(self):
    m = self.yamakes["."]
    m.prebuilt = "build/prebuilt/contrib/tools/ragel6/ya.make.prebuilt"
    # Rename ragel to ragel6.
    m.PROGRAM = ["ragel6"]
    # aapl in ragel 5 and 6 is the same.
    m.PEERDIR += ["contrib/tools/ragel5/aapl"]
    # Join sources to reduce compilation utime.
    srcs = m.SRCS
    m.SRCS = {src for src in srcs if src.startswith("rl")}
    srcs -= m.SRCS
    for prefix in "cd", "cs", "fs", "go", "ml", "r":
        group = {src for src in srcs if src.startswith(prefix)}
        srcs -= group
        m.after("SRCS", join_srcs(f"all_{prefix}.cpp", group))
    m.after("SRCS", join_srcs("all_other.cpp", srcs))


ragel6 = NixProject(
    owners=["pg", "g:cpp-contrib"],
    arcdir="contrib/tools/ragel6",
    nixattr="ragel",
    install_subdir="ragel",
    copy_sources=["*.rl", "*.kl", "*.kh"],
    post_install=ragel6_post_install,
)
