from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.SRCS.remove("elf.c")
        m.after(
            "SRCS",
            Switch(
                [
                    ("OS_DARWIN", Linkable(SRCS=["macho.c"])),
                    ("OS_LINUX OR OS_ANDROID OR OS_FREEBSD", Linkable(SRCS=["elf.c"])),
                ]
            ),
        )
        m.before("END", "SUPPRESSIONS(tsan.supp)")


libbacktrace = GNUMakeNixProject(
    nixattr="libbacktrace",
    arcdir="contrib/libs/backtrace",
    owners=["g:cpp-contrib"],
    copy_sources=["macho.c"],
    platform_dispatchers=["config.h"],
    post_install=post_install,
    disable_includes=["sys/link.h"],
    keep_paths=["tsan.supp"],
)
