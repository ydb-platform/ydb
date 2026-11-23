from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import MesonNixProject

X86_64_SRCS = {
    "pixman/pixman-mmx.c",
    "pixman/pixman-sse2.c",
    "pixman/pixman-ssse3.c",
}


def post_install(self):
    with self.yamakes["."] as pixman:
        # Support arm64 on Linux and Darwin.
        # It looks like all arm-specific optimizations intended for 32-bit arms.
        # Just remove x86_64-specific sources from the list of SRCS.
        pixman.SRCS -= X86_64_SRCS
        pixman.after(
            "SRCS",
            Switch(
                ARCH_X86_64=Linkable(SRCS=X86_64_SRCS),
            ),
        )


pixman = MesonNixProject(
    arcdir="contrib/libs/pixman",
    nixattr="pixman",
    install_targets=[
        "pixman-1",
    ],
    put_with={
        "pixman-1": [
            "pixman-mmx",
            "pixman-sse2",
            "pixman-ssse3",
        ]
    },
    platform_dispatchers=[
        "pixman/pixman-config.h",
    ],
    disable_includes=[
        "asm/hwprobe.h",
        "loongson-mmintrin.h",
    ],
    post_install=post_install,
)
