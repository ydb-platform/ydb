from devtools.yamaker.project import GNUMakeNixProject
from devtools.yamaker.modules import Switch, Linkable


def post_install(self):
    with self.yamakes["."] as m:
        m.CFLAGS.extend(
            f"-D{name}=_argon2_{name}"
            for name in (
                "blake2b",
                "blake2b_init",
                "blake2b_init_key",
                "blake2b_init_param",
                "blake2b_final",
                "blake2b_update",
            )
        )
        # Support arm64
        m.SRCS.remove("src/opt.c")
        m.after("SRCS", Switch(ARCH_X86_64=Linkable(SRCS=["src/opt.c"]), default=Linkable(SRCS=["src/ref.c"])))


argon2 = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/deprecated/argon2",
    copy_sources=["src/blake2/blamka-round-ref.h", "src/ref.c"],
    nixattr="libargon2",
    makeflags=["libargon2.so.1"],
    disable_includes=["genkat.h"],
    post_install=post_install,
)
