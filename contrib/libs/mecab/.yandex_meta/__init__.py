from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.NO_UTIL = False


mecab = GNUMakeNixProject(
    owners=["g:mt"],
    arcdir="contrib/libs/mecab",
    nixattr="mecab",
    install_targets=["libmecab.so.2.0.0"],
    inclink={"include": {"src/mecab.h"}},
    disable_includes=["zlib.h"],
    platform_dispatchers=["config.h"],
    post_install=post_install,
)
