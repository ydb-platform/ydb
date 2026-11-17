from devtools.yamaker.project import MesonNixProject


def post_install(self):
    # Flags with __attribute__ break ya.make grammar
    self.yamakes["."].CFLAGS = [flag for flag in self.yamakes["."].CFLAGS if "__attribute__" not in flag]


fribidi = MesonNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/fribidi",
    nixattr="fribidi",
    flags=[
        "-Dbin=false",
        "-Ddeprecated=false",
        "-Ddocs=false",
        "-Dtests=false",
    ],
    install_targets=["fribidi"],
    put={"fribidi": "."},
    disable_includes=["fribidi-custom.h"],
    addincl_global={".": ["./include"]},
    platform_dispatchers=["config.h"],
    # To obtain this list, execute `pkg-config --cflags fribidi | sed -e 's/-I//' | xargs ls`.
    inclink={
        "include": [
            "gen.tab/fribidi-unicode-version.h",
            "lib/fribidi.h",
            "lib/fribidi-arabic.h",
            "lib/fribidi-begindecls.h",
            "lib/fribidi-bidi-types-list.h",
            "lib/fribidi-bidi-types.h",
            "lib/fribidi-bidi.h",
            "lib/fribidi-brackets.h",
            "lib/fribidi-char-sets-list.h",
            "lib/fribidi-char-sets.h",
            "lib/fribidi-common.h",
            "lib/fribidi-config.h",
            "lib/fribidi-deprecated.h",
            "lib/fribidi-enddecls.h",
            "lib/fribidi-flags.h",
            "lib/fribidi-joining-types-list.h",
            "lib/fribidi-joining-types.h",
            "lib/fribidi-joining.h",
            "lib/fribidi-mirroring.h",
            "lib/fribidi-shape.h",
            "lib/fribidi-types.h",
            "lib/fribidi-unicode.h",
        ]
    },
    post_install=post_install,
)
