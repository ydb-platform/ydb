from devtools.yamaker.project import MesonNixProject


def post_install(self):
    with self.yamakes["pangoft2-1.0"] as pango_ft:
        # pangofc-shape.c and pango-ot-tag.c need hb_glib_script_to_script.
        pango_ft.PEERDIR.add("contrib/libs/harfbuzz/glib")

    for m in self.yamakes.values():
        # fmt: off
        m.CFLAGS = [
            flag
            for flag in m.CFLAGS
            if not flag.startswith("-DGLIB_VERSION")
        ]
        # fmt: on


pango = MesonNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/pango",
    nixattr="pango",
    license="LGPL-2.0-or-later",
    platform_dispatchers=["config.h"],
    copy_sources=[
        "pango/*.h",
    ],
    flags=[
        "-Dlibthai=disabled",
        "-Dxft=disabled",
    ],
    disable_includes=[
        "Carbon/*.h",
        "X11/*.h",
        "thai/*.h",
        "fribidi_tab_char_type_9.i",
        "fribidi_tab_char_type_8.i",
        "fribidi_tab_char_type_7.i",
        "fribidi_tab_char_type_6.i",
        "fribidi_tab_char_type_5.i",
        "fribidi_tab_char_type_4.i",
        "fribidi_tab_char_type_3.i",
        "cairo-quartz.h",
        "sysprof-capture.h",
    ],
    inclink={"pango": ["config.h"]},
    ignore_commands=["bash", "glib-mkenums"],
    install_targets=["pango-1.0", "pangocairo-1.0", "pangoft2-1.0"],
    put={
        "pango-1.0": ".",
        "pangocairo-1.0": "pangocairo-1.0",
        "pangoft2-1.0": "pangoft2-1.0",
    },
    post_install=post_install,
)
