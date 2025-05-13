from devtools.yamaker.modules import Linkable, Switch, py_srcs
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    with self.yamakes["."] as pillow:
        # libxcd is not present in Arcadia and should be removed from build.
        # I have failed to get a proper nix setup.
        pillow.CFLAGS.remove("-DHAVE_XCB")

        pillow.to_py_library(
            PEERDIR=["contrib/python/cffi", "contrib/python/olefile"],
            PY_SRCS=py_srcs(self.dstdir),
            RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
        )
        # TODO: generate PY_REGISTER automagically?
        pillow.PY_REGISTER += [
            "PIL._imagingcms",
            "PIL._imagingft",
            "PIL._imagingmath",
            "PIL._imagingmorph",
            "PIL._webp",
        ]

        # Allow grabbing screen image where GUI is available
        pillow.PY_SRCS.remove("PIL/ImageGrab.py")
        pillow.after(
            "PY_SRCS",
            Switch(
                {
                    "OS_DARWIN OR OS_WINDOWS": Linkable(PY_SRCS=["TOP_LEVEL", "PIL/ImageGrab.py"]),
                },
            ),
        )

        # Allow using libimagequant in non-opensource builds
        pillow.CFLAGS.remove("-DHAVE_LIBIMAGEQUANT")
        pillow.PEERDIR.remove("contrib/libs/libimagequant")
        pillow.ADDINCL.remove("contrib/libs/libimagequant")

        pillow.after(
            "CFLAGS",
            Switch(
                {
                    "NOT OPENSOURCE": Linkable(
                        CFLAGS=["-DHAVE_LIBIMAGEQUANT"],
                        PEERDIR=["contrib/libs/libimagequant"],
                        ADDINCL=["contrib/libs/libimagequant"],
                    ),
                }
            ),
        )


pillow = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/Pillow/py3",
    nixattr=python.make_nixattr("pillow"),
    install_subdir="src",
    build_install_subdir="build",
    copy_sources=[
        "PIL/*.py",
        "libImaging/ImDib.h",
    ],
    copy_sources_except=[
        "PIL/ImageTk.py",
        "PIL/ImageQt.py",
        "PIL/_tkinter_finder.py",
    ],
    disable_includes=[
        "config.h",
        "raqm.h",
        "thirdparty/raqm/raqm.h",
        "fribidi.h",
        "thirdparty/fribidi-shim/fribidi.h",
        "hb.h",
        "xcb/xcb.h",
    ],
    ignore_targets=[
        f"_imagingtk.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu",
    ],
    put={
        f"_imaging.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu": ".",
    },
    put_with={
        f"_imaging.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu": [
            f"_imagingcms.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu",
            f"_imagingft.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu",
            f"_imagingmath.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu",
            f"_imagingmorph.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu",
            f"_webp.cpython-{python._VERSION_DOTLESS}-x86_64-linux-gnu",
        ],
    },
    post_install=post_install,
)
