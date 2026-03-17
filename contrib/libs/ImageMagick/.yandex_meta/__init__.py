import os
import pathlib

from devtools.yamaker.fileutil import re_sub_file
from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import NixProject

UNUSED_CODERS = [
    "aai.c",
    "art.c",
    "avs.c",
    "bgr.c",
    "braille.c",
    "cals.c",
    "caption.c",
    "cin.c",
    "cip.c",
    "clip.c",
    "cmyk.c",
    "dcm.c",
    "debug.c",
    "dib.c",
    "dot.c",
    "dpx.c",
    "ept.c",
    "fits.c",
    "gradient.c",
    "gray.c",
    "hald.c",
    "histogram.c",
    "hrz.c",
    "html.c",
    "info.c",
    "inline.c",
    "ipl.c",
    "jnx.c",
    "json.c",
    "label.c",
    "mac.c",
    "map.c",
    "mask.c",
    "mat.c",
    "matte.c",
    "meta.c",
    "mono.c",
    "mpc.c",
    "mpr.c",
    "msl.c",
    "mtv.c",
    "mvg.c",
    "null.c",
    "otb.c",
    "palm.c",
    "pango.c",
    "pattern.c",
    "pgx.c",
    "pcl.c",
    "pdb.c",
    "pdf.c",
    "pes.c",
    "pict.c",
    "pix.c",
    "plasma.c",
    "preview.c",
    "ps.c",
    "ps2.c",
    "ps3.c",
    "pwp.c",
    "rgf.c",
    "rla.c",
    "rle.c",
    "scr.c",
    "screenshot.c",
    "sct.c",
    "sfw.c",
    "sixel.c",
    "stegano.c",
    "thumbnail.c",
    "tile.c",
    "tim.c",
    "ttf.c",
    "txt.c",
    "uil.c",
    "url.c",
    "uyvy.c",
    "vicar.c",
    "vid.c",
    "viff.c",
    "vips.c",
    "wpg.c",
    "xcf.c",
    "xps.c",
    "ycbcr.c",
    "yuv.c",
]


OS_SPECIFIC_SRCS = Switch(
    OS_DARWIN=Linkable(
        SRCS=[
            "magick/mac.c",
        ],
    ),
    OS_WINDOWS=Linkable(
        SRCS=[
            "magick/nt-base.c",
            "magick/nt-feature.c",
        ]
    ),
)


def post_install(self):
    extra_contrib_libs = [
        "contrib/libs/cairo",
    ]

    # make MAGICKCORE_CONFIGURE_PATH point to /etc/ImageMagick-6
    config = pathlib.Path(self.dstdir) / "magick" / "magick-baseconfig-linux.h"
    config.write_text(config.read_text().replace(f"/var/empty/imagemagick-{self.version}", ""))

    with self.yamakes["."] as m:
        m.PROVIDES = ["imagemagick"]
        for src in UNUSED_CODERS:
            m.SRCS.remove(f"coders/{src}")
            os.remove(f"{self.dstdir}/coders/{src}")
        # for reading text chunks on png ping(patch 44):
        m.ADDINCL.add("contrib/libs/libpng")
        m.ADDINCL.remove("contrib/libs/librsvg")
        m.ADDINCL.append("contrib/libs/librsvg/include/librsvg-2.0")

        m.ADDINCL.remove("contrib/libs/freetype/include/freetype2")
        # TODO: annotate the rationale for removing cairo peerdir
        for lib in extra_contrib_libs:
            m.PEERDIR.remove(lib)
            m.ADDINCL = [a for a in m.ADDINCL if lib not in a]
        m.ADDINCL.sort()

        # Support Darwin and Windows
        m.after("SRCS", OS_SPECIFIC_SRCS)
        m.after(
            "PEERDIR",
            Switch(OS_WINDOWS=Linkable(PEERDIR=["contrib/libs/pthreads_win32"])),
        )

    # Remove defines set in a patched magick/magick-baseconfig.h.
    for m in self.yamakes.values():
        m.CFLAGS.remove("-DMAGICKCORE_HDRI_ENABLE=0")
        m.CFLAGS.remove("-DMAGICKCORE_QUANTUM_DEPTH=8")

    # fix build date to prevent diff on re-import
    re_sub_file(
        self.dstdir + "/magick/version.h",
        r"define MagickReleaseDate(.*)\n",
        'define MagickReleaseDate "2019-06-08"\n',
    )


imagemagick = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/ImageMagick",
    nixattr="imagemagick",
    license="ImageMagick",
    platform_dispatchers=[
        "magick/magick-baseconfig.h",
    ],
    put={
        "Magick++-6.Q8": "Magick++",
    },
    copy_sources=[
        "coders/jp2.c",
        "coders/svg.c",
        "magick/api.h",
        "magick/ImageMagick.h",
        "magick/methods.h",
        "magick/mac.c",
        "magick/mac.h",
        "magick/nt-base.c",
        "magick/nt-feature.c",
        "Magick++/lib/Magick++.h",
    ],
    disable_includes=[
        "autotrace/autotrace.h",
        "CL/cl.h",
        "CLPerfMarker.h",
        "fontconfig/fontconfig.h",
        "ghostscript/",
        "lcms.h",
        "lcms/",
        "lcms2/lcms2.h",
        "librsvg/rsvg-cairo.h",
        "librsvg/librsvg-features.h",
        "ltdl.h",
        "magick-config.h",
        "mman.h",
        "OS.h",
        "raqm.h",
        "X11/",
        # if defined(vms)
        "magick/vms.h",
        # ifdef MAGICKCORE_ZERO_CONFIGURATION_SUPPORT
        "magick/threshold-map.h",
        # if defined(__BORLANDC__)
        "vcl.h",
    ],
    addincl_global={
        # magick/magick.h includes magick/image.h
        ".": {"."},
        # Magick++/lib/Magick++.h includes Magick++/Image.h
        "Magick++": {"./lib"},
    },
    post_install=post_install,
)
