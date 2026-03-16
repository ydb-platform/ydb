LIBRARY()

LICENSE(
    GPL-1.0-or-later AND
    GPL-2.0-or-later AND
    LGPL-2.0-or-later AND
    LGPL-2.1-or-later AND
    TCL
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.32.3)

NO_COMPILER_WARNINGS()

NO_UTIL()

PEERDIR(
    contrib/restricted/glib
    contrib/libs/libpng
    contrib/libs/libjpeg-turbo
)

ADDINCL(
    contrib/libs/gdk-pixbuf
    contrib/libs/libjpeg-turbo
    contrib/libs/libpng
)

IF (OS_DARWIN OR OS_WINDOWS)
    PEERDIR(
        contrib/restricted/gettext-stub
    )
    ADDINCL(
        contrib/restricted/gettext-stub
    )
ENDIF()

CFLAGS(
    GLOBAL -DGDK_PIXBUF_STATIC_COMPILATION
    -DHAVE_CONFIG_H
    -DGDK_PIXBUF_COMPILATION
    -DGDK_PIXBUF_ENABLE_BACKEND
    -DGDK_PIXBUF_LOCALEDIR="\"/usr/local/lib\""
    -DGDK_PIXBUF_LIBDIR="\"/usr/lib\""
    -DGDK_PIXBUF_BINARY_VERSION="\"2.10.0\""
    -DINCLUDE_png
    -DINCLUDE_jpeg
)

SRCS(
    gdk-pixbuf/gdk-pixbuf-animation.c
    # gdk-pixbuf/gdk-pixbuf-csource.c
    gdk-pixbuf/gdk-pixbuf-data.c
    gdk-pixbuf/gdk-pixbuf-enum-types.c
    gdk-pixbuf/gdk-pixbuf-io.c
    gdk-pixbuf/gdk-pixbuf-loader.c
    # gdk-pixbuf/gdk-pixbuf-marshal.c
    # gdk-pixbuf/gdk-pixbuf-pixdata.c
    gdk-pixbuf/gdk-pixbuf-scale.c
    gdk-pixbuf/gdk-pixbuf-scaled-anim.c
    gdk-pixbuf/gdk-pixbuf-simple-anim.c
    gdk-pixbuf/gdk-pixbuf-util.c
    gdk-pixbuf/gdk-pixbuf.c
    gdk-pixbuf/gdk-pixdata.c
    # gdk-pixbuf/io-ani-animation.c
    # gdk-pixbuf/io-ani.c
    # gdk-pixbuf/io-bmp.c
    # gdk-pixbuf/io-gdip-animation.c
    # gdk-pixbuf/io-gdip-bmp.c
    # gdk-pixbuf/io-gdip-emf.c
    # gdk-pixbuf/io-gdip-gif.c
    # gdk-pixbuf/io-gdip-ico.c
    # gdk-pixbuf/io-gdip-jpeg.c
    # gdk-pixbuf/io-gdip-png.c
    # gdk-pixbuf/io-gdip-tiff.c
    # gdk-pixbuf/io-gdip-utils.c
    # gdk-pixbuf/io-gdip-wmf.c
    # gdk-pixbuf/io-gif-animation.c
    # gdk-pixbuf/io-gif.c
    # gdk-pixbuf/io-icns.c
    # gdk-pixbuf/io-ico.c
    # gdk-pixbuf/io-jasper.c
    gdk-pixbuf/io-jpeg.c
    gdk-pixbuf/io-pixdata.c
    gdk-pixbuf/io-png.c
    # gdk-pixbuf/io-pnm.c
    # gdk-pixbuf/io-qtif.c
    # gdk-pixbuf/io-tga.c
    # gdk-pixbuf/io-tiff.c
    # gdk-pixbuf/io-xbm.c
    # gdk-pixbuf/io-xpm.c
    gdk-pixbuf/pixops/pixops.c
    # gdk-pixbuf/pixops/timescale.c
    # gdk-pixbuf/queryloaders.c
    # gdk-pixbuf/test-gdk-pixbuf.c
)

END()
