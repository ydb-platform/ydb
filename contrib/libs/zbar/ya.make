LIBRARY()

LICENSE(
    BSD-3-Clause AND
    LGPL-2.1-or-later AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(0.10)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/zbar/include
    GLOBAL contrib/libs/libjpeg-turbo
    contrib/libs/libiconv/include
    contrib/libs/zbar/zbar
)

SRCDIR(contrib/libs/zbar/zbar)

SRCS(
    config.c
    convert.c
    decoder.c
    error.c
    image.c
    img_scanner.c
    jpeg.c
    processor.c
    refcnt.c
    scanner.c
    svg.c
    symbol.c
    video.c
    window.c
    decoder/code128.c
    decoder/code39.c
    decoder/ean.c
    decoder/i25.c
    decoder/pdf417.c
    decoder/qr_finder.c
    processor/lock.c
    processor/null.c
    processor/posix.c
    qrcode/bch15_5.c
    qrcode/binarize.c
    qrcode/isaac.c
    qrcode/qrdec.c
    qrcode/qrdectxt.c
    qrcode/rs.c
    qrcode/util.c
    video/null.c
    window/null.c
)

PEERDIR(
    contrib/libs/libiconv
)

END()
