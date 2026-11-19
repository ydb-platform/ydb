LIBRARY()

LICENSE(
    LGPL-2.0-only AND
    LGPL-2.1-only AND
    LGPL-2.1-or-later
)

VERSION(0.6.8)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/libs/libxml
    contrib/restricted/glib
)

ADDINCL(
    contrib/libs/libcroco
    contrib/libs/libxml/include
    contrib/restricted/glib
    contrib/restricted/glib/glib
)

SRCS(
    src/cr-additional-sel.c
    src/cr-attr-sel.c
    src/cr-cascade.c
    src/cr-declaration.c
    src/cr-doc-handler.c
    src/cr-enc-handler.c
    src/cr-fonts.c
    src/cr-input.c
    src/cr-num.c
    src/cr-om-parser.c
    src/cr-parser.c
    src/cr-parsing-location.c
    src/cr-prop-list.c
    src/cr-pseudo.c
    src/cr-rgb.c
    src/cr-selector.c
    src/cr-sel-eng.c
    src/cr-simple-sel.c
    src/cr-statement.c
    src/cr-string.c
    src/cr-style.c
    src/cr-stylesheet.c
    src/cr-term.c
    src/cr-tknzr.c
    src/cr-token.c
    src/cr-utils.c
)

END()
