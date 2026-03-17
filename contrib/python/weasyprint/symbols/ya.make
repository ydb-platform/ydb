LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(Service-proxy-version)

NO_COMPILER_WARNINGS()

GLOBAL_SRCS(symb.c)

PEERDIR(
    contrib/libs/harfbuzz
    contrib/libs/pango
    contrib/libs/pango/pangocairo-1.0
    contrib/libs/pango/pangoft2-1.0
)

END()
