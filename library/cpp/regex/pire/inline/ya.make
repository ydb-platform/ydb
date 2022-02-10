PROGRAM(pire_inline) 

CFLAGS(-DPIRE_NO_CONFIG) 
 
OWNER(
    g:util
    davenger
)

PEERDIR(
    ADDINCL library/cpp/regex/pire
)

SRCDIR(
    contrib/libs/pire/pire
)

SRCS(
    inline.l
)

END()
