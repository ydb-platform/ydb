FUZZ()

SRCS(
    main.cpp
    ../../../base/path.cpp
)

CFLAGS(-Wno-deprecated-declarations)

END()
