LIBRARY()

PEERDIR(
    contrib/libs/zlib
    library/cpp/charset
    library/cpp/digest/md5
    library/cpp/http/misc
    library/cpp/logger
    library/cpp/mime/types
    library/cpp/uri
)

SRCS(
    http_digest.cpp
    http_socket.cpp
    httpheader.cpp
    httpload.cpp
    exthttpcodes.cpp
    httpfsm.rl6
    httpagent.h
    httpfetcher.h
    httpheader.h
    httpparser.h
    httpzreader.h
    sockhandler.h
)

GENERATE_ENUM_SERIALIZATION(httpheader.h)

SET(RAGEL6_FLAGS -CG1)

END()

RECURSE_FOR_TESTS(ut)
