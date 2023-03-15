LIBRARY()

SRCS(
    monservice.cpp
    mon_service_http_request.cpp
    service.cpp
    format.cpp
    auth.cpp
)

PEERDIR(
    library/cpp/string_utils/base64
    contrib/libs/protobuf
    library/cpp/coroutine/engine
    library/cpp/coroutine/listener
    library/cpp/http/fetch
    library/cpp/http/server
    library/cpp/http/io
    library/cpp/logger
    library/cpp/malloc/api
    library/cpp/svnversion
    library/cpp/uri
    library/cpp/cgiparam
)

END()
