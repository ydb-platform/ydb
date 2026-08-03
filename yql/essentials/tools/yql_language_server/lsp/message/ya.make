LIBRARY()

PEERDIR(
    yql/essentials/utils/json
    library/cpp/json
)

SRCS(
    completion.cpp
    exception.cpp
    formatting.cpp
    method.cpp
    session.cpp
    synchronization.cpp
    text_document.cpp
)

END()
