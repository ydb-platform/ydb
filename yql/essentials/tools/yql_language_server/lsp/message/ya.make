LIBRARY()

PEERDIR(
    yql/essentials/utils/json
    library/cpp/json
)

SRCS(
    completion.cpp
    diagnostic.cpp
    exception.cpp
    formatting.cpp
    method.cpp
    session.cpp
    synchronization.cpp
    text_document.cpp
)

GENERATE_ENUM_SERIALIZATION(completion.h)
GENERATE_ENUM_SERIALIZATION(diagnostic.h)
GENERATE_ENUM_SERIALIZATION(exception.h)
GENERATE_ENUM_SERIALIZATION(formatting.h)
GENERATE_ENUM_SERIALIZATION(method.h)
GENERATE_ENUM_SERIALIZATION(session.h)
GENERATE_ENUM_SERIALIZATION(synchronization.h)
GENERATE_ENUM_SERIALIZATION(text_document.h)

END()
