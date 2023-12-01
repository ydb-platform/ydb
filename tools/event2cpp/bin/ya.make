SET(IDE_FOLDER "_Builders")

PROGRAM(event2cpp)

PEERDIR(
    ADDINCL contrib/libs/protobuf
    contrib/libs/protoc
    library/cpp/eventlog/proto
)

SRCDIR(
    tools/event2cpp
)

SRCS(
    proto_events.cpp
)

INCLUDE(${ARCADIA_ROOT}/build/prebuilt/tools/event2cpp/ya.make.induced_deps)

END()
