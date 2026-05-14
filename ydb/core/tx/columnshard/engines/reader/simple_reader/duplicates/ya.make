LIBRARY()

SRCS(
<<<<<<< HEAD
    common.cpp
    context.cpp
    events.cpp
    executor.cpp
    filters.cpp
    manager.cpp
=======
    manager.cpp
    events.cpp
    merge.cpp
    common.cpp
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
    private_events.cpp
    splitter.cpp
    context.cpp
    executor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
