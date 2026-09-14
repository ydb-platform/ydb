LIBRARY()

SRCS(
    abstract.cpp
    constructors.cpp
    unordered_result.cpp
    ordered_result_no_limit.cpp
    ordered_result_with_limit.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/predicate
)

END()
