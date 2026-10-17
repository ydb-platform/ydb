LIBRARY()

SRCS(
    granule.cpp  # gn: into ydb/core/tx/columnshard/engines
    storage.cpp  # gn: into ydb/core/tx/columnshard/engines
    portions_index.cpp  # gn: into ydb/core/tx/columnshard/engines
    portion_interval_tree.cpp  # gn: into ydb/core/tx/columnshard/engines
    stages.cpp  # gn: into ydb/core/tx/columnshard/engines
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/actualizer/index
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/base
    ydb/core/formats/arrow/reader
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner  # gn: plugin
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner  # gn: plugin
)

GENERATE_ENUM_SERIALIZATION(granule.h)

END()
