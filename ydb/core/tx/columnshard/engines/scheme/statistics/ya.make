LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    ydb/core/tx/columnshard/engines/scheme/statistics/max
    ydb/core/tx/columnshard/engines/scheme/statistics/count_min_sketch
    ydb/core/tx/columnshard/engines/scheme/statistics/variability
    ydb/core/tx/columnshard/engines/scheme/statistics/protos
)

END()
