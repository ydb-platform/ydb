LIBRARY()

SRCS(
    imdb.cpp
    data_generator.cpp
    GLOBAL registrar.cpp
)

RESOURCE(
    imdb_schema.yaml imdb/schema.yaml
    queries.list imdb/queries.list
)

ALL_RESOURCE_FILES_FROM_DIRS(
    PREFIX imdb/
    queries
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/resource
    library/cpp/streams/factory/open_by_signature
    ydb/library/workload/benchmark_base
    ydb/library/yaml_json
)

END()

RECURSE_FOR_TESTS(ut)
