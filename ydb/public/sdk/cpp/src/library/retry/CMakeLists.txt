file(GLOB_RECURSE LIBRARY_RETRY_PROTOS
  protos/*.proto
)
_ydb_sdk_add_proto_library(retry-protos
  SOURCES ${LIBRARY_RETRY_PROTOS}
)
_ydb_sdk_install_targets(TARGETS retry-protos)

_ydb_sdk_add_library(retry)
target_link_libraries(retry
  PUBLIC
    yutil
    retry-protos
)
target_sources(retry
  PRIVATE
    retry.cpp
    utils.cpp
)
_ydb_sdk_install_targets(TARGETS retry)
