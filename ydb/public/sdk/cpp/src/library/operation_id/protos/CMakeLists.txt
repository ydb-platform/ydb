file(GLOB_RECURSE LIB_OPERATION_ID_PROTOS
  *.proto
)

_ydb_sdk_add_proto_library(lib-operation_id-protos
  SOURCES ${LIB_OPERATION_ID_PROTOS}
)

target_include_directories(lib-operation_id-protos PRIVATE
  ${YDB_SDK_BINARY_DIR}/ydb-cpp-sdk
)

_ydb_sdk_install_targets(TARGETS lib-operation_id-protos)
