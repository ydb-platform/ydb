add_subdirectory(protos)

_ydb_sdk_add_library(library-operation_id)

target_link_libraries(library-operation_id
  PRIVATE
    yutil
    cgiparam
    uri
    lib-operation_id-protos
)

target_sources(library-operation_id PRIVATE
  operation_id.cpp
)

_ydb_sdk_install_targets(TARGETS library-operation_id)
