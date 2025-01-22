_ydb_sdk_add_library(library-uuid)

target_link_libraries(library-uuid PUBLIC
  yutil
)

target_sources(library-uuid PRIVATE
  uuid.cpp
)

_ydb_sdk_install_targets(TARGETS library-uuid)
