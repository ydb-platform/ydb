_ydb_sdk_add_library(library-jwt)

target_link_libraries(library-jwt PUBLIC
  yutil
  jwt-cpp::jwt-cpp
  json
  impl-ydb_internal-common
)

target_sources(library-jwt PRIVATE
  jwt.cpp
)

_ydb_sdk_install_targets(TARGETS library-jwt)
