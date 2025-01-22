_ydb_sdk_add_library(client-resources)
target_link_libraries(client-resources PUBLIC
  yutil
  resource
)
target_sources(client-resources PRIVATE
  ydb_resources.cpp
  ydb_ca.cpp
)

add_global_library_for(client-resources.global client-resources)
target_link_libraries(client-resources.global PUBLIC
  yutil
  resource
)
target_sources(client-resources.global PRIVATE
  ${YDB_SDK_BINARY_DIR}/src/client/resources/6ed212bf45019efe2a5e72b6d5ed50fb.cpp
)
resources(client-resources.global
  ${YDB_SDK_BINARY_DIR}/src/client/resources/6ed212bf45019efe2a5e72b6d5ed50fb.cpp
  INPUTS
  ${YDB_SDK_SOURCE_DIR}/src/client/resources/ydb_sdk_version.txt
  ${YDB_SDK_SOURCE_DIR}/src/client/resources/ydb_root_ca.pem
  KEYS
  ydb_sdk_version_v3.txt
  ydb_root_ca_v3.pem
)

_ydb_sdk_make_client_component(Resources client-resources client-resources.global)
