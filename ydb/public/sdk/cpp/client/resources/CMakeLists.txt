add_library(cpp-client-resources)
target_link_libraries(cpp-client-resources PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
)
target_sources(cpp-client-resources PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/resources/ydb_resources.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/resources/ydb_ca.cpp
)

add_global_library_for(cpp-client-resources.global cpp-client-resources)
target_sources(cpp-client-resources.global PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/public/sdk/cpp/client/resources/6ed212bf45019efe2a5e72b6d5ed50fb.cpp
)
resources(cpp-client-resources.global
  ${CMAKE_BINARY_DIR}/ydb/public/sdk/cpp/client/resources/6ed212bf45019efe2a5e72b6d5ed50fb.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/resources/ydb_sdk_version.txt
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/resources/ydb_root_ca.pem
  KEYS
  ydb_sdk_version.txt
  ydb_root_ca.pem
)
