add_library(ydb-library-security)
target_link_libraries(ydb-library-security PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-crc32c
  client-ydb_types-credentials
)
target_sources(ydb-library-security PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/security/util.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/security/ydb_credentials_provider_factory.cpp
)
