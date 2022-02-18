add_library(ydb-library-keys)
target_link_libraries(ydb-library-keys PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(ydb-library-keys PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/keys/default_keys.cpp
)
