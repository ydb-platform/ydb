add_executable(secondary_index)
target_link_libraries(secondary_index PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-client-ydb_table
)
target_sources(secondary_index PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index_create.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index_delete.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index_drop.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index_generate.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index_list.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index/secondary_index_update.cpp
)
target_link_flags(secondary_index
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(secondary_index)
