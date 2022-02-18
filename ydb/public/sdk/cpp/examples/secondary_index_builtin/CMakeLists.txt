add_executable(secondary_index_builtin)
target_link_libraries(secondary_index_builtin PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-client-ydb_table
)
target_sources(secondary_index_builtin PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/secondary_index.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/secondary_index_create.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/secondary_index_fill.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/secondary_index_select.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/secondary_index_drop.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/secondary_index_builtin/secondary_index_select_join.cpp
)
target_link_flags(secondary_index_builtin
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(secondary_index_builtin)
