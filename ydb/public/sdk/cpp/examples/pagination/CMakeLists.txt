add_executable(pagination)
target_link_libraries(pagination PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-client-ydb_table
)
target_sources(pagination PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/pagination/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/pagination/pagination_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/pagination/pagination.cpp
)
target_link_flags(pagination
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(pagination)
