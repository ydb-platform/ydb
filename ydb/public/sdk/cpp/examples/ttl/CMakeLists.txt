add_executable(ttl)
target_link_libraries(ttl PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-client-ydb_table
)
target_sources(ttl PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/ttl/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/ttl/ttl.cpp
)
target_link_flags(ttl
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ttl)
