add_executable(bulk_upsert_simple)
target_link_libraries(bulk_upsert_simple PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-client-ydb_table
)
target_sources(bulk_upsert_simple PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/bulk_upsert_simple/main.cpp
)
target_link_flags(bulk_upsert_simple
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(bulk_upsert_simple)
