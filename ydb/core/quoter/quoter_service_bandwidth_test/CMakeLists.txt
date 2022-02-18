add_executable(quoter_service_bandwidth_test)
target_compile_options(quoter_service_bandwidth_test PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(quoter_service_bandwidth_test PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-colorizer
  library-cpp-getopt
  ydb-core-base
  core-kesus-tablet
  ydb-core-quoter
  ydb-core-testlib
)
target_sources(quoter_service_bandwidth_test PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/quoter_service_bandwidth_test/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/quoter_service_bandwidth_test/quota_requester.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/quoter_service_bandwidth_test/server.cpp
)
target_link_flags(quoter_service_bandwidth_test
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(quoter_service_bandwidth_test)
