add_executable(quoter_performance_test)
target_compile_options(quoter_performance_test PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(quoter_performance_test PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kesus/tablet
)
target_link_libraries(quoter_performance_test PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-testing-unittest
  core-kesus-tablet
  ydb-core-testlib
)
target_sources(quoter_performance_test PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kesus/tablet/quoter_performance_test/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kesus/tablet/ut_helpers.cpp
)
target_link_flags(quoter_performance_test
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -Wl,-rpath,$ORIGIN
  -fPIC
  -Wl,--gdb-index
  -Wl,-rpath,$ORIGIN
  -fPIC
  -fuse-ld=$(LLD_ROOT-sbr:2283360772)/ld
  -Wl,--no-rosegment
  -lpthread
  -lrt
  -ldl
)
vcs_info(quoter_performance_test)
