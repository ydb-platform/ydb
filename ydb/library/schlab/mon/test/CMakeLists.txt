add_executable(mon-test)
target_link_libraries(mon-test PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-resource
  library-cpp-getopt
  cpp-monlib-dynamic_counters
  library-schlab-mon
)
target_sources(mon-test PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/mon/test/test.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/schlab/mon/test/58594e5c7af65697df8d9899a8761b06.cpp
)
resources(mon-test
  ${CMAKE_BINARY_DIR}/ydb/library/schlab/mon/test/58594e5c7af65697df8d9899a8761b06.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/mon/static/schviz-test0.json
  KEYS
  schlab/schviz-test0.json
)
target_link_flags(mon-test
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(mon-test)
