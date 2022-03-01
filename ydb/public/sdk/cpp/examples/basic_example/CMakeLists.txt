
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(basic_example)
target_link_libraries(basic_example PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  cpp-client-ydb_table
)
target_sources(basic_example PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/basic_example/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/basic_example/basic_example_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/examples/basic_example/basic_example.cpp
)
target_link_flags(basic_example
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
vcs_info(basic_example)
