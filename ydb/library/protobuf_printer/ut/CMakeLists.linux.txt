
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-library-protobuf_printer-ut)
target_include_directories(ydb-library-protobuf_printer-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/protobuf_printer
)
target_link_libraries(ydb-library-protobuf_printer-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  ydb-library-protobuf_printer
  contrib-libs-protobuf
)
target_proto_messages(ydb-library-protobuf_printer-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/protobuf_printer/ut/test_proto.proto
)
target_sources(ydb-library-protobuf_printer-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/protobuf_printer/protobuf_printer_ut.cpp
)
add_test(
  NAME
  ydb-library-protobuf_printer-ut
  COMMAND
  ydb-library-protobuf_printer-ut
)
target_link_flags(ydb-library-protobuf_printer-ut
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -fPIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-library-protobuf_printer-ut)
