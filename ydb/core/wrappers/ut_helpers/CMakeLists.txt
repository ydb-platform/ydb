
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-wrappers-ut_helpers)
target_link_libraries(core-wrappers-ut_helpers PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-md5
  cpp-http-server
  cpp-xml-document
  library-cpp-cgiparam
)
target_sources(core-wrappers-ut_helpers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/wrappers/ut_helpers/s3_mock.cpp
)
