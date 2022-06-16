
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(blobstorage-ut_pdiskfit-lib)
target_link_libraries(blobstorage-ut_pdiskfit-lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  ydb-core-base
  core-blobstorage-pdisk
  ydb-library-pdisk_io
  cpp-deprecated-atomic
)
target_sources(blobstorage-ut_pdiskfit-lib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/ut_pdiskfit/lib/basic_test.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/ut_pdiskfit/lib/objectwithstate.cpp
)
