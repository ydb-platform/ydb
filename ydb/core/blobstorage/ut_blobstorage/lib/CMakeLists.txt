
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(blobstorage-ut_blobstorage-lib)
target_link_libraries(blobstorage-ut_blobstorage-lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-md5
  cpp-testing-unittest
  ydb-core-base
  core-blobstorage-backpressure
  blobstorage-dsproxy-mock
  core-blobstorage-nodewarden
  blobstorage-pdisk-mock
  blobstorage-vdisk-common
  core-mind-bscontroller
  core-tx-scheme_board
  ydb-core-util
  udf-service-stub
  yql-sql-pg_dummy
)
target_sources(blobstorage-ut_blobstorage-lib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/ut_blobstorage/lib/node_warden_mock_bsc.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/ut_blobstorage/lib/node_warden_mock_pipe.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/ut_blobstorage/lib/node_warden_mock_state.cpp
)
