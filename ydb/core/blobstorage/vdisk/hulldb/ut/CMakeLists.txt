
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-core-blobstorage-vdisk-hulldb-ut)
target_include_directories(ydb-core-blobstorage-vdisk-hulldb-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb
)
target_link_libraries(ydb-core-blobstorage-vdisk-hulldb-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  blobstorage-vdisk-hulldb
  library-cpp-getopt
  library-cpp-svnversion
  ydb-core-base
  ydb-core-blobstorage
  ydb-core-erasure
)
target_sources(ydb-core-blobstorage-vdisk-hulldb-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/hullds_cache_block_ut.cpp
)
add_test(
  NAME
  ydb-core-blobstorage-vdisk-hulldb-ut
  COMMAND
  ydb-core-blobstorage-vdisk-hulldb-ut
)
target_link_flags(ydb-core-blobstorage-vdisk-hulldb-ut
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
vcs_info(ydb-core-blobstorage-vdisk-hulldb-ut)
