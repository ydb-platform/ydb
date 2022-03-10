
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-core-blobstorage-vdisk-anubis_osiris-ut)
target_include_directories(ydb-core-blobstorage-vdisk-anubis_osiris-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/anubis_osiris
)
target_link_libraries(ydb-core-blobstorage-vdisk-anubis_osiris-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  blobstorage-vdisk-anubis_osiris
  library-cpp-getopt
  library-cpp-svnversion
  ydb-core-base
)
target_sources(ydb-core-blobstorage-vdisk-anubis_osiris-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubis_algo_ut.cpp
)
add_test(
  NAME
  ydb-core-blobstorage-vdisk-anubis_osiris-ut
  COMMAND
  ydb-core-blobstorage-vdisk-anubis_osiris-ut
)
target_link_flags(ydb-core-blobstorage-vdisk-anubis_osiris-ut
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
vcs_info(ydb-core-blobstorage-vdisk-anubis_osiris-ut)
