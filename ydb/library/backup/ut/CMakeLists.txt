
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-library-backup-ut)
target_include_directories(ydb-library-backup-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/backup
)
target_link_libraries(ydb-library-backup-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  kikimr_backup
  cpp-string_utils-quote
)
target_sources(ydb-library-backup-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/backup/ut/ut.cpp
)
add_test(
  NAME
  ydb-library-backup-ut
  COMMAND
  ydb-library-backup-ut
)
target_link_flags(ydb-library-backup-ut
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
vcs_info(ydb-library-backup-ut)
