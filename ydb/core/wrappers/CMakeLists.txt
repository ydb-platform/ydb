
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)
add_subdirectory(ut_helpers)

add_library(ydb-core-wrappers)
target_link_libraries(ydb-core-wrappers PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-aws-sdk-cpp-aws-cpp-sdk-s3
  contrib-libs-curl
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  core-wrappers-ut_helpers
)
target_sources(ydb-core-wrappers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/wrappers/s3_out.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/wrappers/s3_wrapper.cpp
)
