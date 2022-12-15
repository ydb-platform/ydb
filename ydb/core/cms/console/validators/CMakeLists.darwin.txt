
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(cms-console-validators)
target_link_libraries(cms-console-validators PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  ydb-core-tablet
  api-protos
  cpp-deprecated-atomic
)
target_sources(cms-console-validators PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/core_validators.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/registry.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/validator.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/validator_bootstrap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/validator_nameservice.cpp
)
