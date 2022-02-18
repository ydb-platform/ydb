add_library(cms-console-validators)
target_link_libraries(cms-console-validators PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  ydb-core-tablet
  api-protos
)
target_sources(cms-console-validators PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/core_validators.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/registry.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/validator.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/validator_bootstrap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/cms/console/validators/validator_nameservice.cpp
)
