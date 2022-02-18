add_library(core-driver_lib-cli_config_base)
target_compile_options(core-driver_lib-cli_config_base PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-driver_lib-cli_config_base PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-enum_codegen
  ydb-core-util
  lib-deprecated-client
  library-yql-minikql
)
target_sources(core-driver_lib-cli_config_base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_config_base/config_base.cpp
)
