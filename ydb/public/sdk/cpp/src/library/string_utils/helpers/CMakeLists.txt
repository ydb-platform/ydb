_ydb_sdk_add_library(string_utils-helpers)

target_sources(string_utils-helpers PRIVATE
  ${YDB_SDK_SOURCE_DIR}/src/library/string_utils/helpers/helpers.cpp
)

_ydb_sdk_install_targets(TARGETS string_utils-helpers)
