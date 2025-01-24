_ydb_sdk_add_library(string_utils-helpers)

target_sources(string_utils-helpers
  PRIVATE
    helpers.cpp
)

_ydb_sdk_install_targets(TARGETS string_utils-helpers)
