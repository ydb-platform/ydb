_ydb_sdk_add_library(string_utils-misc INTERFACE)

target_link_libraries(string_utils-misc INTERFACE
  yutil
  string_utils-helpers
)

_ydb_sdk_install_targets(TARGETS string_utils-misc)
