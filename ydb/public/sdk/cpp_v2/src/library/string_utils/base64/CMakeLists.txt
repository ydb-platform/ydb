_ydb_sdk_add_library(string_utils-base64)

target_link_libraries(string_utils-base64 PUBLIC
  yutil
  aklomp::base64
)

target_sources(string_utils-base64 PRIVATE
  ${YDB_SDK_SOURCE_DIR}/src/library/string_utils/base64/base64.cpp
)

_ydb_sdk_install_targets(TARGETS string_utils-base64)
