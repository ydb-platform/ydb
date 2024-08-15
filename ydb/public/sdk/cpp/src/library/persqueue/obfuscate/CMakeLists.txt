_ydb_sdk_add_library(persqueue-obfuscate)

target_link_libraries(persqueue-obfuscate PUBLIC
  yutil
)

target_sources(persqueue-obfuscate PRIVATE
  obfuscate.cpp
)

_ydb_sdk_install_targets(TARGETS persqueue-obfuscate)
