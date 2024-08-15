_ydb_sdk_add_library(enum_serialization_runtime)

target_link_libraries(enum_serialization_runtime PUBLIC
  yutil
)

target_sources(enum_serialization_runtime 
  PRIVATE
    dispatch_methods.cpp
    enum_runtime.cpp
    ordered_pairs.cpp
)

_ydb_sdk_install_targets(TARGETS enum_serialization_runtime)
