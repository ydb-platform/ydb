add_library(core-mind-address_classification)
target_link_libraries(core-mind-address_classification PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  cpp-actors-core
  cpp-monlib-dynamic_counters
  ydb-core-base
  core-cms-console
  ydb-core-mon
  ydb-core-protos
  ydb-core-util
)
target_sources(core-mind-address_classification PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/mind/address_classification/counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/mind/address_classification/net_classifier.cpp
)
generate_enum_serilization(core-mind-address_classification
  ${CMAKE_SOURCE_DIR}/ydb/core/mind/address_classification/net_classifier.h
  INCLUDE_HEADERS
  ydb/core/mind/address_classification/net_classifier.h
)
