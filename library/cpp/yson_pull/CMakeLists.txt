add_library(yson_pull)
target_link_libraries(yson_pull PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
)
target_sources(yson_pull PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/consumer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/event.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/exceptions.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/input.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/output.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/read_ops.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/reader.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/scalar.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/writer.cpp
)
generate_enum_serilization(yson_pull
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/event.h
  INCLUDE_HEADERS
  library/cpp/yson_pull/event.h
)
generate_enum_serilization(yson_pull
  ${CMAKE_SOURCE_DIR}/library/cpp/yson_pull/scalar.h
  INCLUDE_HEADERS
  library/cpp/yson_pull/scalar.h
)
