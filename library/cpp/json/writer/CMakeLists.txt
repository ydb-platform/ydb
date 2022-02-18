add_library(cpp-json-writer)
target_link_libraries(cpp-json-writer PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-json-common
  tools-enum_parser-enum_serialization_runtime
)
target_sources(cpp-json-writer PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/json/writer/json_value.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/json/writer/json.cpp
)
generate_enum_serilization(cpp-json-writer
  ${CMAKE_SOURCE_DIR}/library/cpp/json/writer/json_value.h
  INCLUDE_HEADERS
  library/cpp/json/writer/json_value.h
)
