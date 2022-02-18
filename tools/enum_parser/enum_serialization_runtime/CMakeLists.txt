add_library(tools-enum_parser-enum_serialization_runtime)
target_link_libraries(tools-enum_parser-enum_serialization_runtime PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(tools-enum_parser-enum_serialization_runtime PRIVATE
  ${CMAKE_SOURCE_DIR}/tools/enum_parser/enum_serialization_runtime/enum_runtime.cpp
)
