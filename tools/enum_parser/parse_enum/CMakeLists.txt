add_library(tools-enum_parser-parse_enum)
target_link_libraries(tools-enum_parser-parse_enum PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cppparser
)
target_sources(tools-enum_parser-parse_enum PRIVATE
  ${CMAKE_SOURCE_DIR}/tools/enum_parser/parse_enum/parse_enum.cpp
)
