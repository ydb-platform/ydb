_ydb_sdk_add_library(tools-enum_parser-parse_enum)

target_link_libraries(tools-enum_parser-parse_enum
  PUBLIC
    yutil
  PRIVATE
    cppparser
)

target_sources(tools-enum_parser-parse_enum PRIVATE
  parse_enum.cpp
)


