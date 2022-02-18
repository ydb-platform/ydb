add_library(cpp-json-easy_parse)
target_link_libraries(cpp-json-easy_parse PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
)
target_sources(cpp-json-easy_parse PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/json/easy_parse/json_easy_parser.cpp
)
