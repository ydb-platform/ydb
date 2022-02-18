add_library(cpp-string_utils-parse_size)
target_link_libraries(cpp-string_utils-parse_size PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-string_utils-parse_size PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/parse_size/parse_size.cpp
)
