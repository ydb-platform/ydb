add_library(cpp-string_utils-indent_text)
target_link_libraries(cpp-string_utils-indent_text PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-string_utils-indent_text PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/indent_text/indent_text.cpp
)
