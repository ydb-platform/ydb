add_library(cpp-string_utils-relaxed_escaper)
target_link_libraries(cpp-string_utils-relaxed_escaper PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-string_utils-relaxed_escaper PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/relaxed_escaper/relaxed_escaper.cpp
)
