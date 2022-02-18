add_library(cpp-string_utils-levenshtein_diff)
target_link_libraries(cpp-string_utils-levenshtein_diff PUBLIC
  contrib-libs-cxxsupp
  yutil
  util-draft
)
target_sources(cpp-string_utils-levenshtein_diff PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/levenshtein_diff/levenshtein_diff.cpp
)
