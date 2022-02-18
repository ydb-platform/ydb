add_library(cpp-deprecated-split)
target_link_libraries(cpp-deprecated-split PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-kmp
)
target_sources(cpp-deprecated-split PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/split/delim_string_iter.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/split/split_iterator.cpp
)
