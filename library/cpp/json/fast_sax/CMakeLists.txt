add_library(cpp-json-fast_sax)
target_link_libraries(cpp-json-fast_sax PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-json-common
)
target_sources(cpp-json-fast_sax PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/json/fast_sax/unescape.cpp
)
target_ragel_lexers(cpp-json-fast_sax
  PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/json/fast_sax/parser.rl6
  -CG2
)
