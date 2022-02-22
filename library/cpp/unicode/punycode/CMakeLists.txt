add_library(cpp-unicode-punycode)
target_link_libraries(cpp-unicode-punycode PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-unicode-punycode PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/unicode/punycode/punycode.cpp
)
