add_library(cpp-html-escape)
target_link_libraries(cpp-html-escape PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-html-escape PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/html/escape/escape.cpp
)
