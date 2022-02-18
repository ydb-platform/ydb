add_library(library-cpp-cgiparam)
target_link_libraries(library-cpp-cgiparam PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-iterator
  cpp-string_utils-quote
  cpp-string_utils-scan
)
target_sources(library-cpp-cgiparam PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/cgiparam/cgiparam.cpp
)
