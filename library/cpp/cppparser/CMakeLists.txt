add_library(library-cpp-cppparser)
target_link_libraries(library-cpp-cppparser PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-cppparser PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/cppparser/parser.cpp
)
