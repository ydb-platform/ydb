add_library(library-cpp-comptable)
target_link_libraries(library-cpp-comptable PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-compproto
)
target_sources(library-cpp-comptable PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/comptable/comptable.cpp
)
