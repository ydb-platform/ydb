add_library(library-cpp-compproto)
target_link_libraries(library-cpp-compproto PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-compproto PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/compproto/lib.cpp
)
