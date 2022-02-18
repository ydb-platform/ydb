add_library(library-cpp-enumbitset)
target_link_libraries(library-cpp-enumbitset PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-enumbitset PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/enumbitset/enumbitset.cpp
)
