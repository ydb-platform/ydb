add_library(library-cpp-iterator)
target_link_libraries(library-cpp-iterator PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-iterator PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/cartesian_product.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/concatenate.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/enumerate.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/iterate_keys.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/iterate_values.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/filtering.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/functools.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/mapped.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/iterator/zip.cpp
)
