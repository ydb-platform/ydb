add_library(containers-flat_hash-lib)
target_link_libraries(containers-flat_hash-lib PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(containers-flat_hash-lib PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/containers.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/expanders.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/iterator.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/map.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/probings.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/set.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/size_fitters.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/table.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/flat_hash/lib/value_markers.cpp
)
