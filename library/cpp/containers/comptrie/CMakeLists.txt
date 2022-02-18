add_library(cpp-containers-comptrie)
target_link_libraries(cpp-containers-comptrie PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-packers
  cpp-containers-compact_vector
  cpp-on_disk-chunks
  util-draft
)
target_sources(cpp-containers-comptrie PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/comptrie.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/comptrie_builder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/comptrie_impl.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/make_fast_layout.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/minimize.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/node.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/opaque_trie_iterator.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/prefix_iterator.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/search_iterator.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/write_trie_backwards.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/comptrie/writeable_node.cpp
)
