
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-graph)
target_compile_options(restricted-boost-graph PRIVATE
  -Wno-everything
)
target_include_directories(restricted-boost-graph PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/graph/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/disjoint_sets/include
)
target_link_libraries(restricted-boost-graph PUBLIC
  contrib-libs-cxxsupp
  restricted-boost-algorithm
  restricted-boost-any
  restricted-boost-array
  restricted-boost-assert
  restricted-boost-bimap
  restricted-boost-bind
  restricted-boost-concept_check
  restricted-boost-config
  restricted-boost-container_hash
  restricted-boost-conversion
  restricted-boost-core
  restricted-boost-detail
  restricted-boost-foreach
  restricted-boost-function
  restricted-boost-integer
  restricted-boost-iterator
  restricted-boost-lexical_cast
  restricted-boost-math
  restricted-boost-move
  restricted-boost-mpl
  restricted-boost-multi_index
  restricted-boost-optional
  restricted-boost-parameter
  restricted-boost-preprocessor
  restricted-boost-property_map
  restricted-boost-property_tree
  restricted-boost-random
  restricted-boost-range
  restricted-boost-regex
  restricted-boost-serialization
  restricted-boost-smart_ptr
  restricted-boost-spirit
  restricted-boost-static_assert
  restricted-boost-throw_exception
  restricted-boost-tti
  restricted-boost-tuple
  restricted-boost-type_traits
  restricted-boost-typeof
  restricted-boost-unordered
  restricted-boost-utility
  restricted-boost-xpressive
)
target_sources(restricted-boost-graph PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/graph/src/graphml.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/graph/src/read_graphviz_new.cpp
)
