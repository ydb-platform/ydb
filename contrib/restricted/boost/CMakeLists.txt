
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(contrib-restricted-boost INTERFACE)
target_include_directories(contrib-restricted-boost INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost
)
target_link_libraries(contrib-restricted-boost INTERFACE
  contrib-libs-cxxsupp
  restricted-boost-align
  restricted-boost-any
  restricted-boost-array
  restricted-boost-assert
  restricted-boost-bind
  restricted-boost-circular_buffer
  restricted-boost-concept_check
  restricted-boost-config
  restricted-boost-container
  restricted-boost-container_hash
  restricted-boost-conversion
  restricted-boost-core
  restricted-boost-detail
  restricted-boost-endian
  restricted-boost-exception
  restricted-boost-format
  restricted-boost-function
  restricted-boost-function_types
  restricted-boost-fusion
  restricted-boost-integer
  restricted-boost-interval
  restricted-boost-intrusive
  restricted-boost-io
  restricted-boost-iterator
  restricted-boost-lambda
  restricted-boost-move
  restricted-boost-mp11
  restricted-boost-mpl
  restricted-boost-multi_array
  restricted-boost-numeric_conversion
  restricted-boost-optional
  restricted-boost-polygon
  restricted-boost-predef
  restricted-boost-preprocessor
  restricted-boost-qvm
  restricted-boost-rational
  restricted-boost-smart_ptr
  restricted-boost-static_assert
  restricted-boost-throw_exception
  restricted-boost-tokenizer
  restricted-boost-tti
  restricted-boost-tuple
  restricted-boost-type_index
  restricted-boost-type_traits
  restricted-boost-typeof
  restricted-boost-unordered
  restricted-boost-utility
  restricted-boost-variant
  restricted-boost-vmd
  restricted-boost-winapi
)
