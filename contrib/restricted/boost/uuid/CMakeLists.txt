
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-uuid INTERFACE)
target_include_directories(restricted-boost-uuid INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/uuid/include
)
target_link_libraries(restricted-boost-uuid INTERFACE
  contrib-libs-cxxsupp
  yutil
  restricted-boost-assert
  restricted-boost-config
  restricted-boost-core
  restricted-boost-io
  restricted-boost-numeric_conversion
  restricted-boost-predef
  restricted-boost-random
  restricted-boost-serialization
  restricted-boost-smart_ptr
  restricted-boost-static_assert
  restricted-boost-throw_exception
  restricted-boost-tti
  restricted-boost-type_traits
  restricted-boost-winapi
)
