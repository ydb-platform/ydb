
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-multiprecision INTERFACE)
target_include_directories(restricted-boost-multiprecision INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/multiprecision/include
)
target_link_libraries(restricted-boost-multiprecision INTERFACE
  contrib-libs-cxxsupp
  yutil
  restricted-boost-array
  restricted-boost-assert
  restricted-boost-config
  restricted-boost-container_hash
  restricted-boost-core
  restricted-boost-integer
  restricted-boost-lexical_cast
  restricted-boost-math
  restricted-boost-mpl
  restricted-boost-predef
  restricted-boost-random
  restricted-boost-rational
  restricted-boost-smart_ptr
  restricted-boost-static_assert
  restricted-boost-throw_exception
  restricted-boost-type_traits
)
