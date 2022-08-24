
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-lockfree INTERFACE)
target_include_directories(restricted-boost-lockfree INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/lockfree/include
)
target_link_libraries(restricted-boost-lockfree INTERFACE
  contrib-libs-cxxsupp
  yutil
  restricted-boost-align
  restricted-boost-array
  restricted-boost-assert
  restricted-boost-atomic
  restricted-boost-config
  restricted-boost-core
  restricted-boost-integer
  restricted-boost-mpl
  restricted-boost-parameter
  restricted-boost-predef
  restricted-boost-static_assert
  restricted-boost-tuple
  restricted-boost-type_traits
  restricted-boost-utility
)
