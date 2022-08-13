
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-multi_array INTERFACE)
target_include_directories(restricted-boost-multi_array INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/multi_array/include
)
target_link_libraries(restricted-boost-multi_array INTERFACE
  contrib-libs-cxxsupp
  yutil
  restricted-boost-array
  restricted-boost-assert
  restricted-boost-concept_check
  restricted-boost-config
  restricted-boost-core
  restricted-boost-functional
  restricted-boost-iterator
  restricted-boost-mpl
  restricted-boost-static_assert
  restricted-boost-type_traits
)
