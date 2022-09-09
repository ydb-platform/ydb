
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-ptr_container INTERFACE)
target_include_directories(restricted-boost-ptr_container INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/ptr_container/include
)
target_link_libraries(restricted-boost-ptr_container INTERFACE
  contrib-libs-cxxsupp
  yutil
  restricted-boost-array
  restricted-boost-assert
  restricted-boost-circular_buffer
  restricted-boost-config
  restricted-boost-core
  restricted-boost-iterator
  restricted-boost-mpl
  restricted-boost-range
  restricted-boost-serialization
  restricted-boost-smart_ptr
  restricted-boost-static_assert
  restricted-boost-type_traits
  restricted-boost-unordered
  restricted-boost-utility
)
