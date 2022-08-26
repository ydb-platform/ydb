
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-filesystem)
target_compile_options(restricted-boost-filesystem PRIVATE
  -DBOOST_FILESYSTEM_NO_CXX20_ATOMIC_REF
  -Wno-everything
)
target_include_directories(restricted-boost-filesystem PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/include
)
target_link_libraries(restricted-boost-filesystem PUBLIC
  contrib-libs-cxxsupp
  restricted-boost-assert
  restricted-boost-atomic
  restricted-boost-config
  restricted-boost-container_hash
  restricted-boost-core
  restricted-boost-detail
  restricted-boost-io
  restricted-boost-iterator
  restricted-boost-range
  restricted-boost-smart_ptr
  restricted-boost-static_assert
  restricted-boost-system
  restricted-boost-type_traits
)
target_sources(restricted-boost-filesystem PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/codecvt_error_category.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/directory.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/exception.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/operations.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/path.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/path_traits.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/portability.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/unique_path.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/utf8_codecvt_facet.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/filesystem/src/windows_file_codecvt.cpp
)
