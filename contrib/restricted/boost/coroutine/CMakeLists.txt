
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-coroutine)
target_compile_options(restricted-boost-coroutine PUBLIC
  -DBOOST_COROUTINES_NO_DEPRECATION_WARNING
)
target_compile_options(restricted-boost-coroutine PRIVATE
  -DBOOST_COROUTINES_SOURCE
  -Wno-everything
)
target_include_directories(restricted-boost-coroutine PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/coroutine/include
)
target_link_libraries(restricted-boost-coroutine PUBLIC
  contrib-libs-cxxsupp
  restricted-boost-assert
  restricted-boost-config
  boost-context-fcontext_impl
  restricted-boost-core
  restricted-boost-exception
  restricted-boost-move
  restricted-boost-range
  restricted-boost-system
  restricted-boost-thread
  restricted-boost-throw_exception
  restricted-boost-type_traits
  restricted-boost-utility
)
target_sources(restricted-boost-coroutine PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/coroutine/src/posix/stack_traits.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/coroutine/src/detail/coroutine_context.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/coroutine/src/exceptions.cpp
)
