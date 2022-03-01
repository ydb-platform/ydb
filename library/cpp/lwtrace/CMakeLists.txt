
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(library-cpp-lwtrace)
target_link_libraries(library-cpp-lwtrace PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-lwtrace-protos
)
target_sources(library-cpp-lwtrace PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/check.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/control.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/custom_action.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/kill_action.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/log_shuttle.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/perf.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/probes.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/shuttle.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/sleep_action.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/start.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/stderr_writer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/symbol.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/trace.cpp
)
