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
