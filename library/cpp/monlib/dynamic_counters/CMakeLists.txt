add_library(cpp-monlib-dynamic_counters)
target_link_libraries(cpp-monlib-dynamic_counters PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-stack_vector
  monlib-encode-json
  monlib-encode-spack
  monlib-encode-prometheus
  monlib-service-pages
  cpp-string_utils-quote
  cpp-threading-light_rw_lock
)
target_sources(cpp-monlib-dynamic_counters PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/dynamic_counters/counters.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/dynamic_counters/encode.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/dynamic_counters/golovan_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/dynamic_counters/page.cpp
)
