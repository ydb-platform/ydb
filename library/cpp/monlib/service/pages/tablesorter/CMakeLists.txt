add_library(service-pages-tablesorter INTERFACE)
target_link_libraries(service-pages-tablesorter INTERFACE
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
  cpp-monlib-dynamic_counters
)

add_global_library_for(service-pages-tablesorter.global service-pages-tablesorter)
target_sources(service-pages-tablesorter.global PRIVATE
  ${CMAKE_BINARY_DIR}/library/cpp/monlib/service/pages/tablesorter/0c255888afcc3e2adc5fd360ee152bac.cpp
)
resources(service-pages-tablesorter.global
  ${CMAKE_BINARY_DIR}/library/cpp/monlib/service/pages/tablesorter/0c255888afcc3e2adc5fd360ee152bac.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/tablesorter/resources/jquery.tablesorter.css
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/tablesorter/resources/jquery.tablesorter.js
  KEYS
  jquery.tablesorter.css
  jquery.tablesorter.js
)
