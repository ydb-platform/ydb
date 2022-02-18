add_library(monlib-encode-prometheus)
target_link_libraries(monlib-encode-prometheus PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-encode
)
target_sources(monlib-encode-prometheus PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/prometheus/prometheus_decoder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/prometheus/prometheus_encoder.cpp
)
