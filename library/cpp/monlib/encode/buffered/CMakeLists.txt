add_library(monlib-encode-buffered)
target_link_libraries(monlib-encode-buffered PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-encode
  cpp-monlib-metrics
)
target_sources(monlib-encode-buffered PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/buffered/buffered_encoder_base.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/buffered/string_pool.cpp
)
