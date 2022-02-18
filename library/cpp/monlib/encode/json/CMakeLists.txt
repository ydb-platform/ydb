add_library(monlib-encode-json)
target_link_libraries(monlib-encode-json PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-encode
  monlib-encode-buffered
  cpp-monlib-exception
  library-cpp-json
  cpp-json-writer
)
target_sources(monlib-encode-json PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/json/json_decoder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/json/json_encoder.cpp
)
