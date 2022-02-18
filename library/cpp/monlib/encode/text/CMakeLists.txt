add_library(monlib-encode-text)
target_link_libraries(monlib-encode-text PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-encode
)
target_sources(monlib-encode-text PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/text/text_encoder.cpp
)
