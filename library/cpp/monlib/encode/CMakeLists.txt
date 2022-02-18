add_library(cpp-monlib-encode)
target_link_libraries(cpp-monlib-encode PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-metrics
  tools-enum_parser-enum_serialization_runtime
)
target_sources(cpp-monlib-encode PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/encoder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/encoder_state.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/format.cpp
)
generate_enum_serilization(cpp-monlib-encode
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/encoder_state_enum.h
  GEN_HEADER
  ${CMAKE_BINARY_DIR}/library/cpp/monlib/encode/encoder_state_enum.h_serialized.h
  INCLUDE_HEADERS
  library/cpp/monlib/encode/encoder_state_enum.h
)
