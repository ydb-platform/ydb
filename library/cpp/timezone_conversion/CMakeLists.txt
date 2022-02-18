add_library(library-cpp-timezone_conversion)
target_link_libraries(library-cpp-timezone_conversion PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-cctz-tzdata
  util-draft
  tools-enum_parser-enum_serialization_runtime
)
target_sources(library-cpp-timezone_conversion PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/timezone_conversion/convert.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/timezone_conversion/civil.cpp
)
generate_enum_serilization(library-cpp-timezone_conversion
  ${CMAKE_SOURCE_DIR}/library/cpp/timezone_conversion/civil.h
  INCLUDE_HEADERS
  library/cpp/timezone_conversion/civil.h
)
