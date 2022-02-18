add_library(cpp-http-misc)
target_link_libraries(cpp-http-misc PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  library-cpp-case_insensitive_string
  library-cpp-cgiparam
  cpp-digest-lower_case
)
target_sources(cpp-http-misc PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/http/misc/httpcodes.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/misc/httpdate.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/misc/httpreqdata.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/misc/parsed_request.cpp
)
generate_enum_serilization(cpp-http-misc
  ${CMAKE_SOURCE_DIR}/library/cpp/http/misc/httpcodes.h
  INCLUDE_HEADERS
  library/cpp/http/misc/httpcodes.h
)
