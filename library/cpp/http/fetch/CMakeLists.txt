find_package(ZLIB)

add_library(cpp-http-fetch)
target_link_libraries(cpp-http-fetch PUBLIC
  contrib-libs-cxxsupp
  yutil
  ZLIB::ZLIB
  library-cpp-charset
  cpp-digest-md5
  cpp-http-misc
  library-cpp-logger
  cpp-mime-types
  library-cpp-uri
  tools-enum_parser-enum_serialization_runtime
)
target_sources(cpp-http-fetch PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/http_digest.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/http_socket.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/httpheader.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/httpload.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/exthttpcodes.cpp
)
generate_enum_serilization(cpp-http-fetch
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/httpheader.h
  INCLUDE_HEADERS
  library/cpp/http/fetch/httpheader.h
)
target_ragel_lexers(cpp-http-fetch
  PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/http/fetch/httpfsm.rl6
  -CF1
)
