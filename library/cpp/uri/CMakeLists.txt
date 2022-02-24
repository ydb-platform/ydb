find_package(libidn REQUIRED)

add_library(library-cpp-uri)
target_link_libraries(library-cpp-uri PUBLIC
  contrib-libs-cxxsupp
  yutil
  libidn::libidn
  library-cpp-charset
)
target_sources(library-cpp-uri PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/assign.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/common.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/encode.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/location.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/other.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/parse.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/qargs.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/uri.cpp
)
target_ragel_lexers(library-cpp-uri
  PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/encodefsm.rl6
  -CG2
)
target_ragel_lexers(library-cpp-uri
  PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/uri/parsefsm.rl6
  -CG2
)
