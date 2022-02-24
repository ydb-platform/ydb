find_package(OpenSSL REQUIRED)

add_library(cpp-actors-http)
target_link_libraries(cpp-actors-http PUBLIC
  contrib-libs-cxxsupp
  yutil
  OpenSSL::OpenSSL
  cpp-actors-core
  cpp-actors-interconnect
  library-cpp-dns
  cpp-monlib-metrics
  cpp-string_utils-quote
)
target_sources(cpp-actors-http PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http_cache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http_proxy_acceptor.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http_proxy_incoming.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http_proxy_outgoing.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http_proxy.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http_static.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/http/http.cpp
)
