add_library(internal-proxies-sse41)
target_link_libraries(internal-proxies-sse41 PUBLIC
  contrib-libs-cxxsupp
  internal-proxies-macro
  argonish-internal-argon2
  argonish-internal-blake2b
)
target_sources(internal-proxies-sse41 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/sse41/proxy_sse41.cpp
)
set_property(
  SOURCE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/sse41/proxy_sse41.cpp
  APPEND
  PROPERTY
  COMPILE_OPTIONS
  -msse4.1
)
