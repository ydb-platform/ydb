add_library(internal-proxies-sse2)
target_link_libraries(internal-proxies-sse2 PUBLIC
  contrib-libs-cxxsupp
  internal-proxies-macro
  argonish-internal-argon2
  argonish-internal-blake2b
)
target_sources(internal-proxies-sse2 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/sse2/proxy_sse2.cpp
)
set_property(
  SOURCE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/sse2/proxy_sse2.cpp
  APPEND
  PROPERTY
  COMPILE_OPTIONS
  -msse2
)
