add_library(internal-proxies-ssse3)
target_link_libraries(internal-proxies-ssse3 PUBLIC
  contrib-libs-cxxsupp
  internal-proxies-macro
  argonish-internal-argon2
  argonish-internal-blake2b
)
target_sources(internal-proxies-ssse3 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/ssse3/proxy_ssse3.cpp
)
set_property(
  SOURCE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/ssse3/proxy_ssse3.cpp
  APPEND
  PROPERTY
  COMPILE_OPTIONS
  -mssse3
)
