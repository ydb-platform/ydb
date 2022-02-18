add_library(internal-proxies-avx2)
target_link_libraries(internal-proxies-avx2 PUBLIC
  contrib-libs-cxxsupp
  internal-proxies-macro
  argonish-internal-argon2
  argonish-internal-blake2b
)
target_sources(internal-proxies-avx2 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/avx2/proxy_avx2.cpp
)
set_property(
  SOURCE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/avx2/proxy_avx2.cpp
  APPEND
  PROPERTY
  COMPILE_OPTIONS
  -mavx2
  -mfma
)
