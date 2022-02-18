add_library(internal-proxies-ref)
target_link_libraries(internal-proxies-ref PUBLIC
  contrib-libs-cxxsupp
  internal-proxies-macro
  argonish-internal-argon2
  argonish-internal-blake2b
)
target_sources(internal-proxies-ref PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/internal/proxies/ref/proxy_ref.cpp
)
