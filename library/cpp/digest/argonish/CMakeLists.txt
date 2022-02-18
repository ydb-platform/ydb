add_library(cpp-digest-argonish)
target_link_libraries(cpp-digest-argonish PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-poor_man_openmp
  internal-proxies-avx2
  internal-proxies-ref
  internal-proxies-sse2
  internal-proxies-sse41
  internal-proxies-ssse3
)
target_sources(cpp-digest-argonish PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/argonish/factory/factory.cpp
)
