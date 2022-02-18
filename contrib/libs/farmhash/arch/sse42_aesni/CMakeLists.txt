add_library(farmhash-arch-sse42_aesni)
target_compile_options(farmhash-arch-sse42_aesni PRIVATE
  -msse4.2
  -maes
)
target_link_libraries(farmhash-arch-sse42_aesni PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(farmhash-arch-sse42_aesni PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashsu.cc
)
