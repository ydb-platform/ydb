add_library(contrib-libs-farmhash)
target_include_directories(contrib-libs-farmhash PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/include
)
target_include_directories(contrib-libs-farmhash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/include
)
target_link_libraries(contrib-libs-farmhash PUBLIC
  contrib-libs-cxxsupp
  yutil
  farmhash-arch-sse41
  farmhash-arch-sse42
  farmhash-arch-sse42_aesni
)
target_sources(contrib-libs-farmhash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashuo.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashxo.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashna.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashmk.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashcc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhash_iface.cc
)
