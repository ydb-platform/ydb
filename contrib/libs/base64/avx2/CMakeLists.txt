add_library(libs-base64-avx2)
target_link_libraries(libs-base64-avx2 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-base64-avx2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/avx2/codec_avx2.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/avx2/lib.c
)
