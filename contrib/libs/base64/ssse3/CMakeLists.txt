add_library(libs-base64-ssse3)
target_link_libraries(libs-base64-ssse3 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-base64-ssse3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/ssse3/codec_ssse3.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/ssse3/lib.c
)
