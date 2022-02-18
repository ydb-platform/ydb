add_library(libs-base64-neon32)
target_link_libraries(libs-base64-neon32 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-base64-neon32 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/neon32/codec_neon32.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/neon32/lib.c
)
