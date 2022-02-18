add_library(libs-base64-neon64)
target_link_libraries(libs-base64-neon64 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-base64-neon64 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/neon64/codec_neon64.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/neon64/lib.c
)
