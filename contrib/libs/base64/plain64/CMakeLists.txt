add_library(libs-base64-plain64)
target_link_libraries(libs-base64-plain64 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-base64-plain64 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/plain64/codec_plain.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/plain64/lib.c
)
