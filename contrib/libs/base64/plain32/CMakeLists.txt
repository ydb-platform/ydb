add_library(libs-base64-plain32)
target_link_libraries(libs-base64-plain32 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-base64-plain32 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/plain32/codec_plain.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/base64/plain32/lib.c
)
