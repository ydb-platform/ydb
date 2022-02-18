add_library(cpp-string_utils-base64)
target_link_libraries(cpp-string_utils-base64 PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-base64-avx2
  libs-base64-ssse3
  libs-base64-neon32
  libs-base64-neon64
  libs-base64-plain32
  libs-base64-plain64
)
target_sources(cpp-string_utils-base64 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/base64/base64.cpp
)
