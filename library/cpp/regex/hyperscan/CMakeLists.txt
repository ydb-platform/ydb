add_library(cpp-regex-hyperscan)
target_link_libraries(cpp-regex-hyperscan PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-hyperscan
  libs-hyperscan-runtime_core2
  libs-hyperscan-runtime_corei7
  libs-hyperscan-runtime_avx2
  libs-hyperscan-runtime_avx512
)
target_sources(cpp-regex-hyperscan PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/regex/hyperscan/hyperscan.cpp
)
