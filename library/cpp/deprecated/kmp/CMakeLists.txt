add_library(cpp-deprecated-kmp)
target_link_libraries(cpp-deprecated-kmp PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-deprecated-kmp PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/kmp/kmp.cpp
)
