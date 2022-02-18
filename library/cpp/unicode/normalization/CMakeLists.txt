add_library(cpp-unicode-normalization)
target_link_libraries(cpp-unicode-normalization PUBLIC
  contrib-libs-cxxsupp
)
target_sources(cpp-unicode-normalization PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/unicode/normalization/generated/composition.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/unicode/normalization/generated/decomposition.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/unicode/normalization/normalization.cpp
)
