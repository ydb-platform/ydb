add_library(contrib-libs-fastlz)
target_link_libraries(contrib-libs-fastlz PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-fastlz PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/fastlz/fastlz.c
)
