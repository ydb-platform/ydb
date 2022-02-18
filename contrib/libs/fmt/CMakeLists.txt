add_library(contrib-libs-fmt)
target_compile_options(contrib-libs-fmt PUBLIC
  -DFMT_EXPORT
)
target_include_directories(contrib-libs-fmt PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/fmt/include
)
target_include_directories(contrib-libs-fmt PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/fmt/include
)
target_link_libraries(contrib-libs-fmt PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-fmt PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/fmt/src/format.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/fmt/src/os.cc
)
