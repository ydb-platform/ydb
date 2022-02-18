add_library(contrib-libs-utf8proc)
target_compile_options(contrib-libs-utf8proc PUBLIC
  -DUTF8PROC_STATIC
)
target_compile_options(contrib-libs-utf8proc PRIVATE
  -std=c99
)
target_link_libraries(contrib-libs-utf8proc PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-utf8proc PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/utf8proc/utf8proc.c
)
