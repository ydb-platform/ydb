add_library(libs-pcre-pcre32)
target_compile_options(libs-pcre-pcre32 PRIVATE
  -DHAVE_CONFIG_H
)
target_include_directories(libs-pcre-pcre32 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre
)
target_sources(libs-pcre-pcre32 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_byte_order.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_chartables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_compile.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_config.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_dfa_exec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_exec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_fullinfo.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_get.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_globals.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_jit_compile.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_maketables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_newline.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_ord2utf32.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_refcount.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_string_utils.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_study.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_tables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_ucd.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_utf32_utils.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_valid_utf32.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_version.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre32_xclass.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_chartables.c
)
