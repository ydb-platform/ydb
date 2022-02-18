add_library(contrib-libs-pcre)
target_compile_options(contrib-libs-pcre PUBLIC
  -DPCRE_STATIC
)
target_compile_options(contrib-libs-pcre PRIVATE
  -DHAVE_CONFIG_H
  -DARCADIA_PCRE_ENABLE_JIT
)
target_include_directories(contrib-libs-pcre PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre
)
target_sources(contrib-libs-pcre PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_byte_order.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_chartables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_compile.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_config.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_dfa_exec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_exec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_fullinfo.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_get.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_globals.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_jit_compile.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_maketables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_newline.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_ord2utf8.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_refcount.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_string_utils.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_study.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_tables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_ucd.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_valid_utf8.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_version.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_xclass.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcreposix.c
)
