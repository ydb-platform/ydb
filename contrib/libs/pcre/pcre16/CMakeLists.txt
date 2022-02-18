add_library(libs-pcre-pcre16)
target_compile_options(libs-pcre-pcre16 PRIVATE
  -DHAVE_CONFIG_H
)
target_include_directories(libs-pcre-pcre16 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre
)
target_sources(libs-pcre-pcre16 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_byte_order.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_chartables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_compile.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_config.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_dfa_exec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_exec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_fullinfo.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_get.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_globals.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_jit_compile.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_maketables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_newline.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_ord2utf16.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_refcount.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_string_utils.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_study.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_tables.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_ucd.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_utf16_utils.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_valid_utf16.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_version.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre16_xclass.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/pcre/pcre_chartables.c
)
