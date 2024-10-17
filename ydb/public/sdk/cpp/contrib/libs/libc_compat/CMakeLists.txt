_ydb_sdk_add_library(contrib-libs-libc_compat)

target_compile_options(contrib-libs-libc_compat PRIVATE
  $<IF:$<CXX_COMPILER_ID:MSVC>,,-Wno-everything>
)

target_sources(contrib-libs-libc_compat PRIVATE
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/string.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/explicit_bzero.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/memfd_create.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/strlcat.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/strlcpy.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/reallocarray/reallocarray.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/random/getrandom.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/libc_compat/random/getentropy.c
)

_ydb_sdk_install_targets(TARGETS contrib-libs-libc_compat)
