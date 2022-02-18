add_library(contrib-libs-libc_compat)
target_include_directories(contrib-libs-libc_compat PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/include/readpassphrase
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/include/random
)
target_include_directories(contrib-libs-libc_compat PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/include/readpassphrase
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/include/random
)
target_sources(contrib-libs-libc_compat PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/string.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/readpassphrase.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/explicit_bzero.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/getrandom.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/memfd_create.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/reallocarray.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/strlcat.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libc_compat/strlcpy.c
)
