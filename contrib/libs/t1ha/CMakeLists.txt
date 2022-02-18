add_library(contrib-libs-t1ha)
target_compile_options(contrib-libs-t1ha PRIVATE
  -maes
)
target_sources(contrib-libs-t1ha PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha0.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha0_ia32aes_avx.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha0_ia32aes_avx2.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha0_ia32aes_noavx.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha0_selfcheck.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha1.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha1_selfcheck.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha2.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha2_selfcheck.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha_selfcheck.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/t1ha/src/t1ha_selfcheck_all.c
)
