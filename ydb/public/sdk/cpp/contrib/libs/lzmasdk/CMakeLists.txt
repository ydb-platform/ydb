_ydb_sdk_add_library(contrib-libs-lzmasdk)

target_compile_options(contrib-libs-lzmasdk PRIVATE
  -D_7ZIP_ST=1
)

target_sources(contrib-libs-lzmasdk PRIVATE
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/7zStream.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Aes.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/AesOpt.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Alloc.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Bra.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Bra86.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/BraIA64.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/CpuArch.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/LzFind.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Lzma2Dec.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Lzma2Enc.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/LzmaDec.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/LzmaEnc.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/LzmaLib.c
  ${YDB_SDK_SOURCE_DIR}/contrib/libs/lzmasdk/Sha256.c
)

_ydb_sdk_install_targets(TARGETS contrib-libs-lzmasdk)
