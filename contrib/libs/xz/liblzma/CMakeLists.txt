
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(libs-xz-liblzma)
target_compile_options(libs-xz-liblzma PUBLIC
  -DLZMA_API_STATIC
)
target_compile_options(libs-xz-liblzma PRIVATE
  -DHAVE_CONFIG_H
  -DTUKLIB_SYMBOL_PREFIX=lzma_
)
target_include_directories(libs-xz-liblzma PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/api
)
target_include_directories(libs-xz-liblzma PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/api
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/delta
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lz
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/rangecoder
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/common
)
target_link_libraries(libs-xz-liblzma PUBLIC
  contrib-libs-cxxsupp
  libs-xz-common
)
target_sources(libs-xz-liblzma PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check/check.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check/crc32_fast.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check/crc32_table.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check/crc64_fast.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check/crc64_table.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/check/sha256.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/alone_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/alone_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/auto_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_buffer_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_buffer_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_header_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_header_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/block_util.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/common.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/easy_buffer_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/easy_decoder_memusage.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/easy_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/easy_encoder_memusage.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/easy_preset.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_buffer_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_buffer_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_common.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_flags_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/filter_flags_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/hardware_cputhreads.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/index.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/index_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/index_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/index_hash.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/outqueue.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_buffer_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_buffer_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_encoder_mt.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_flags_common.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_flags_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/stream_flags_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/vli_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/vli_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/common/vli_size.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/delta/delta_common.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/delta/delta_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/delta/delta_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lz/lz_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lz/lz_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lz/lz_encoder_mf.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/fastpos_table.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma2_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma2_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma_encoder_optimum_fast.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma_encoder_optimum_normal.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/lzma/lzma_encoder_presets.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/rangecoder/price_table.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/arm.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/armthumb.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/ia64.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/powerpc.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/simple_coder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/simple_decoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/simple_encoder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/sparc.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/liblzma/simple/x86.c
)
