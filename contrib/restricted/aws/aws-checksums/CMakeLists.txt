add_library(restricted-aws-aws-checksums)
target_compile_options(restricted-aws-aws-checksums PRIVATE
  -DAWS_CHECKSUMS_EXPORTS
  -DAWS_CHECKSUMS_USE_IMPORT_EXPORT
  -DAWS_COMMON_USE_IMPORT_EXPORT
  -DHAVE_SYSCONF
  -Daws_checksums_EXPORTS
)
target_include_directories(restricted-aws-aws-checksums PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-checksums/include
)
target_include_directories(restricted-aws-aws-checksums PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-checksums/include
)
target_link_libraries(restricted-aws-aws-checksums PUBLIC
  restricted-aws-aws-c-common
)
target_sources(restricted-aws-aws-checksums PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-checksums/source/crc.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-checksums/source/crc_sw.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-checksums/source/intel/asm/crc32c_sse42_asm.c
)
