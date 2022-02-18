add_library(contrib-restricted-libffi)
target_compile_options(contrib-restricted-libffi PUBLIC
  -DFFI_BUILDING
)
target_compile_options(contrib-restricted-libffi PRIVATE
  -DHAVE_CONFIG_H
)
target_include_directories(contrib-restricted-libffi PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/configs/x86_64-pc-linux-gnu/include
)
target_include_directories(contrib-restricted-libffi PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/configs/x86_64-pc-linux-gnu
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/configs/x86_64-pc-linux-gnu/include
)
target_sources(contrib-restricted-libffi PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/closures.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/java_raw_api.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/prep_cif.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/raw_api.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/types.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/x86/ffi64.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/x86/ffiw64.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/x86/unix64.S
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libffi/src/x86/win64.S
)
