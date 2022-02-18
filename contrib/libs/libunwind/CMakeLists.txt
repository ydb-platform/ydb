add_library(contrib-libs-libunwind)
target_compile_options(contrib-libs-libunwind PUBLIC
  -D_libunwind_
)
target_compile_options(contrib-libs-libunwind PRIVATE
  -D_LIBUNWIND_IS_NATIVE_ONLY
  -fno-exceptions
  -fno-rtti
  -funwind-tables
)
target_include_directories(contrib-libs-libunwind PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/include
)
target_sources(contrib-libs-libunwind PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/Unwind-EHABI.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/Unwind-seh.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/Unwind-sjlj.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/UnwindLevel1-gcc-ext.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/UnwindLevel1.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/UnwindRegistersRestore.S
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/UnwindRegistersSave.S
  ${CMAKE_SOURCE_DIR}/contrib/libs/libunwind/src/libunwind.cpp
)
