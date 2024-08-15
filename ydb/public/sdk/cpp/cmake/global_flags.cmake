# workaround when old NDK toolchain that does not set CMAKE_ANDROID_NDK_VERSION is used
# See for details: https://gitlab.kitware.com/cmake/cmake/-/issues/24386

set(flagPrefixSymbol "-")

string(APPEND ${COMMON_C_CXX_FLAGS} "\
  -fexceptions \
  -fno-common \
  -fcolor-diagnostics \
  -faligned-allocation \
  -fdebug-default-version=4 \
  -ffunction-sections \
  -fdata-sections \
  -Wall \
  -Wunused \
  -Wextra \
  -Wno-parentheses \
  -Wno-implicit-const-int-float-conversion \
  -Wno-unknown-warning-option \
  -pipe \
  -D_THREAD_SAFE \
  -D_PTHREADS \
  -D_REENTRANT \
  -D_LARGEFILE_SOURCE \
  -D__STDC_CONSTANT_MACROS \
  -D__STDC_FORMAT_MACROS \
  -D__LONG_LONG_SUPPORTED \
")

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  # Use .init_array instead of .ctors (default for old clang versions)
  # See: https://maskray.me/blog/2021-11-07-init-ctors-init-array
  string(APPEND COMMON_C_CXX_FLAGS " -fuse-init-array")
endif()

string(APPEND COMMON_C_CXX_FLAGS " -D_FILE_OFFSET_BITS=64")

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm.*|aarch64|ppc64le)")
  string(APPEND COMMON_C_CXX_FLAGS " -fsigned-char")
endif()

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(i686|x86_64|AMD64)$")
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "i686")
    string(APPEND COMMON_C_CXX_FLAGS " -m32")
  elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
    string(APPEND COMMON_C_CXX_FLAGS " -m64")
  endif()
  string(APPEND COMMON_C_CXX_FLAGS "\
    -msse2 \
    -msse3 \
    -mssse3 \
  ")

  if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
    string(APPEND COMMON_C_CXX_FLAGS "\
      -msse4.1 \
      -msse4.2 \
      -mpopcnt \
    ")
    string(APPEND COMMON_C_CXX_FLAGS " -mcx16")
  endif()
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_C_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_C_CXX_FLAGS} \
  -Woverloaded-virtual \
  -Wimport-preprocessor-directive-pedantic \
  -Wno-undefined-var-template \
  -Wno-return-std-move \
  -Wno-defaulted-function-deleted \
  -Wno-pessimizing-move \
  -Wno-deprecated-anon-enum-enum-conversion \
  -Wno-deprecated-enum-enum-conversion \
  -Wno-deprecated-enum-float-conversion \
  -Wno-ambiguous-reversed-operator \
  -Wno-deprecated-volatile \
")

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(i686|x86_64|AMD64)$")
  set(_ALL_X86_EXTENSIONS_DEFINES "\
    ${flagPrefixSymbol}DSSE_ENABLED=1 \
    ${flagPrefixSymbol}DSSE3_ENABLED=1 \
    ${flagPrefixSymbol}DSSSE3_ENABLED=1 \
  ")
  if ((CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$") OR (NOT ANDROID))
    string(APPEND _ALL_X86_EXTENSIONS_DEFINES "\
      ${flagPrefixSymbol}DSSE41_ENABLED=1 \
      ${flagPrefixSymbol}DSSE42_ENABLED=1 \
      ${flagPrefixSymbol}DPOPCNT_ENABLED=1 \
    ")
    string(APPEND _ALL_X86_EXTENSIONS_DEFINES " ${flagPrefixSymbol}DCX16_ENABLED=1")
  endif()

  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${_ALL_X86_EXTENSIONS_DEFINES}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${_ALL_X86_EXTENSIONS_DEFINES}")
endif()

message(VERBOSE "CMAKE_C_FLAGS = \"${CMAKE_C_FLAGS}\"")
message(VERBOSE "CMAKE_CXX_FLAGS = \"${CMAKE_CXX_FLAGS}\"")
