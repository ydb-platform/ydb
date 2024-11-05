if (REQUIRED_LLVM_TOOLING_VERSION)
  if (CMAKE_VERSION VERSION_LESS 3.18)
    message(FATAL_ERROR "Forcing LLVM tooling versions requires at least cmake 3.18")
  endif()
  find_program(CLANGPLUSPLUS clang++-${REQUIRED_LLVM_TOOLING_VERSION} REQUIRED)
  find_program(CLANGC clang-${REQUIRED_LLVM_TOOLING_VERSION} REQUIRED)
  find_program(LLVMLINK llvm-link-${REQUIRED_LLVM_TOOLING_VERSION} REQUIRED)
  find_program(LLVMOPT opt-${REQUIRED_LLVM_TOOLING_VERSION} REQUIRED)
  find_program(LLVMLLC llc-${REQUIRED_LLVM_TOOLING_VERSION} REQUIRED)
  find_program(LLVMAS llvm-as-${REQUIRED_LLVM_TOOLING_VERSION} REQUIRED)
else()
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CLANGPLUSPLUS ${CMAKE_CXX_COMPILER})
    message(STATUS "Using ${CLANGPLUSPLUS} for c++ to LLVM IR translation")
  else()
    find_program(CLANGPLUSPLUS NAMES clang++-12 clang++-14 clang++)
    if (CLANGPLUSPLUS MATCHES "CLANGPLUSPLUS-NOTFOUND")
      message(SEND_ERROR "clang++ not found")
    else()
      message(STATUS "Using ${CLANGPLUSPLUS} for c++ to LLVM IR translation")
    endif()
  endif()

  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    set(CLANGC ${CMAKE_C_COMPILER})
    message(STATUS "Using ${CLANGC} for c++ to LLVM IR translation")
  else()
    find_program(CLANGC NAMES clang-12 clang-14 clang)
    if (CLANGC MATCHES "CLANGC-NOTFOUND")
      message(SEND_ERROR "clang not found")
    else()
      message(STATUS "Using ${CLANGC} for c to LLVM IR translation")
    endif()
  endif()

  find_program(LLVMLINK NAMES llvm-link-12 llvm-link-14 llvm-link)
  if (LLVMLINK MATCHES "LLVMLINK-NOTFOUND")
    message(SEND_ERROR "llvm-link not found")
  else()
    message(STATUS "Using ${LLVMLINK} for LLVM IR linking")
  endif()
  find_program(LLVMOPT NAMES opt-12 opt-14 opt)
  if (LLVMOPT MATCHES "LLVMOPT-NOTFOUND")
    message(SEND_ERROR "llvm opt tool not found")
  else()
    message(STATUS "Using ${LLVMOPT} for LLVM IR optimization")
  endif()

  find_program(LLVMLLC NAMES llc-12 llc-14 llc)
  if (LLVMLLC MATCHES "LLVMLLC-NOTFOUND")
    message(SEND_ERROR "llvm llc tool not found")
  else()
    message(STATUS "Using ${LLVMLLC} for LLVM IR to binary code compilation")
  endif()
  find_program(LLVMAS NAMES llvm-as-12 llvm-as-14 llvm-as)
  if (LLVMAS MATCHES "LLVMAS-NOTFOUND")
    message(SEND_ERROR "llvm-as not found")
  else()
    message(STATUS "Using ${LLVMAS} for LLVM IR -> BC assembling")
  endif()
endif()

function(llvm_compile_cxx Tgt Inpt Out Tool UseC)
  list(APPEND TARGET_INCLUDES "-I$<JOIN:$<TARGET_PROPERTY:${Tgt},INCLUDE_DIRECTORIES>,$<SEMICOLON>-I>")
  list(APPEND TARGET_COMPILE_OPTIONS "'$<JOIN:$<TARGET_GENEX_EVAL:${Tgt},$<TARGET_PROPERTY:${Tgt},COMPILE_OPTIONS>>,'$<SEMICOLON>'>'")
  list(APPEND TARGET_COMPILE_DEFINITIONS "'-D$<JOIN:$<TARGET_GENEX_EVAL:${Tgt},$<TARGET_PROPERTY:${Tgt},COMPILE_DEFINITIONS>>,'$<SEMICOLON>'-D>'")
  if (${UseC})
    set(STD_FLAG "")
    separate_arguments(LANG_FLAGS NATIVE_COMMAND ${CMAKE_C_FLAGS})
    separate_arguments(FLAGSLIST_DEBUG NATIVE_COMMAND ${CMAKE_C_FLAGS_DEBUG})
    separate_arguments(FLAGSLIST_RELEASE NATIVE_COMMAND ${CMAKE_C_FLAGS_RELEASE})
    separate_arguments(FLAGSLIST_MINSIZEREL NATIVE_COMMAND ${CMAKE_C_FLAGS_MINSIZEREL})
    separate_arguments(FLAGSLIST_RELWITHDEBINFO NATIVE_COMMAND ${CMAKE_C_FLAGS_RELWITHDEBINFO})
  else()
    get_target_property(TARGET_STANDARD ${Tgt} CXX_STANDARD)
    set(STD_FLAG "-std=c++${TARGET_STANDARD}")
    separate_arguments(LANG_FLAGS NATIVE_COMMAND ${CMAKE_CXX_FLAGS})
    separate_arguments(FLAGSLIST_DEBUG NATIVE_COMMAND ${CMAKE_CXX_FLAGS_DEBUG})
    separate_arguments(FLAGSLIST_RELEASE NATIVE_COMMAND ${CMAKE_CXX_FLAGS_RELEASE})
    separate_arguments(FLAGSLIST_MINSIZEREL NATIVE_COMMAND ${CMAKE_CXX_FLAGS_MINSIZEREL})
    separate_arguments(FLAGSLIST_RELWITHDEBINFO NATIVE_COMMAND ${CMAKE_CXX_FLAGS_RELWITHDEBINFO})
  endif()

  add_custom_command(
    OUTPUT ${Out}
    COMMAND
    ${Tool}
    ${TARGET_INCLUDES}
    ${LANG_FLAGS}
    "$<$<CONFIG:DEBUG>:${FLAGSLIST_DEBUG}>"
    "$<$<CONFIG:RELEASE>:${FLAGSLIST_RELEASE}>"
    "$<$<CONFIG:MINSIZEREL>:${FLAGSLIST_MINSIZEREL}>"
    "$<$<CONFIG:RELWITHDEBINFO>:${FLAGSLIST_RELWITHDEBINFO}>"
    ${TARGET_COMPILE_DEFINITIONS}
    ${STD_FLAG}
    ${TARGET_COMPILE_OPTIONS}
    -Wno-unknown-warning-option
    -fno-lto
    -emit-llvm
    -c
    ${Inpt}
    -o
    ${Out}
    COMMAND_EXPAND_LISTS
    DEPENDS ${Inpt} ${Tool}
  )
endfunction()

function(llvm_compile_c Tgt Inpt Out Tool)
  llvm_compile_cxx(${Tgt} ${Inpt} ${Out} ${Tool} TRUE)
endfunction()
