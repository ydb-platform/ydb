if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CLANGPLUSPLUS ${CMAKE_CXX_COMPILER})
  message(STATUS "Using ${CLANGPLUSPLUS} for c++ to LLVM IR translation")
else()
  find_program(CLANGPLUSPLUS NAMES clang++-12 clang++)
  if (CLANGPLUSPLUS MATCHES "CLANGPLUSPLUS-NOTFOUND")
    message(SEND_ERROR "clang++ not found")
  else()
    message(STATUS "Using ${CLANGPLUSPLUS} for c++ to LLVM IR translation")
  endif()
endif()

find_program(LLVMLINK NAMES llvm-link-12 llvm-link)
if (LLVMLINK MATCHES "LLVMLINK-NOTFOUND")
  message(SEND_ERROR "llvm-link not found")
else()
  message(STATUS "Using ${LLVMLINK} for LLVM IR linking")
endif()
find_program(LLVMOPT NAMES opt-12 opt)
if (LLVMOPT MATCHES "LLVMOPT-NOTFOUND")
  message(SEND_ERROR "llvm opt tool not found")
else()
  message(STATUS "Using ${LLVMOPT} for LLVM IR optimization")
endif()

function(llvm_compile_cxx Tgt Inpt Out Tool)
  list(APPEND TARGET_INCLUDES "-I$<JOIN:$<TARGET_PROPERTY:${Tgt},INCLUDE_DIRECTORIES>,$<SEMICOLON>-I>")
  list(APPEND TARGET_COMPILE_OPTIONS "$<JOIN:$<TARGET_PROPERTY:${Tgt},COMPILE_OPTIONS>,$<SEMICOLON>>")
  get_target_property(TARGET_STANDARD ${Tgt} CXX_STANDARD)

  add_custom_command(
    OUTPUT ${Out}
    COMMAND
    ${Tool}
    ${TARGET_INCLUDES}
    ${TARGET_COMPILE_OPTIONS}
    -std=c++${TARGET_STANDARD}
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
