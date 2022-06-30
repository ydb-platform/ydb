if (CMAKE_SCRIPT_MODE_FILE)
  file(GLOB_RECURSE RES_FILES 
    LIST_DIRECTORIES Off
    RELATIVE ${IN_DIR}
    ${IN_DIR}/*
  )
  set(RESC_ARGS ${OUT})
  foreach(res ${RES_FILES})
    set(RESC_ARGS ${RESC_ARGS} ${IN_DIR}/${res} ${PREFIX}/${res})
  endforeach()
  execute_process(
    COMMAND ${RESCOMPILER_BIN} ${RESC_ARGS}
    RESULT_VARIABLE RESC_RC
  )
  if (NOT RESC_RC EQUAL 0)
    message(FATAL_ERROR "Failed to compile resources into ${OUT}")
  endif()
  return()
endif()

function(add_gen_resources GenTarget Output)
  set(opts "")
  set(oneval_args IN_DIR PREFIX)
  set(multival_args "")
  cmake_parse_arguments(RECRES_ARG
    "${opts}"
    "${oneval_args}"
    "${multival_args}"
    ${ARGN}
  )
  add_custom_command(
    OUTPUT ${Output}
    COMMAND
      ${CMAKE_COMMAND} -DRESCOMPILER_BIN=$<TARGET_FILE:rescompiler> -DIN_DIR=${RECRES_ARG_IN_DIR} -DPREFIX=${RECRES_ARG_PREFIX} -DOUT=${Output} -P ${CMAKE_SOURCE_DIR}/ydb/core/viewer/recursive_resources.cmake
    DEPENDS ${GenTarget}
  )
endfunction()

function(add_dir_resources Output)
  set(opts "")
  set(oneval_args IN_DIR PREFIX)
  set(multival_args "")
  cmake_parse_arguments(RECRES_ARG
    "${opts}"
    "${oneval_args}"
    "${multival_args}"
    ${ARGN}
  )
  file(GLOB_RECURSE RES_FILES 
    LIST_DIRECTORIES Off
    RELATIVE ${RECRES_ARG_IN_DIR}
    CONFIGURE_DEPENDS
    ${RECRES_ARG_IN_DIR}/*
  )
  set(RESC_ARGS ${Output})
  foreach(res ${RES_FILES})
    set(RESC_ARGS ${RESC_ARGS} ${RECRES_ARG_IN_DIR}/${res} ${RECRES_ARG_PREFIX}/${res})
  endforeach()
  add_custom_command(
    OUTPUT ${Output}
    COMMAND
      rescompiler ${RESC_ARGS}
  )
endfunction()
