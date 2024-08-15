function(add_yunittest)
  set(opts "")
  set(oneval_args NAME TEST_TARGET)
  set(multival_args TEST_ARG)
  cmake_parse_arguments(YUNITTEST_ARGS
    "${opts}"
    "${oneval_args}"
    "${multival_args}"
    ${ARGN}
  )

  get_property(SPLIT_FACTOR  TARGET ${YUNITTEST_ARGS_TEST_TARGET} PROPERTY SPLIT_FACTOR)
  get_property(SPLIT_TYPE TARGET ${YUNITTEST_ARGS_TEST_TARGET} PROPERTY SPLIT_TYPE)

  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/run_testpack")
    add_test(NAME ${YUNITTEST_ARGS_NAME} COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/run_testpack" ${YUNITTEST_ARGS_TEST_ARG})
    set_property(TEST ${YUNITTEST_ARGS_NAME} PROPERTY ENVIRONMENT "source_root=${YDB_SDK_SOURCE_DIR};build_root=${YDB_SDK_BINARY_DIR};test_split_factor=${SPLIT_FACTOR};test_split_type=${SPLIT_TYPE}")
    return()
  endif()

  if (${SPLIT_FACTOR} EQUAL 1)
  	add_test(NAME ${YUNITTEST_ARGS_NAME} COMMAND ${YUNITTEST_ARGS_TEST_TARGET} ${YUNITTEST_ARGS_TEST_ARG})
  	return()
  endif()

  if ("${SPLIT_TYPE}")
    set(FORK_MODE_ARG --fork-mode ${SPLIT_TYPE})
  endif()
  math(EXPR LastIdx "${SPLIT_FACTOR} - 1")
  foreach(Idx RANGE ${LastIdx})
    add_test(NAME ${YUNITTEST_ARGS_NAME}_${Idx}
      COMMAND Python3::Interpreter ${YDB_SDK_SOURCE_DIR}/scripts/split_unittest.py --split-factor ${SPLIT_FACTOR} ${FORK_MODE_ARG} --shard ${Idx}
       $<TARGET_FILE:${YUNITTEST_ARGS_TEST_TARGET}> ${YUNITTEST_ARGS_TEST_ARG})
  endforeach()
endfunction()

function(set_yunittest_property)
  set(opts "")
  set(oneval_args TEST PROPERTY)
  set(multival_args )
  cmake_parse_arguments(YUNITTEST_ARGS
    "${opts}"
    "${oneval_args}"
    "${multival_args}"
    ${ARGN}
  )
  get_property(SPLIT_FACTOR TARGET ${YUNITTEST_ARGS_TEST} PROPERTY SPLIT_FACTOR)

  if ((${SPLIT_FACTOR} EQUAL 1) OR (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/run_testpack"))
    set_property(TEST ${YUNITTEST_ARGS_TEST} PROPERTY ${YUNITTEST_ARGS_PROPERTY} ${YUNITTEST_ARGS_UNPARSED_ARGUMENTS})
  	return()
  endif()

  math(EXPR LastIdx "${SPLIT_FACTOR} - 1")
  foreach(Idx RANGE ${LastIdx})
    set_property(TEST ${YUNITTEST_ARGS_TEST}_${Idx} PROPERTY ${YUNITTEST_ARGS_PROPERTY} ${YUNITTEST_ARGS_UNPARSED_ARGUMENTS})
  endforeach()
endfunction()

function(add_ydb_test)
  set(opts GTEST)
  set(oneval_args NAME)
  set(multival_args INCLUDE_DIRS SOURCES LINK_LIBRARIES LABELS)
  cmake_parse_arguments(YDB_TEST
    "${opts}"
    "${oneval_args}"
    "${multival_args}"
    ${ARGN}
  )

  add_executable(${YDB_TEST_NAME})
  target_include_directories(${YDB_TEST_NAME} PRIVATE ${YDB_TEST_INCLUDE_DIRS})
  target_link_libraries(${YDB_TEST_NAME} PRIVATE ${YDB_TEST_LINK_LIBRARIES})
  target_sources(${YDB_TEST_NAME} PRIVATE ${YDB_TEST_SOURCES})

  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
    target_link_libraries(${YDB_TEST_NAME} PRIVATE
      cpuid_check
    )
  endif()

  if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_options(${YDB_TEST_NAME} PRIVATE
      -ldl
      -lrt
      -Wl,--no-as-needed
      -lpthread
    )
  elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    target_link_options(${YDB_TEST_NAME} PRIVATE
      -Wl,-platform_version,macos,11.0,11.0
      -framework
      CoreFoundation
    )
  endif()

  set_property(
    TARGET
    ${YDB_TEST_NAME}
    PROPERTY
    SPLIT_FACTOR
    1
  )
  if (YDB_TEST_GTEST)
    add_yunittest(
      NAME
      ${YDB_TEST_NAME}
      TEST_TARGET
      ${YDB_TEST_NAME}
    )
  else()
    add_yunittest(
      NAME
      ${YDB_TEST_NAME}
      TEST_TARGET
      ${YDB_TEST_NAME}
      TEST_ARG
      --print-before-suite
      --print-before-test
      --fork-tests
      --print-times
      --show-fails
    )
  endif()

  set_yunittest_property(
    TEST
    ${YDB_TEST_NAME}
    PROPERTY
    LABELS
    MEDIUM
    ${YDB_TEST_LABELS}
  )

  set_yunittest_property(
    TEST
    ${YDB_TEST_NAME}
    PROPERTY
    PROCESSORS
    1
  )

  set_yunittest_property(
    TEST
    ${YDB_TEST_NAME}
    PROPERTY
    TIMEOUT
    600
  )

  vcs_info(${YDB_TEST_NAME})
endfunction()
