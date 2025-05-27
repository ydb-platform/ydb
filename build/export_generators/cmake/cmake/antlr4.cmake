function(ensure_antlr4)
    if(NOT ANTLR4_EXECUTABLE)
        find_program(ANTLR4_EXECUTABLE NAMES antlr4)
        if (NOT ANTLR4_EXECUTABLE)
            message(FATAL_ERROR "Unable to find antlr4 program. Please install antlr4 and make sure executable file present in the $PATH env.")
        endif()
    endif()
endfunction()

function(run_antlr4)
    ensure_antlr4()
    set(options "")
    set(oneValueArgs WORKING_DIRECTORY)
    set(multiValueArgs OUTPUT DEPENDS ANTLER_ARGS)
    cmake_parse_arguments(
        RUN_ANTLR4
         "${options}"
         "${oneValueArgs}"
         "${multiValueArgs}"
         ${ARGN}
    )

    add_custom_command(
        OUTPUT ${RUN_ANTLR4_OUTPUT}
        COMMAND ${ANTLR4_EXECUTABLE} ${RUN_ANTLR4_ANTLER_ARGS}
        WORKING_DIRECTORY ${RUN_ANTLR4_WORKING_DIRECTORY}
        DEPENDS ${RUN_ANTLR4_DEPENDS}
    )

endfunction()
