function(ensure_antlr)
    if(NOT ANTLR3_EXECUTABLE)
        find_program(ANTLR3_EXECUTABLE
                     NAMES antlr3)
        if (NOT ANTLR3_EXECUTABLE)
            message(FATAL_ERROR "Unable to find antlr3 program. Please install antlr3 and make sure executable file present in the $PATH env.")
        endif()
    endif()
endfunction()

function(run_antlr)
    ensure_antlr()
    set(options "")
    set(oneValueArgs WORKING_DIRECTORY)
    set(multiValueArgs OUTPUT DEPENDS ANTLER_ARGS)
    cmake_parse_arguments(
        RUN_ANTLR
         "${options}"
         "${oneValueArgs}"
         "${multiValueArgs}"
         ${ARGN}
    )

    add_custom_command(
        OUTPUT ${RUN_ANTLR_OUTPUT}
        COMMAND ${ANTLR3_EXECUTABLE} ${RUN_ANTLR_ANTLER_ARGS}
        WORKING_DIRECTORY ${RUN_ANTLR_WORKING_DIRECTORY}
        DEPENDS ${RUN_ANTLR_DEPENDS}
    )

endfunction()
