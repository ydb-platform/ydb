DLL_TOOL(custom_pid PREFIX "")

NO_RUNTIME()

IF (OS_LINUX)
    EXPORTS_SCRIPT(custom_pid.exports)

    SRCS(
        custom_pid.c
    )
ENDIF()

END()
