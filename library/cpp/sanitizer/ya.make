IF (OS_LINUX)
    RECURSE(
        address/static
        leak/static
        memory/static
        thread/static
        undefined/static
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        address/dynamic
        leak/dynamic
        thread/dynamic
        undefined/dynamic
    )
ENDIF()

