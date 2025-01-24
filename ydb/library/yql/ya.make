RECURSE(
    core
    dq
    minikql
    providers
    public
    tests
    tools
    udfs
    utils
)

IF (OS_LINUX)
    # YT target is a shared library, works only under Linux.
    RECURSE(
        yt
    )
ENDIF()
