RECURSE(
    ast
    core
    dq
    minikql
    parser
    providers
    public
    sql
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
