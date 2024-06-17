IF (NOT OPENSOURCE)
    # Linters can be broken after import to Arcadia, because paths are changed
    NO_LINT()
ENDIF()

RECURSE(
    apps
    core
    library
    mvp
    public
    services
    tests
    tools
)
