RECURSE(
    apps
    docs
    core
    library
    mvp
    public
    services
    tools
    udfs
)

IF(NOT EXPORT_CMAKE)
  RECURSE(
    tests
  )
ENDIF()
