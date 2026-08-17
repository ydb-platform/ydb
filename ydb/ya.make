RECURSE(
    apps
    docs
    core
    library
    mvp
    public
    services
    tools
)

IF(NOT EXPORT_CMAKE)
  RECURSE(
    tests
  )
ENDIF()
