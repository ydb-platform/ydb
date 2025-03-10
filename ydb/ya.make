RECURSE(
    apps
    docs
    core
    library
    mvp
    public
    services
    tools
    yql_docs
)

IF(NOT EXPORT_CMAKE)
  RECURSE(
    tests
  )
ENDIF()


