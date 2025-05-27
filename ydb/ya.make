RECURSE(
    apps
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

IF(NOT OPENSOURCE)
  # YFM tool is not supported 
  # for OSS ya make yet
  RECURSE(
    docs
    yql_docs
  )
ENDIF()
