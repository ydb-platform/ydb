RECURSE(
    apps
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

IF(NOT OPENSOURCE)
  # YFM tool is not supported 
  # for OSS ya make yet
  RECURSE(
    docs
  )
ENDIF()
