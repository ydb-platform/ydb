add_library(yql-sql-settings)
target_link_libraries(yql-sql-settings PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-split
  yql-core-issue
  core-issue-protos
  library-yql-utils
)
target_sources(yql-sql-settings PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/settings/translation_settings.cpp
)
