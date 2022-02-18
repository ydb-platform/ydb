add_library(commands)
target_link_libraries(commands PUBLIC
  contrib-libs-cxxsupp
  yutil
  clicommands
)
target_sources(commands PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/ydb/commands/ydb_root.cpp
)
