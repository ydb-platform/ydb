add_library(src-Modules-_sqlite)
target_compile_options(src-Modules-_sqlite PRIVATE
  -DMODULE_NAME="sqlite3"
)
target_include_directories(src-Modules-_sqlite PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/python/Include
)
target_include_directories(src-Modules-_sqlite PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/sqlite3
  ${CMAKE_SOURCE_DIR}/contrib/libs/python/Include
)
target_link_libraries(src-Modules-_sqlite PUBLIC
  contrib-libs-sqlite3
)
target_sources(src-Modules-_sqlite PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/cache.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/connection.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/cursor.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/microprotocols.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/module.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/prepare_protocol.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/row.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/statement.c
  ${CMAKE_SOURCE_DIR}/contrib/tools/python3/src/Modules/_sqlite/util.c
)
