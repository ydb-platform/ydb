
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(contrib-libs-sqlite3)
target_compile_options(contrib-libs-sqlite3 PRIVATE
  -DBUILD_sqlite
  -DHAVE_USLEEP
  -DSQLITE_ENABLE_COLUMN_METADATA
  -DSQLITE_ENABLE_DBSTAT_VTAB
  -DSQLITE_ENABLE_FTS3
  -DSQLITE_ENABLE_FTS3_PARENTHESIS
  -DSQLITE_ENABLE_FTS3_TOKENIZER
  -DSQLITE_ENABLE_FTS4
  -DSQLITE_ENABLE_FTS5
  -DSQLITE_ENABLE_JSON1
  -DSQLITE_ENABLE_MATH_FUNCTIONS
  -DSQLITE_ENABLE_RTREE
  -DSQLITE_ENABLE_STMT_SCANSTATUS
  -DSQLITE_ENABLE_UNLOCK_NOTIFY
  -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
  -DSQLITE_HAVE_ZLIB=1
  -DSQLITE_MAX_EXPR_DEPTH=10000
  -DSQLITE_MAX_VARIABLE_NUMBER=250000
  -DSQLITE_SECURE_DELETE
  -DSQLITE_SOUNDEX
  -DSQLITE_TEMP_STORE=1
  -DSQLITE_THREADSAFE=1
  -DSQLITE_OS_UNIX
)
target_include_directories(contrib-libs-sqlite3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/sqlite3
)
target_sources(contrib-libs-sqlite3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/sqlite3/sqlite3.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/sqlite3/test_multiplex.c
)
