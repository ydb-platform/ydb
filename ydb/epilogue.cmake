add_custom_target(ydb_py_protos)

add_custom_command(
    TARGET ydb_py_protos POST_BUILD
    COMMAND ${CMAKE_SOURCE_DIR}/ydb/tests/oss/launch/compile_protos.sh ${CMAKE_SOURCE_DIR} ydb library/cpp/actors
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Compiling PY protos..."
)
