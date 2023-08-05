function(compile_python_protos Tgt)

add_custom_command(
    TARGET ${Tgt} POST_BUILD
    COMMAND ${CMAKE_SOURCE_DIR}/ydb/tests/oss/launch/compile_protos.sh ${CMAKE_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Compiling PY protos..."
)

endfunction()

