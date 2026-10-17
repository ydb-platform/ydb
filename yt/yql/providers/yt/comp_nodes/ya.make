LIBRARY()

SRCS(
    yql_mkql_block_table_content.h  # gn: slot yt_codegen
    yql_mkql_file_input_state.h  # gn: slot yt_codegen
    yql_mkql_file_list.h  # gn: slot yt_codegen
    yql_mkql_input.h  # gn: slot yt_codegen
    yql_mkql_input_stream.h  # gn: slot yt_codegen
    yql_mkql_output.h  # gn: slot yt_codegen
    yql_mkql_table.h  # gn: slot yt_codegen
    yql_mkql_table_content.h  # gn: slot yt_codegen
    yql_mkql_ungrouping_list.h  # gn: slot yt_codegen
)

END()

RECURSE(
    llvm16
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)
