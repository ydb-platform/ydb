RECURSE(
    ast
    core
    core/arrow_kernels/request
    core/arrow_kernels/registry
    core/common_opt
    core/credentials
    core/expr_nodes
    core/expr_nodes_gen
    core/extract_predicate
    core/facade
    core/file_storage
    core/issue
    core/peephole_opt
    core/services
    core/sql_types
    core/type_ann
    core/user_data
    dq
    minikql
    parser
    protos
    providers
    public
    sql
    udfs
    utils
)

# TODO(max42): Recurse unconditionally as a final step of YT-19210.
IF (NOT EXPORT_CMAKE)
    RECURSE(
        core/url_preprocessing
    )
ENDIF()

#TODO(max42): Recurse unconditionally as a final step of YT-19210.
IF (NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        core/extract_predicate/ut
        core/ut
    )
ENDIF()
