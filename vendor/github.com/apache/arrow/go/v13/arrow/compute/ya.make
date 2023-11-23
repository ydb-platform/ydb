GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    arithmetic.go
    cast.go
    datum.go
    datumkind_string.go
    doc.go
    exec.go
    executor.go
    expression.go
    fieldref.go
    funckind_string.go
    functions.go
    registry.go
    scalar_bool.go
    scalar_compare.go
    selection.go
    utils.go
    vector_hash.go
    vector_run_ends.go
)

GO_TEST_SRCS(
    exec_internals_test.go
    exec_test.go
)

GO_XTEST_SRCS(
    arithmetic_test.go
    cast_test.go
    expression_test.go
    fieldref_test.go
    functions_test.go
    registry_test.go
    scalar_bool_test.go
    scalar_compare_test.go
    vector_hash_test.go
    vector_run_end_test.go
    vector_selection_test.go
)

END()

RECURSE(internal)
