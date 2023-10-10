LLVM_BC(
    postgresql/src/backend/access/brin/brin.c
    pg_kernels.0.cpp

    NAME PgFuncs0

    SYMBOLS
    arrow_brin_desummarize_range
    arrow_brin_summarize_new_values
    arrow_brin_summarize_range
    arrow_brinhandler
)
