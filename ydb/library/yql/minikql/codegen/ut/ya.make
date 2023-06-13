UNITTEST_FOR(ydb/library/yql/minikql/codegen)

NO_COMPILER_WARNINGS()

SRCS(
    codegen_ut.cpp
)

IF (OS_WINDOWS)
    LLVM_BC(
        fib.cpp
        sum_sqr.cpp
        sum_sqr2.cpp
        str.cpp
        128_bit.cpp
        128_bit_win.ll
        NAME Funcs
        SYMBOLS
        fib
        sum_sqr
        sum_sqr2
        sum_sqr_128
        sum_sqr_128_ir
        str_size
    )
ELSE()
    LLVM_BC(
        fib.cpp
        sum_sqr.cpp
        sum_sqr2.cpp
        str.cpp
        128_bit.cpp
        128_bit.ll
        NAME Funcs
        SYMBOLS
        fib
        sum_sqr
        sum_sqr2
        sum_sqr_128
        sum_sqr_128_ir
        str_size
    )
ENDIF()

END()
