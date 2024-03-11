OWNER(g:yql)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/${MODDIR}
)

COPY_FILE(
    TEXT
    ../codegen_ut_llvm_deps.h.txt codegen_ut_llvm_deps.h
    OUTPUT_INCLUDES
       llvm/IR/Constants.h
       llvm/IR/DerivedTypes.h
       llvm/IR/Instructions.h
       llvm/IR/LLVMContext.h
       llvm/IR/Module.h
)


COPY_FILE(
    AUTO
    ../codegen_ut.cpp codegen_ut.cpp
    OUTPUT_INCLUDES
        codegen_ut_llvm_deps.h
)

SRCDIR(ydb/library/yql/minikql/codegen/ut)

IF (OS_WINDOWS)
    LLVM_BC(
        fib.cpp
        sum_sqr.cpp
        sum_sqr2.cpp
        str.cpp
        128_bit.cpp
        128_bit_win.ll
        NAME Funcs
        SUFFIX .${LLVM_VER}
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
        SUFFIX .${LLVM_VER}
        SYMBOLS
        fib
        sum_sqr
        sum_sqr2
        sum_sqr_128
        sum_sqr_128_ir
        str_size
    )
ENDIF()


