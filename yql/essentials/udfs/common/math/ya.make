IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319902006 OUT_NOAUTO libmath_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(math_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        math_udf.cpp
    )
    
    USE_LLVM_BC14()
    
    LLVM_BC(
        math_ir.cpp
        lib/erfinv.cpp
        NAME Math
        SYMBOLS
        PiIR
        EIR
        EpsIR
        RoundDownwardIR
        RoundToNearestIR
        RoundTowardZeroIR
        RoundUpwardIR
        AbsIR
        AcosIR
        AsinIR
        AsinhIR
        AtanIR
        CbrtIR
        CeilIR
        CosIR
        CoshIR
        ErfIR
        ErfInvIR
        ErfcInvIR
        ExpIR
        Exp2IR
        FabsIR
        FloorIR
        LgammaIR
        RintIR
        SinIR
        SinhIR
        SqrtIR
        TanIR
        TanhIR
        TgammaIR
        TruncIR
        IsFiniteIR
        IsInfIR
        IsNaNIR
        Atan2IR
        FmodIR
        HypotIR
        RemainderIR
        PowIR
        LdexpIR
        LogIR
        Log2IR
        Log10IR
        SigmoidIR
        FuzzyEqualsIR
        RoundIR
        ModIR
        RemIR
    )
    
    PEERDIR(
        yql/essentials/udfs/common/math/lib
    )
    
    END()
ENDIF ()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
   test
)


