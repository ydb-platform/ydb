YQL_UDF_CONTRIB(math_udf)

    YQL_ABI_VERSION(
        2
        46
        0
    )

    SRCS(
        math_udf.cpp
    )

    # USE_LLVM_BC16 pins the bitcode compiler to Clang 16, which rejects
    # -fcoverage-mcdc (Clang 18+ flag, added to BC_CXXFLAGS via $CXXFLAGS under
    # MC/DC coverage). Fall back to the native DISABLE_IR path, same as on
    # non-Linux. Further move to LLVM 18 will be done in scope of YQL-21388.
    IF (NOT OS_LINUX OR CLANG_MCDC_COVERAGE == "yes")
        CFLAGS(-DDISABLE_IR)
    ELSE()
        USE_LLVM_BC16()

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

    ENDIF()

    PEERDIR(
        yql/essentials/core/langver
        yql/essentials/udfs/common/math/lib
    )

    END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
   test
)
