YQL_UDF_YDB(math_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    math_udf.cpp
)

LLVM_BC(
    math_ir.cpp
    lib/erfinv.cpp
    NAME Math
    SYMBOLS
    PiIR
    EIR
    EpsIR
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
    ydb/library/yql/udfs/common/math/lib
)

END()

RECURSE_FOR_TESTS(
   test
)

