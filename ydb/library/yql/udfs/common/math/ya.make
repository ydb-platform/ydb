YQL_UDF(math_udf)

YQL_ABI_VERSION(
    2
    16
    0
)

OWNER(g:yql g:yql_ydb_core)

SRCS(
    math_udf.cpp
)

LLVM_BC(
    math_ir.cpp
    NAME Math
    SYMBOLS
    PiIR
    EIR
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
