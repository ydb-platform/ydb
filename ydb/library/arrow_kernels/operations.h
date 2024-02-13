#pragma once

namespace NKikimr::NKernels {

enum class EOperation {
    Unspecified = 0,
    Constant,
    //
    CastBoolean,
    CastInt8,
    CastInt16,
    CastInt32,
    CastInt64,
    CastUInt8,
    CastUInt16,
    CastUInt32,
    CastUInt64,
    CastFloat,
    CastDouble,
    CastBinary,
    CastFixedSizeBinary,
    CastString,
    CastTimestamp,
    //
    IsValid,
    IsNull,
    //
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    //
    Invert,
    And,
    Or,
    Xor,
    //
    Add,
    Subtract,
    Multiply,
    Divide,
    Abs,
    Negate,
    Gcd,
    Lcm,
    Modulo,
    ModuloOrZero,
    AddNotNull,
    SubtractNotNull,
    MultiplyNotNull,
    DivideNotNull,
    //
    BinaryLength,
    MatchSubstring,
    MatchLike,
    StartsWith,
    EndsWith,
    // math
    Acosh,
    Atanh,
    Cbrt,
    Cosh,
    E,
    Erf,
    Erfc,
    Exp,
    Exp2,
    Exp10,
    Hypot,
    Lgamma,
    Pi,
    Sinh,
    Sqrt,
    Tgamma,
    // round
    Floor,
    Ceil,
    Trunc,
    Round,
    RoundBankers,
    RoundToExp2
};

}
