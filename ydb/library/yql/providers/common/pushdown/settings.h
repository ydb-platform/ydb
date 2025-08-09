#pragma once
#include <yql/essentials/utils/log/log_component.h>

#include <util/system/types.h>

#include <unordered_set>

namespace NYql::NPushdown {

struct TSettings {
    enum EFeatureFlag : ui64 {
        LikeOperator = 1,
        LikeOperatorOnlyForUtf8 = 1 << 1,
        JsonQueryOperators = 1 << 2,
        JsonExistsOperator = 1 << 3,
        LogicalXorOperator = 1 << 4,
        ExpressionAsPredicate = 1 << 5, // Bool columns and bool expressions can take part in AND/OR/NOT statements
        ArithmeticalExpressions = 1 << 6, // *, +, -
        ParameterExpression = 1 << 7, // Query parameters
        CastExpression = 1 << 8, // CAST()
        StringTypes = 1 << 9, // Support string types
        DateTimeTypes = 1 << 10, // Date, Datetime, Timestamp
        UuidType = 1 << 11,
        DecimalType = 1 << 12,
        DyNumberType = 1 << 13,
        ImplicitConversionToInt64 = 1 << 14, // Allow implicit conversions to 64-bits integers from other types of integers
        UnaryOperators = 1 << 15, // -, Abs, Size
        DoNotCheckCompareArgumentsTypes = 1 << 16,
        TimestampCtor = 1 << 17,
        JustPassthroughOperators = 1 << 18, // if + coalesce + just
        InOperator = 1 << 19, // IN()
        IsDistinctOperator = 1 << 20, // IS NOT DISTINCT FROM / IS DISTINCT FROM
        DivisionExpressions = 1 << 21, // %, / -- NOTE: division by zero is not handled and also pushdown

        // Option which enables partial pushdown for sequence of OR
        // For example next predicate:
        // ($A AND $B) OR ($C AND $D)
        // May be partially pushdowned as:
        // $A OR $C
        // In case of unsupported / complicated expressions $B and $D
        SplitOrOperator = 1 << 22,
        ToBytesFromStringExpressions = 1 << 23, // ToBytes(string like)
        FlatMapOverOptionals = 1 << 24, // FlatMap(Optional<T>, Lambda (T) -> Optional<U>)
        ToStringFromStringExpressions = 1 << 25, // ToString(string like)
        IntervalCtor = 1 << 26,
        MinMax = 1 << 27,
        NonDeterministic = 1 << 28,
    };

    explicit TSettings(NLog::EComponent logComponent)
        : LogComponent(logComponent)
    {
    }

    TSettings(const TSettings&) = default;

    void Enable(ui64 flagsMask, bool set = true);

    void EnableFunction(const TString& functionName);

    bool IsEnabled(EFeatureFlag flagMask) const;

    bool IsEnabledFunction(const TString& functionName) const;

    NLog::EComponent GetLogComponent() const {
        return LogComponent;
    }

private:
    const NLog::EComponent LogComponent;
    ui64 FeatureFlags = 0;
    std::unordered_set<TString> EnabledFunctions;
};

} // namespace NYql::NPushdown
