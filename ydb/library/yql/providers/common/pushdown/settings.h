#pragma once
#include <ydb/library/yql/utils/log/log_component.h>

#include <util/system/types.h>

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
        JustPassthroughOperators = 1 << 18 // if + coalesce + just
    };

    explicit TSettings(NLog::EComponent logComponent)
        : LogComponent(logComponent)
    {
    }

    TSettings(const TSettings&) = default;

    void Enable(ui64 flagsMask, bool set = true);

    bool IsEnabled(EFeatureFlag flagMask) const;

    NLog::EComponent GetLogComponent() const {
        return LogComponent;
    }

private:
    const NLog::EComponent LogComponent;
    ui64 FeatureFlags = 0;
};

} // namespace NYql::NPushdown
