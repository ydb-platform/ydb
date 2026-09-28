#pragma once

#include <ydb/library/yql/providers/common/pushdown/collection.h>

namespace NYql::NPq {

struct TCommonPushdownSettings: public NPushdown::TSettings {
    TCommonPushdownSettings()
        : NPushdown::TSettings(NLog::EComponent::ProviderGeneric)
    {
        using EFlag = NPushdown::TSettings::EFeatureFlag;
        Enable(
            // Operator features
            EFlag::ExpressionAsPredicate |
            EFlag::MinMax |
            EFlag::ArithmeticalExpressions |
            EFlag::ImplicitConversionToInt64 |
            EFlag::StringTypes |
            EFlag::LikeOperator |
            EFlag::DoNotCheckCompareArgumentsTypes |
            EFlag::InOperator |
            EFlag::IsDistinctOperator |
            EFlag::JustPassthroughOperators |
            EFlag::DivisionExpressions |
            EFlag::CastExpression |
            EFlag::ToBytesFromStringExpressions |
            EFlag::ToStringFromStringExpressions |
            EFlag::FlatMapOverOptionals |
            EFlag::PredicateAsExpression |
            // EFlag::UnaryOperators | // TODO: no support in connector protocol/Format/Serialize
            EFlag::StructOperators |

            // Type features
            EFlag::DateCtor |
            EFlag::DateTimeTypes |
            EFlag::TimestampCtor |
            EFlag::DecimalType |
            EFlag::IntervalCtor |
            0
        );
        EnableFunction("Re2.Grep");  // For REGEXP pushdown
    }
};

} // namespace NYql::NPq
