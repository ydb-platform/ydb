#include "kqp_opt_predicate_selectivity.h"
#include "kqp_opt_stat.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;

namespace {

    using namespace NKikimr::NKqp;
    using namespace NKikimr::NKqp::NPredicateTree;

    const double TWO_COLUMNS_DEFAULT_SELECTIVITY = 0.3;

    double DefaultEqualitySelectivity(const std::shared_ptr<NKikimr::NKqp::TOptimizerStatistics>& stats, const TString& attributeName) {
        if (stats == nullptr) {
            return 1.0;
        }

        if (stats->KeyColumns && stats->KeyColumns->Data.size() == 1 && attributeName == stats->KeyColumns->Data[0]) {
            return 1.0 / std::max<double>(stats->Nrows, 1.0);
        } else {
            if (stats->Nrows > 1) {
                return 0.1;
            }

            return 1.0;
        }
    }

    double DefaultInequalitySelectivity(const std::shared_ptr<NKikimr::NKqp::TOptimizerStatistics>& stats, const TString& attributeName) {
        Y_UNUSED(attributeName);

        if (stats == nullptr) {
            return 1.0;
        }

        return 0.4;
    }

    // Estimates number of rows based on histogram and predicate type.
    // Supported types by histogram: i8, i16, i32, ui8, ui16, ui32, ui64, float, double
    template <typename T>
    TMaybe<ui64> EstimateInequalityPredicateByType(const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& estimator,
                                                    T val, EInequalityPredicateType predicate) {
        switch (predicate) {
            case EInequalityPredicateType::Less:
                return estimator->EstimateLess<T>(val);
            case EInequalityPredicateType::LessOrEqual:
                return estimator->EstimateLessOrEqual<T>(val);
            case EInequalityPredicateType::Greater:
                return estimator->EstimateGreater<T>(val);
            case EInequalityPredicateType::GreaterOrEqual:
                return estimator->EstimateGreaterOrEqual<T>(val);
            case EInequalityPredicateType::Equal:
                return estimator->EstimateEqual<T>(val);
            case EInequalityPredicateType::NotEqual:
                auto equal = estimator->EstimateEqual<T>(val);
                auto total = estimator->GetNumElements();
                return (equal <= total) ? (total - equal) : 0;
        }
        return Nothing();
    }

    template <typename T>
    TMaybe<ui64> EstimateRangePredicateByType(const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& estimator,
                                                T leftVal, T rightVal,
                                                EInequalityPredicateType leftPredicate, EInequalityPredicateType rightPredicate) {
        if (leftVal > rightVal) {
            return Nothing();
        } else if (leftVal == rightVal) {
            return estimator->EstimateEqual<T>(leftVal);
        }

        if (leftPredicate == EInequalityPredicateType::Greater && rightPredicate == EInequalityPredicateType::Less) {
            return estimator->EstimateRangeGreaterLess<T>(leftVal, rightVal);
        } else if (leftPredicate == EInequalityPredicateType::Greater && rightPredicate == EInequalityPredicateType::LessOrEqual) {
            return estimator->EstimateRangeGreaterLessOrEqual<T>(leftVal, rightVal);
        } else if (leftPredicate == EInequalityPredicateType::GreaterOrEqual && rightPredicate == EInequalityPredicateType::Less) {
            return estimator->EstimateRangeGreaterOrEqualLess<T>(leftVal, rightVal);
        } else if (leftPredicate == EInequalityPredicateType::GreaterOrEqual && rightPredicate == EInequalityPredicateType::LessOrEqual) {
            return estimator->EstimateRangeGreaterOrEqualLessOrEqual<T>(leftVal, rightVal);
        }

        return Nothing();
    }

    // Returns a number of rows based on predicate.
    TMaybe<ui64> EstimateInequalityPredicateByHistogramImpl(NYql::NNodes::TExprBase maybeLiteral, const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType predicate) {
        const TMaybe<TString> literal = ExtractLiteral(maybeLiteral);
        if (literal.Defined()) {
            const TString value = literal.GetRef();
            if (columnType == "Bool") {
                ui8 val = FromString<bool>(value);
                return EstimateInequalityPredicateByType<ui8>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Uint8") {
                ui8 val = FromString<ui8>(value);
                return EstimateInequalityPredicateByType<ui8>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Int8") {
                i8 val = FromString<i8>(value);
                return EstimateInequalityPredicateByType<i8>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Uint16") {
                ui16 val = FromString<ui16>(value);
                return EstimateInequalityPredicateByType<ui16>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Int16") {
                i16 val = FromString<i16>(value);
                return EstimateInequalityPredicateByType<i16>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Uint32") {
                ui32 val = FromString<ui32>(value);
                return EstimateInequalityPredicateByType<ui32>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Int32") {
                i32 val = FromString<i32>(value);
                return EstimateInequalityPredicateByType<i32>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Uint64") {
                ui64 val = FromString<ui64>(value);
                return EstimateInequalityPredicateByType<ui64>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Int64") {
                i64 val = FromString<i64>(value);
                return EstimateInequalityPredicateByType<i64>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Float") {
                float val = FromString<float>(value);
                return EstimateInequalityPredicateByType<float>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Double" || columnType.StartsWith("Decimal")) {
                double val = FromString<double>(value);
                return EstimateInequalityPredicateByType<double>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Date") {
                ui16 val = FromString<ui16>(value);
                return EstimateInequalityPredicateByType<ui16>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Date32") {
                i32 val = FromString<i32>(value);
                return EstimateInequalityPredicateByType<i32>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Datetime") {
                ui32 val = FromString<ui32>(value);
                return EstimateInequalityPredicateByType<ui32>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Timestamp") {
                ui64 val = FromString<ui64>(value);
                return EstimateInequalityPredicateByType<ui64>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Interval" || columnType == "Interval64" || columnType == "Datetime64" || columnType == "Timestamp64") {
                i64 val = FromString<i64>(value);
                return EstimateInequalityPredicateByType<i64>(eqWidthHistogram, val, predicate);
            }

            // TODO: Add support for other types.
            // Utf8, String, Text, Uuid, Bytes, DyNumber, Yson, Json, JsonDocument
            return Nothing();
        }

        return Nothing();
    }

    TMaybe<ui64> EstimateInequalityPredicateByHistogram(NYql::NNodes::TExprBase maybeLiteral, const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType predicate) {
        try {
            return EstimateInequalityPredicateByHistogramImpl(maybeLiteral, columnType, eqWidthHistogram, predicate);
        } catch (const yexception&) {
            return Nothing();
        }
    }

    TMaybe<ui64> EstimateRangePredicateByHistogramImpl(NYql::NNodes::TExprBase maybeLeftLiteral, NYql::NNodes::TExprBase maybeRightLiteral,
                                            const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType leftPredicate, EInequalityPredicateType rightPredicate) {
        const TMaybe<TString> leftLiteral = ExtractLiteral(maybeLeftLiteral);
        const TMaybe<TString> rightLiteral = ExtractLiteral(maybeRightLiteral);

        if (leftLiteral.Defined() && rightLiteral.Defined()) {
            const TString leftValue = leftLiteral.GetRef();
            const TString rightValue = rightLiteral.GetRef();

            if (columnType == "Bool") {
                ui8 leftVal = FromString<bool>(leftValue);
                ui8 rightVal = FromString<bool>(rightValue);
                return EstimateRangePredicateByType<ui8>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Uint8") {
                ui8 leftVal = FromString<ui8>(leftValue);
                ui8 rightVal = FromString<ui8>(rightValue);
                return EstimateRangePredicateByType<ui8>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Int8") {
                i8 leftVal = FromString<i8>(leftValue);
                i8 rightVal = FromString<i8>(rightValue);
                return EstimateRangePredicateByType<i8>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Uint16") {
                ui16 leftVal = FromString<ui16>(leftValue);
                ui16 rightVal = FromString<ui16>(rightValue);
                return EstimateRangePredicateByType<ui16>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Int16") {
                i16 leftVal = FromString<i16>(leftValue);
                i16 rightVal = FromString<i16>(rightValue);
                return EstimateRangePredicateByType<i16>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Uint32") {
                ui32 leftVal = FromString<ui32>(leftValue);
                ui32 rightVal = FromString<ui32>(rightValue);
                return EstimateRangePredicateByType<ui32>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Int32") {
                i32 leftVal = FromString<i32>(leftValue);
                i32 rightVal = FromString<i32>(rightValue);
                return EstimateRangePredicateByType<i32>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Uint64") {
                ui64 leftVal = FromString<ui64>(leftValue);
                ui64 rightVal = FromString<ui64>(rightValue);
                return EstimateRangePredicateByType<ui64>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Int64") {
                i64 leftVal = FromString<i64>(leftValue);
                i64 rightVal = FromString<i64>(rightValue);
                return EstimateRangePredicateByType<i64>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Float") {
                float leftVal = FromString<float>(leftValue);
                float rightVal = FromString<float>(rightValue);
                return EstimateRangePredicateByType<float>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Double" || columnType.StartsWith("Decimal")) {
                double leftVal = FromString<double>(leftValue);
                double rightVal = FromString<double>(rightValue);
                return EstimateRangePredicateByType<double>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Date") {
                ui16 leftVal = FromString<ui16>(leftValue);
                ui16 rightVal = FromString<ui16>(rightValue);
                return EstimateRangePredicateByType<ui16>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Date32") {
                i32 leftVal = FromString<i32>(leftValue);
                i32 rightVal = FromString<i32>(rightValue);
                return EstimateRangePredicateByType<i32>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Datetime") {
                ui32 leftVal = FromString<ui32>(leftValue);
                ui32 rightVal = FromString<ui32>(rightValue);
                return EstimateRangePredicateByType<ui32>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Timestamp") {
                ui64 leftVal = FromString<ui64>(leftValue);
                ui64 rightVal = FromString<ui64>(rightValue);
                return EstimateRangePredicateByType<ui64>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Interval" || columnType == "Interval64" || columnType == "Datetime64" || columnType == "Timestamp64") {
                i64 leftVal = FromString<i64>(leftValue);
                i64 rightVal = FromString<i64>(rightValue);
                return EstimateRangePredicateByType<i64>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            }

            // TODO: Add support for other types.
            // Utf8, String, Text, Uuid, Bytes, DyNumber, Yson, Json, JsonDocument
            return Nothing();
        }

        return Nothing();
    }

    TMaybe<ui64> EstimateRangePredicateByHistogram(NYql::NNodes::TExprBase maybeLeftLiteral, NYql::NNodes::TExprBase maybeRightLiteral,
                                            const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType leftPredicate, EInequalityPredicateType rightPredicate) {
        try {
            return EstimateRangePredicateByHistogramImpl(maybeLeftLiteral, maybeRightLiteral, columnType, eqWidthHistogram, leftPredicate, rightPredicate);
        } catch (const yexception&) {
            return Nothing();
        }
    }

    TMaybe<ui32> EstimateEqualityPredicateBySketchImpl(NYql::NNodes::TExprBase maybeLiteral, TString columnType,
                                        const std::shared_ptr<NKikimr::TCountMinSketch>& countMinSketch) {
        const TMaybe<TString> literal = ExtractLiteral(maybeLiteral);
        if (literal.Defined()) {
            const TString value = literal.GetRef();
            if (columnType == "Bool") {
                ui8 val = FromString<bool>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Uint8") {
                ui8 val = FromString<ui8>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Int8") {
                i8 val = FromString<i8>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Uint16") {
                ui16 val = FromString<ui16>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Int16") {
                i16 val = FromString<i16>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Uint32") {
                ui32 val = FromString<ui32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Int32") {
                i32 val = FromString<i32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Uint64") {
                ui64 val = FromString<ui64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Int64") {
                i64 val = FromString<i64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Float") {
                float val = FromString<float>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Double" || columnType.StartsWith("Decimal")) {
                double val = FromString<double>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Date") {
                ui16 val = FromString<ui16>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Date32") {
                i32 val = FromString<i32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Datetime") {
                ui32 val = FromString<ui32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Timestamp") {
                ui64 val = FromString<ui64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Interval" || columnType == "Interval64" || columnType == "Datetime64" || columnType == "Timestamp64") {
                i64 val = FromString<i64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Utf8" || columnType == "String" || columnType == "Yson" || columnType == "Json") {
                return countMinSketch->Probe(value.data(), value.size());
            } else if (columnType == "Uuid") {
                const ui64* uuidData = reinterpret_cast<const ui64*>(value.data());
                std::pair<ui64, ui64> val{};
                val.first = uuidData[0];   // low128
                val.second = uuidData[1];  // high128
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            }

            // TODO: Add support for other types.
            // Text, Bytes, DyNumber, JsonDocument
            return Nothing();
        }

        return Nothing();
    }

    TMaybe<ui32> EstimateEqualityPredicateBySketch(NYql::NNodes::TExprBase maybeLiteral, TString columnType,
                                        const std::shared_ptr<NKikimr::TCountMinSketch>& countMinSketch) {
        try {
            return EstimateEqualityPredicateBySketchImpl(maybeLiteral, columnType, countMinSketch);
        } catch (const yexception&) {
            return Nothing();
        }
    }
}

namespace NKikimr::NKqp {

using namespace NPredicateTree;

double TPredicateSelectivityComputer::ComputeInequalitySelectivity(
    const TExprBase& left,
    const TExprBase& right,
    bool collectMembers,
    EInequalityPredicateType predicate
) {
    Y_UNUSED(collectMembers); // members are collected while the predicate tree is built
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeInequalitySelectivity(right, left, collectMembers, GetOppositePredicateType(predicate));
    }

    if (auto attribute = IsAttribute(left)) {
        // In case both arguments refer to an attribute, return TWO_COLUMNS_DEFAULT_SELECTIVITY
        if (IsAttribute(right)) {
            return TWO_COLUMNS_DEFAULT_SELECTIVITY;
        }

        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        else if (IsConstantExprWithParams(right.Ptr())) {
            TString attributeName = attribute.GetRef();
            if (!IsConstantExpr(right.Ptr())) {
                return DefaultInequalitySelectivity(Stats, attributeName);
            }

            if (!Stats || !Stats->ColumnStatistics) {
                return DefaultInequalitySelectivity(Stats, attributeName);
            }

            if (const auto eqWidthHistogram = Stats->ColumnStatistics->Data[attributeName].EqWidthHistogramEstimator) {
                const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
                TMaybe<ui64> estimation = EstimateInequalityPredicateByHistogram(right, columnType, eqWidthHistogram, predicate);
                if (!estimation.Defined() || !Stats->Nrows) {
                    return DefaultInequalitySelectivity(Stats, attributeName);
                }
                return estimation.GetRef() / std::max(Stats->Nrows, 1e-4);
            }
            return DefaultInequalitySelectivity(Stats, attributeName);
        }
    }

    return 1.0;
}

double TPredicateSelectivityComputer::ComputeEqualitySelectivity(
    const TExprBase& left,
    const TExprBase& right,
    bool collectMembers
) {
    Y_UNUSED(collectMembers); // members are collected while the predicate tree is built

    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeEqualitySelectivity(right, left, collectMembers);
    }

    if (auto attribute = IsAttribute(left)) {
        // In case both arguments refer to an attribute, return TWO_COLUMNS_DEFAULT_SELECTIVITY
        if (IsAttribute(right)) {
            return TWO_COLUMNS_DEFAULT_SELECTIVITY;
        }

        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        else if (IsConstantExprWithParams(right.Ptr())) {
            TString attributeName = attribute.GetRef();
            if (!IsConstantExpr(right.Ptr())) {
                return DefaultEqualitySelectivity(Stats, attributeName);
            }

            if (!Stats || !Stats->ColumnStatistics) {
                return DefaultEqualitySelectivity(Stats, attributeName);
            }

            if (const auto countMinSketch = Stats->ColumnStatistics->Data[attributeName].CountMinSketch) {
                const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
                TMaybe<ui32> estimation = EstimateEqualityPredicateBySketch(right, columnType, countMinSketch);
                if (!estimation.Defined() || !Stats->Nrows) {
                    return DefaultEqualitySelectivity(Stats, attributeName);
                }
                return estimation.GetRef() / std::max(Stats->Nrows, 1e-4);
            }
            return DefaultEqualitySelectivity(Stats, attributeName);
        }
    }

    return 1.0;
}

double TPredicateSelectivityComputer::ComputeComparisonSelectivity(
    const TExprBase& left,
    const TExprBase& right,
    bool containString
) {
    // column is on the left
    if (IsAttribute(left)) {
        // both sides refer to an attribute
        if (IsAttribute(right)) {
            return TWO_COLUMNS_DEFAULT_SELECTIVITY;
        }
        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        // Currently, we just return 0.5 for LIKE str% and 0.1 for LIKE %str% string predicates
        else if (IsConstantExprWithParams(right.Ptr())) {
            if (containString && IsConstantExpr(right.Ptr())) { 
                return 0.1;
            }
            return 0.5;
        }
    }

    return 1.0;
}

double TPredicateSelectivityComputer::ReComputeEstimation(const TString& attributeName, const TPredicateRange& mergedRange) {
    // unbounded range
    if (!mergedRange.LeftBound.Defined() && !mergedRange.RightBound.Defined()) {
        return 1.0;
    }

    if (!Stats || !Stats->ColumnStatistics) {
        return DefaultEqualitySelectivity(Stats, attributeName);
    }

    if (!Stats->Nrows) {
        return DefaultEqualitySelectivity(Stats, attributeName); 
    }

    // point predicate logic
    if (mergedRange.LeftBound.Defined() && mergedRange.RightBound.Defined()
            && mergedRange.LeftBound.GetRef() == mergedRange.RightBound.GetRef()) {

        if (!mergedRange.LeftInclusive || !mergedRange.RightInclusive) {
            return 0.0;
        }

        if (!mergedRange.Left.Defined() || !mergedRange.Right.Defined()) {
            return DefaultEqualitySelectivity(Stats, attributeName);
        }

        if (const auto countMinSketch = Stats->ColumnStatistics->Data[attributeName].CountMinSketch) {
            const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
            TMaybe<ui32> estimation = EstimateEqualityPredicateBySketch(
                mergedRange.Left.GetRef(),
                columnType,
                countMinSketch
            );

            if (!estimation.Defined()) {
                return DefaultEqualitySelectivity(Stats, attributeName);
            }
            return (double) estimation.GetRef() / std::max(Stats->Nrows, 1e-4);
        }

        return DefaultEqualitySelectivity(Stats, attributeName);
    }

    // range predicate logic
    if (mergedRange.LeftBound.Defined() && mergedRange.RightBound.Defined()) {

        if (!mergedRange.Left.Defined() || !mergedRange.Right.Defined()) {
            return DefaultInequalitySelectivity(Stats, attributeName);
        }

        EInequalityPredicateType leftInequalitySign = EInequalityPredicateType::Greater;
        if (mergedRange.LeftInclusive) {
            leftInequalitySign = EInequalityPredicateType::GreaterOrEqual;
        }

        EInequalityPredicateType rightInequalitySign = EInequalityPredicateType::Less;
        if (mergedRange.RightInclusive) {
            rightInequalitySign = EInequalityPredicateType::LessOrEqual;
        }

        if (const auto eqWidthHistogram = Stats->ColumnStatistics->Data[attributeName].EqWidthHistogramEstimator) {
            const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
            TMaybe<ui64> estimation = EstimateRangePredicateByHistogram(
                mergedRange.Left.GetRef(),
                mergedRange.Right.GetRef(),
                columnType,
                eqWidthHistogram,
                leftInequalitySign,
                rightInequalitySign
            );

            if (!estimation.Defined()) {
                return DefaultInequalitySelectivity(Stats, attributeName);
            }
            return (double) estimation.GetRef() / std::max(Stats->Nrows, 1e-4);
        }

        return DefaultInequalitySelectivity(Stats, attributeName);
    }

    // inequality predicate logic
    if (!mergedRange.Left.Defined() && !mergedRange.Right.Defined()) {
        return DefaultInequalitySelectivity(Stats, attributeName);
    }

    TMaybe<TExprBase> nodePtr;
    EInequalityPredicateType inequalitySign;
    if (mergedRange.Left.Defined()) {
        nodePtr = mergedRange.Left.GetRef();
        inequalitySign = EInequalityPredicateType::Greater;
        if (mergedRange.LeftInclusive) {
            inequalitySign = EInequalityPredicateType::GreaterOrEqual;
        }
    } else if (mergedRange.Right.Defined()) {
        nodePtr = mergedRange.Right.GetRef();
        inequalitySign = EInequalityPredicateType::Less;
        if (mergedRange.RightInclusive) {
            inequalitySign = EInequalityPredicateType::LessOrEqual;
        }
    }

    if (const auto eqWidthHistogram = Stats->ColumnStatistics->Data[attributeName].EqWidthHistogramEstimator) {
        const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
        TMaybe<ui64> estimation = EstimateInequalityPredicateByHistogram(
            nodePtr.GetRef(),
            columnType,
            eqWidthHistogram,
            inequalitySign
        );

        if (!estimation.Defined()) {
            return DefaultInequalitySelectivity(Stats, attributeName);
        }
        return (double) estimation.GetRef() / std::max(Stats->Nrows, 1e-4);
    }

    return DefaultInequalitySelectivity(Stats, attributeName);
}

double TPredicateSelectivityComputer::ComputeLeafSelectivity(const TTreeNode& leaf) {
    switch (leaf.Kind) {
        case EPredicateKind::Comparison:
            if (leaf.Sign == EInequalityPredicateType::Equal) {
                return ComputeEqualitySelectivity(leaf.Left.GetRef(), leaf.Right.GetRef(), false);
            }
            return ComputeInequalitySelectivity(leaf.Left.GetRef(), leaf.Right.GetRef(), false, leaf.Sign);

        case EPredicateKind::MergedRanges: {
            // a range unbounded on both sides covers the domain and estimates to 1.0
            double selectivity = 0.0;
            for (const auto& range : leaf.Ranges) {
                selectivity += ReComputeEstimation(leaf.Column, range);
            }
            return std::min(1.0, selectivity);
        }

        case EPredicateKind::StringMatch: {
            const double selectivity = ComputeComparisonSelectivity(leaf.Left.GetRef(), leaf.Right.GetRef(), leaf.ContainString);
            return leaf.UnderNot ? 1.0 - selectivity : selectivity;
        }

        case EPredicateKind::TwoColumns:
            return TWO_COLUMNS_DEFAULT_SELECTIVITY;
        case EPredicateKind::Regex:
            return leaf.UnderNot ? 1.0 - 0.5 : 0.5;
        case EPredicateKind::BoolLiteral:
            return leaf.BoolValue ? 1.0 : 0.0;
        case EPredicateKind::NonAttribute: // EmptyAttr
        case EPredicateKind::NonConstant: // ConstantExprWithParams
        case EPredicateKind::Unknown:
            return 1.0;
        case EPredicateKind::Parameter: // EmptyConstantExpr
            return DefaultEqualitySelectivity(Stats, leaf.Attribute);
    }
    return 1.0;
}

double TPredicateSelectivityComputer::ComputeSelectivity(const std::shared_ptr<TTreeNode>& node) {
    if (!node) { // it is empty tree, i.e. no selection predicates
        return 1.0;
    }

    if (node->Operator == ELogicalOperator::Leaf) {
        return ComputeLeafSelectivity(*node);
    }

    if (node->Operator == ELogicalOperator::And) {
        double resSelectivity = 1.0;
        for (const auto& child : node->Children) {
            resSelectivity *= ComputeSelectivity(child);
        }
        // cap at 1.0 to stop error propagation
        return std::min(1.0, resSelectivity);
    }

    // processing Or nodes
    double resSelectivity = 0.0;
    for (const auto& child : node->Children) {
        resSelectivity += ComputeSelectivity(child);
    }
    // cap at 1.0 to stop error propagation
    return std::min(1.0, resSelectivity);
}

double TPredicateSelectivityComputer::Compute(const NNodes::TExprBase& input) {
    std::shared_ptr<TTreeNode> rootNode = Builder.Build(input);
    if (!rootNode) {
        return 1.0;
    }
    YQL_CLOG(TRACE, CoreDq) << "\nPredicate tree:\n" << rootNode->ToString();

    TSet<TString> tableAliases;
    CollectTableAliases(rootNode, tableAliases);

    if (tableAliases.size() == 0) {
        YQL_CLOG(TRACE, CoreDq) << "No table aliases in computing selectivity.";
    } else if (tableAliases.size() != 1) {
        YQL_CLOG(TRACE, CoreDq) << "Multiple table aliases in computing selectivity.";
    }

    const std::shared_ptr<TTreeNode> normalizedNode = TPredicateTreeBuilder::Normalize(rootNode);
    YQL_CLOG(TRACE, CoreDq) << "\nNormalized tree:\n" << normalizedNode->ToString();

    return std::min(1.0, ComputeSelectivity(normalizedNode));
}

} // namespace NKikimr::NKqp
