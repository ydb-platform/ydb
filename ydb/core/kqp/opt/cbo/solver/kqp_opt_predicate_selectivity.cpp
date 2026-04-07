#include "kqp_opt_predicate_selectivity.h"
#include "kqp_opt_stat.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;

namespace {

    using namespace NKikimr::NKqp;

    THashSet<TString> PgComparisonSigns = {
        "<", "<=", ">", ">=", "="};

    const double TWO_COLUMNS_DEFAULT_SELECTIVITY = 0.3;

    THashMap<TString, EInequalityPredicateType> StringToComparisonOperatorMap{
        {"<", EInequalityPredicateType::Less},
        {"<=", EInequalityPredicateType::LessOrEqual},
        {">", EInequalityPredicateType::Greater},
        {">=", EInequalityPredicateType::GreaterOrEqual},
        {"=", EInequalityPredicateType::Equal},
        {"<>", EInequalityPredicateType::NotEqual},
        {"!=", EInequalityPredicateType::NotEqual}};

    THashMap<EInequalityPredicateType, TString> ComparisonOperatorToStringMap{
        {EInequalityPredicateType::Less, "<"},
        {EInequalityPredicateType::LessOrEqual, "<="},
        {EInequalityPredicateType::Greater, ">"},
        {EInequalityPredicateType::GreaterOrEqual, ">="},
        {EInequalityPredicateType::Equal, "="},
        {EInequalityPredicateType::NotEqual, "<>"}};

    /**
     * Check if a callable is an attribute of some table
     * Currently just return a boolean and cover only basic cases
     */
    TMaybe<TString> IsAttribute(const TExprBase& input) {
        if (auto member = input.Maybe<TCoMember>()) {
            return TString(member.Cast().Name());
        } else if (auto cast = input.Maybe<TCoSafeCast>()) {
            return IsAttribute(cast.Cast().Value());
        } else if (auto ifPresent = input.Maybe<TCoIfPresent>()) {
            return IsAttribute(ifPresent.Cast().Optional());
        } else if (auto just = input.Maybe<TCoJust>()) {
            return IsAttribute(just.Cast().Input());
        } else if (input.Ptr()->IsCallable("PgCast")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child);
        } else if (input.Ptr()->IsCallable("FromPg") || input.Ptr()->IsCallable("ToPg")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child);
        } else if (auto exists = input.Maybe<TCoExists>()) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child);
        } else if (auto argument = input.Maybe<TCoArgument>()) {
            TString argumentName = TString(argument.Cast().Name());
            TStringBuf olapApplyMemberPrefix = "members_";
            if (argumentName.StartsWith(olapApplyMemberPrefix)) {
                return argumentName.substr(olapApplyMemberPrefix.length(), argumentName.size() - olapApplyMemberPrefix.length());
            } else {
                return argumentName;
            }
        }
        return Nothing();
    }

    TMaybe<TCoMember> IsMember(const TExprBase& input) {
        if (auto member = input.Maybe<TCoMember>()) {
            return member.Cast();
        } else if (auto cast = input.Maybe<TCoSafeCast>()) {
            return IsMember(cast.Cast().Value());
        } else if (auto ifPresent = input.Maybe<TCoIfPresent>()) {
            return IsMember(ifPresent.Cast().Optional());
        } else if (auto just = input.Maybe<TCoJust>()) {
            return IsMember(just.Cast().Input());
        } else if (input.Ptr()->IsCallable("PgCast")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsMember(child);
        } else if (input.Ptr()->IsCallable("FromPg") || input.Ptr()->IsCallable("ToPg")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsMember(child);
        } else if (auto exists = input.Maybe<TCoExists>()) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsMember(child);
        }
        return Nothing();
    }

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
                return estimator->GetNumElements() - estimator->EstimateEqual<T>(val);
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

    // Returns an opposite predicate.
    EInequalityPredicateType GetOppositePredicateType(EInequalityPredicateType predicate) {
        switch (predicate) {
            case EInequalityPredicateType::Less:
                return EInequalityPredicateType::Greater;
            case EInequalityPredicateType::Greater:
                return EInequalityPredicateType::Less;
            case EInequalityPredicateType::LessOrEqual:
                return EInequalityPredicateType::GreaterOrEqual;
            case EInequalityPredicateType::GreaterOrEqual:
                return EInequalityPredicateType::LessOrEqual;
            case EInequalityPredicateType::Equal:
                return EInequalityPredicateType::NotEqual;
            case EInequalityPredicateType::NotEqual:
                return EInequalityPredicateType::Equal;
            default:
                Y_ABORT();
        }
    }

    // Returns an opposite range direction.
    EInequalityPredicateType GetOppositeRangeDirection(EInequalityPredicateType predicate) {
        switch (predicate) {
            case EInequalityPredicateType::Less:
                return EInequalityPredicateType::GreaterOrEqual;
            case EInequalityPredicateType::Greater:
                return EInequalityPredicateType::LessOrEqual;
            case EInequalityPredicateType::LessOrEqual:
                return EInequalityPredicateType::Greater;
            case EInequalityPredicateType::GreaterOrEqual:
                return EInequalityPredicateType::Less;
            case EInequalityPredicateType::Equal:
                return EInequalityPredicateType::NotEqual;
            case EInequalityPredicateType::NotEqual:
                return EInequalityPredicateType::Equal;
            default:
                Y_ABORT();
        }
    }

    TMaybe<TString> ExtractLiteral(NYql::NNodes::TExprBase maybeLiteral) {
        if (auto maybeJust = maybeLiteral.Maybe<NYql::NNodes::TCoJust>()) {
            maybeLiteral = maybeJust.Cast().Input();
        }
        if (maybeLiteral.Maybe<NYql::NNodes::TCoDataCtor>()) {
            auto literal = maybeLiteral.Maybe<NYql::NNodes::TCoDataCtor>().Cast();
            auto value = literal.Literal().Value();
            return TString(value);
        }
        return Nothing();
    }

    i8 CompareValues(const TString& left, const TString& right, const TString columnType) {
        if (columnType == "Bool") {
            ui8 l = FromString<bool>(left);
            ui8 r = FromString<bool>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Uint8") {
            ui8 l = FromString<ui8>(left);
            ui8 r = FromString<ui8>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Int8") {
            i8 l = FromString<i8>(left);
            i8 r = FromString<i8>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Uint32") {
            ui32 l = FromString<ui32>(left);
            ui32 r = FromString<ui32>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Int32") {
            i32 l = FromString<i32>(left);
            i32 r = FromString<i32>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Uint64") {
            ui64 l = FromString<ui64>(left);
            ui64 r = FromString<ui64>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Int64") {
            i64 l = FromString<i64>(left);
            i64 r = FromString<i64>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Float") {
            float l = FromString<float>(left);
            float r = FromString<float>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Double" || columnType == "Decimal(12,2)") {
            double l = FromString<double>(left);
            double r = FromString<double>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Date" || columnType == "Date32") {
            ui16 l = FromString<ui16>(left);
            ui16 r = FromString<ui16>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Datetime") {
            ui32 l = FromString<ui32>(left);
            ui32 r = FromString<ui32>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Interval" || columnType == "Timestamp64" || columnType == "Interval64") {
            i64 l = FromString<i64>(left);
            i64 r = FromString<i64>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        } else if (columnType == "Timestamp") {
            ui64 l = FromString<ui64>(left);
            ui64 r = FromString<ui64>(right);
            return (l < r) ? -1 : (l > r) ? 1 : 0;
        }

        // other types are considered as string and compared lexicographically
        // "Utf8", "String", Uuid, "Yson", "Json"
        return (left < right) ? -1 : (left > right) ? 1 : 0;
    }

    // Returns a number of rows based on predicate.
    TMaybe<ui64> EstimateInequalityPredicateByHistogram(NYql::NNodes::TExprBase maybeLiteral, const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType predicate) {
        const TMaybe<TString> literal = ExtractLiteral(maybeLiteral);
        if (literal.Defined()) {
            const TString value = literal.GetRef();
            if (columnType == "Uint32") {
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
            } else if (columnType == "Double" || columnType == "Decimal(12,2)") {
                double val = FromString<double>(value);
                return EstimateInequalityPredicateByType<double>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Date" || columnType == "Date32") {
                ui16 val = FromString<ui16>(value);
                return EstimateInequalityPredicateByType<ui16>(eqWidthHistogram, val, predicate);
            }
            // TODO: Add support for other types.
            return Nothing();
        }

        return Nothing();
    }

    TMaybe<ui64> EstimateRangePredicateByHistogram(NYql::NNodes::TExprBase maybeLeftLiteral, NYql::NNodes::TExprBase maybeRightLiteral,
                                            const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType leftPredicate, EInequalityPredicateType rightPredicate) {
        const TMaybe<TString> leftLiteral = ExtractLiteral(maybeLeftLiteral);
        const TMaybe<TString> rightLiteral = ExtractLiteral(maybeRightLiteral);

        if (leftLiteral.Defined() && rightLiteral.Defined()) {
            const TString leftValue = leftLiteral.GetRef();
            const TString rightValue = rightLiteral.GetRef();

            if (columnType == "Uint32") {
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
            } else if (columnType == "Double" || columnType == "Decimal(12,2)") {
                double leftVal = FromString<double>(leftValue);
                double rightVal = FromString<double>(rightValue);
                return EstimateRangePredicateByType<double>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            } else if (columnType == "Date" || columnType == "Date32") {
                ui16 leftVal = FromString<ui16>(leftValue);
                ui16 rightVal = FromString<ui16>(rightValue);
                return EstimateRangePredicateByType<ui16>(eqWidthHistogram, leftVal, rightVal, leftPredicate, rightPredicate);
            }
            // TODO: Add support for other types.
            return Nothing();
        }

        return Nothing();
    }

    TMaybe<ui32> EstimateEqualityPredicateBySketch(NYql::NNodes::TExprBase maybeLiteral, TString columnType,
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
            } else if (columnType == "Double" || columnType == "Decimal(12,2)") {
                double val = FromString<double>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Date" || columnType == "Date32") {
                ui16 val = FromString<ui16>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Datetime") {
                ui32 val = FromString<ui32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Utf8" || columnType == "String" || columnType == "Yson" || columnType == "Json") {
                return countMinSketch->Probe(value.data(), value.size());
            } else if (columnType == "Interval" || columnType == "Timestamp64" || columnType == "Interval64") {
                i64 val = FromString<i64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Timestamp") {
                ui64 val = FromString<ui64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Uuid") {
                const ui64* uuidData = reinterpret_cast<const ui64*>(value.data());
                std::pair<ui64, ui64> val{};
                val.first = uuidData[0];   // low128
                val.second = uuidData[1];  // high128
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            }
            return Nothing();
        }

        return Nothing();
    }
}

template<typename T>
TExprNode::TPtr FindNode(const TExprBase& input) {
    for (const auto& child : input.Ptr()->Children()) {
        if (TExprBase(child).Maybe<T>()) {
            return child;
        }

        auto tmp = FindNode<T>(TExprBase(child));
        if (tmp != nullptr) {
            return tmp;
        }
    }

    return nullptr;
}

namespace NKikimr::NKqp {


TMaybe<TString> TPredicateSelectivityComputer::GetAttributeType(const TString& attributeName) {
    if (Stats && Stats->ColumnStatistics) {
        return Stats->ColumnStatistics->Data[attributeName].Type;
    }
    return Nothing();
}

double TPredicateSelectivityComputer::ComputeInequalitySelectivity(
    const TExprBase& left,
    const TExprBase& right,
    bool collectMembers,
    EInequalityPredicateType predicate
) {
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeInequalitySelectivity(right, left, collectMembers, GetOppositePredicateType(predicate));
    }

    if (auto attribute = IsAttribute(left)) {
        // It seems like this is not possible in current version ?!
        // In case both arguments refer to an attribute, return TWO_COLUMNS_DEFAULT_SELECTIVITY
        if (IsAttribute(right)) {
            return TWO_COLUMNS_DEFAULT_SELECTIVITY;
        } else if (IsConstantExprWithParams(right.Ptr())) {
            const TString attributeName = attribute.GetRef();
            if (!IsConstantExpr(right.Ptr())) {
                return DefaultInequalitySelectivity(Stats, attributeName);
            }

            if (!Stats || !Stats->ColumnStatistics) {
                if (CollectColumnsStatUsedMembers) {
                    if (auto maybeMember = IsMember(left)) {
                        ColumnStatsUsedMembers.AddInequality(maybeMember.GetRef());
                    }
                }
                return DefaultInequalitySelectivity(Stats, attributeName);
            }

            if (const auto eqWidthHistogram = Stats->ColumnStatistics->Data[attributeName].EqWidthHistogramEstimator) {
                const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
                TMaybe<ui64> estimation = EstimateInequalityPredicateByHistogram(right, columnType, eqWidthHistogram, predicate);
                if (!estimation.Defined() || !Stats->Nrows) {
                    return DefaultInequalitySelectivity(Stats, attributeName);
                }
                // Should we compare the number of rows in histogram against `Nrows` and adjust `value` based on that ?!
                return estimation.GetRef() / Stats->Nrows;
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
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeEqualitySelectivity(right, left, collectMembers);
    }

    if (auto attribute = IsAttribute(left)) {
        // In case both arguments refer to an attribute, return TWO_COLUMNS_DEFAULT_SELECTIVITY
        if (IsAttribute(right)) {
            if (collectMembers) {
                auto maybeMember = IsMember(left);
                auto maybeAnotherMember = IsMember(right);
                if (maybeMember && maybeAnotherMember) {
                    MemberEqualities.emplace_back(maybeMember.GetRef(), maybeAnotherMember.GetRef());
                }
            }
            return TWO_COLUMNS_DEFAULT_SELECTIVITY;
        }

        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        else if (IsConstantExprWithParams(right.Ptr())) {
            const TString attributeName = attribute.GetRef();
            if (!IsConstantExpr(right.Ptr())) {
                return DefaultEqualitySelectivity(Stats, attributeName);
            }

            if (collectMembers) {
                auto maybeMember = IsMember(left);
                if (maybeMember) {
                    ConstantMembers.push_back(maybeMember.GetRef());
                }
            }

            if (!Stats || !Stats->ColumnStatistics) {
                if (CollectColumnsStatUsedMembers) {
                    if (auto maybeMember = IsMember(left)) {
                        ColumnStatsUsedMembers.AddEquality(maybeMember.GetRef());
                    }
                }
                return DefaultEqualitySelectivity(Stats, attributeName);
            }

            if (const auto countMinSketch = Stats->ColumnStatistics->Data[attributeName].CountMinSketch) {
                const auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
                TMaybe<ui32> estimation = EstimateEqualityPredicateBySketch(right, columnType, countMinSketch);
                if (!estimation.Defined() || !Stats->Nrows) {
                    return DefaultEqualitySelectivity(Stats, attributeName);
                }
                return estimation.GetRef() / Stats->Nrows;
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
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeComparisonSelectivity(right, left, containString);
    }

    if (IsAttribute(left)) {
        // In case both arguments refer to an attribute, return TWO_COLUMNS_DEFAULT_SELECTIVITY
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

std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::CreateLeafNode(TMaybe<TString> attribute) {
    auto node = std::make_shared<TTreeNode>();
    node->Operator = ELogicalOperator::Leaf;

    if (!attribute.Defined()) {
        return node;
    }

    TString ref = attribute.GetRef();
    size_t dotPos = ref.find('.');
    if (dotPos != TString::npos) {
        node->TableAlias = ref.substr(0, dotPos);
        node->Column = ref.substr(dotPos + 1);
    } else {
        node->Column = ref;
    }

    TMaybe<TString> colType = GetAttributeType(ref);
    if (colType.Defined()) {
        node->ColumnType = colType.GetRef();
    }

    return node;
}

std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::ProcessStringPredicate(
    const TExprBase& left,
    const TExprBase& right,
    bool underNot,
    bool collectMembers,
    bool containString
) {
    Y_UNUSED(collectMembers);

    // left is column and right is constant expression
    auto leftAttr = IsAttribute(left);
    auto rightAttr = IsAttribute(right);

    double resSelectivity = ComputeComparisonSelectivity(left, right, containString);
    resSelectivity = underNot ? 1.0 - resSelectivity : resSelectivity;

    std::shared_ptr<TTreeNode> node = CreateLeafNode(leftAttr);
    node->Selectivity = resSelectivity;


    return node;
}

std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::ProcessRegexPredicte(
    bool underNot,
    bool collectMembers
) {
    Y_UNUSED(collectMembers);

    // NOTE: TCoAtom is not a Callable and consider NOT
    double resSelectivity = underNot ? 1.0 - 0.5 : 0.5;

    // TODO: temporal column naming
    std::shared_ptr<TTreeNode> node = CreateLeafNode("Re2");
    node->Selectivity = resSelectivity;

    return node;
}

std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::ConvertInequalityToRange(
    const TExprBase& left,
    const TExprBase& right,
    bool underNot,
    bool collectMembers,
    EInequalityPredicateType inequalitySign
) {
    auto leftAttr = IsAttribute(left);
    auto rightAttr = IsAttribute(right);

    // the case when both are attributes
    if (leftAttr.Defined() && rightAttr.Defined()) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode(leftAttr);
        node->Selectivity = TWO_COLUMNS_DEFAULT_SELECTIVITY;
        return node;
    } else if (!leftAttr.Defined() && !rightAttr.Defined()) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode("EmptyAttr");
        node->Selectivity = 1.0;
        return node;
    }

    auto leftConstParam = IsConstantExprWithParams(left.Ptr());
    auto rightConstParam = IsConstantExprWithParams(right.Ptr());

    // the case when both are literals
    if ((leftConstParam && rightConstParam) || (!leftConstParam && !rightConstParam)) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode("ConstantExprWithParams");
        node->Selectivity = 1.0;
        return node;
    }

    auto leftConst = IsConstantExpr(left.Ptr());
    auto rightConst = IsConstantExpr(right.Ptr());

    // the case when both are literals
    if ((leftConst && rightConst) || (!leftConst && !rightConst)) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode("EmptyConstantExpr");
        if (leftAttr.Defined()) {
            node->Selectivity = DefaultEqualitySelectivity(Stats, leftAttr.GetRef());
        } else {
            node->Selectivity = DefaultEqualitySelectivity(Stats, rightAttr.GetRef());
        }
        return node;
    }

    const TExprBase* leftPtr = &left;
    const TExprBase* rightPtr = &right;

    if (rightAttr) {
        std::swap(leftPtr, rightPtr);
        std::swap(leftAttr, rightAttr);
        inequalitySign = GetOppositePredicateType(inequalitySign);
    }

    if (underNot) {
        inequalitySign = GetOppositeRangeDirection(inequalitySign);
    }

    bool inclusive = false;
    if (inequalitySign == EInequalityPredicateType::LessOrEqual || inequalitySign == EInequalityPredicateType::GreaterOrEqual) {
        inclusive = true;
    }

    std::shared_ptr<TTreeNode> node = CreateLeafNode(leftAttr);
    node->Selectivity = ComputeInequalitySelectivity(*leftPtr, *rightPtr, collectMembers, inequalitySign);

    TMaybe<TString> literal = ExtractLiteral(*rightPtr);
    if (literal.Defined()) {
        if (inequalitySign == EInequalityPredicateType::Less || inequalitySign == EInequalityPredicateType::LessOrEqual) {
            TPredicateRange rangeNode(Nothing(), Nothing(), false, *rightPtr, literal.GetRef(), inclusive);
            node->Range = std::move(rangeNode);
        } else if (inequalitySign == EInequalityPredicateType::Greater || inequalitySign == EInequalityPredicateType::GreaterOrEqual) {
            TPredicateRange rangeNode(*rightPtr, literal.GetRef(), inclusive, Nothing(), Nothing(), false);
            node->Range = std::move(rangeNode);
        }
    }


    return node;
}

std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::ConvertEqualityToRange(
    const TExprBase& left,
    const TExprBase& right,
    bool underNot,
    bool collectMembers
) {
    auto leftAttr = IsAttribute(left);
    auto rightAttr = IsAttribute(right);

    // the case when both are attributes
    if (leftAttr.Defined() && rightAttr.Defined()) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode(leftAttr);
        node->Selectivity = TWO_COLUMNS_DEFAULT_SELECTIVITY;
        return node;
    } else if (!leftAttr.Defined() && !rightAttr.Defined()) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode("EmptyAttr");
        node->Selectivity = 1.0;
        return node;
    }

    auto leftConstParam = IsConstantExprWithParams(left.Ptr());
    auto rightConstParam = IsConstantExprWithParams(right.Ptr());

    // the case when both are literals
    if ((leftConstParam && rightConstParam) || (!leftConstParam && !rightConstParam)) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode("ConstantExprWithParams");
        node->Selectivity = 1.0;
        return node;
    }

    auto leftConst = IsConstantExpr(left.Ptr());
    auto rightConst = IsConstantExpr(right.Ptr());

    // the case when both are literals
    if ((leftConst && rightConst) || (!leftConst && !rightConst)) {
        std::shared_ptr<TTreeNode> node = CreateLeafNode("EmptyConstantExpr");
        if (leftAttr.Defined()) {
            node->Selectivity = DefaultEqualitySelectivity(Stats, leftAttr.GetRef());
        } else {
            node->Selectivity = DefaultEqualitySelectivity(Stats, rightAttr.GetRef());
        }
        return node;
    }

    const TExprBase* leftPtr = &left;
    const TExprBase* rightPtr = &right;

    if (rightAttr) {
        std::swap(leftPtr, rightPtr);
        std::swap(leftAttr, rightAttr);
    }

    if (underNot) {
        // converting not equality into two ranges (col < x) OR (col > x)
        auto node = std::make_shared<TTreeNode>();
        node->Operator = ELogicalOperator::Or;

        std::shared_ptr<TTreeNode> left = ConvertInequalityToRange(*leftPtr, *rightPtr, false, collectMembers, EInequalityPredicateType::Less);
        std::shared_ptr<TTreeNode> right = ConvertInequalityToRange(*leftPtr, *rightPtr, false, collectMembers, EInequalityPredicateType::Greater);

        if (left) {
            node->Children.push_back(left);
        }
        if (right) {
            node->Children.push_back(right);
        }

        return node;
    }

    std::shared_ptr<TTreeNode> node = CreateLeafNode(leftAttr);
    node->Selectivity = ComputeEqualitySelectivity(*leftPtr, *rightPtr, collectMembers);

    TMaybe<TString> literal = ExtractLiteral(*rightPtr);
    if (literal.Defined()) {
        TPredicateRange rangeNode(*rightPtr, literal.GetRef(), true, *rightPtr, literal.GetRef(), true);
        node->Range = std::move(rangeNode);
    }

    return node;
}

std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::ProcessSetPredicate(
    const TExprBase& left,
    const TExprNode::TPtr list,
    bool underNot,
    bool collectMembers
) {
    auto node = std::make_shared<TTreeNode>();

    // convert set predicate into a subtree
    auto logicalOperator = underNot ? ELogicalOperator::And : ELogicalOperator::Or;
    node->Operator = logicalOperator;

    for (const auto& element : list->Children()) {
        TExprBase right = TExprBase(element);
        auto child = ConvertEqualityToRange(left, right, underNot, collectMembers);
        if (child) {
            node->Children.push_back(child);
        }
    }

    return node;
}

double TPredicateSelectivityComputer::ReComputeEstimation(TString attributeName, TPredicateRange& mergedRange) {
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
            return (double) estimation.GetRef() / Stats->Nrows;
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
            return (double) estimation.GetRef() / Stats->Nrows;
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
        mergedRange.Left.GetRef();
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
        return (double) estimation.GetRef() / Stats->Nrows;
    }

    return DefaultInequalitySelectivity(Stats, attributeName);
}

void SortRanges(TVector<std::shared_ptr<TTreeNode>>& allRanges, TString columnType) {
    // sorting by left bound only
    std::sort(allRanges.begin(), allRanges.end(), [columnType](const std::shared_ptr<TTreeNode>& left, const std::shared_ptr<TTreeNode>& right) {

        // -inf bounds first
        if (!left->Range.LeftBound.Defined() && !right->Range.LeftBound.Defined()) {
            return false;
        }
        if (!left->Range.LeftBound.Defined()) {
            return true;
        }
        if (!right->Range.LeftBound.Defined()) {
            return false;
        }

        i8 cmp = CompareValues(left->Range.LeftBound.GetRef(), right->Range.LeftBound.GetRef(), columnType);
        if (cmp != 0) {
            return cmp < 0;
        }

        // inclusive starts earlier
        if (left->Range.LeftInclusive != right->Range.LeftInclusive) {
            return left->Range.LeftInclusive;
        }
        return false;
    });
}

TMaybe<TPredicateRange> IntersectOverlappingConjunctions(TVector<std::shared_ptr<TTreeNode>>& allRanges, TString columnType) {
    if (allRanges.empty()) {
        return Nothing();
    }

    TPredicateRange mergedRange = allRanges.front()->Range;

    for (size_t i = 1; i < allRanges.size(); ++i) {
        auto& range = allRanges[i]->Range;

        // intersect left bound
        if (range.LeftBound.Defined()) {
            if (!mergedRange.LeftBound.Defined()) {
                mergedRange.Left = range.Left;
                mergedRange.LeftBound = range.LeftBound;
                mergedRange.LeftInclusive = range.LeftInclusive;
            } else {
                i8 cmp = CompareValues(range.LeftBound.GetRef(), mergedRange.LeftBound.GetRef(), columnType);
                if (cmp > 0 || (cmp == 0 && !range.LeftInclusive)) {
                    mergedRange.Left = range.Left;
                    mergedRange.LeftBound = range.LeftBound;
                    mergedRange.LeftInclusive = range.LeftInclusive;
                }
            }
        }

        // intersect right bound
        if (range.RightBound.Defined()) {
            if (!mergedRange.RightBound.Defined()) {
                mergedRange.Right = range.Right;
                mergedRange.RightBound = range.RightBound;
                mergedRange.RightInclusive = range.RightInclusive;
            } else {
                i8 cmp = CompareValues(range.RightBound.GetRef(), mergedRange.RightBound.GetRef(), columnType);
                if (cmp < 0 || (cmp == 0 && !range.RightInclusive)) {
                    mergedRange.Right = range.Right;
                    mergedRange.RightBound = range.RightBound;
                    mergedRange.RightInclusive = range.RightInclusive;
                }
            }
        }

        // check for empty intersection
        if (mergedRange.LeftBound.Defined() && mergedRange.RightBound.Defined()) {
            i8 cmp = CompareValues(mergedRange.LeftBound.GetRef(), mergedRange.RightBound.GetRef(), columnType);
            if (cmp > 0) {
                return Nothing();
            } else if (cmp == 0 && (!mergedRange.LeftInclusive || !mergedRange.RightInclusive)) {
                return Nothing();
            }
        }
    }

    return mergedRange;
}

TMaybe<TVector<TPredicateRange>> UnionOverlappingDisjunctions(TVector<std::shared_ptr<TTreeNode>>& allRanges, TString columnType) {
    if (allRanges.empty()) {
        return Nothing();
    }

    // sorting ranges by left bound only
    SortRanges(allRanges, columnType);

    TVector<TPredicateRange> mergedRanges;
    TPredicateRange current = allRanges.front()->Range;

    for (size_t i = 1; i < allRanges.size(); ++i) {
        auto& range = allRanges[i]->Range;

        // check whether the intervals do not overlap
        bool disjoint = false;
        if (current.RightBound.Defined() && range.LeftBound.Defined()) {
            i8 cmp = CompareValues(current.RightBound.GetRef(), range.LeftBound.GetRef(), columnType);
            if (cmp < 0) {
                disjoint = true;
            } else if (cmp == 0 && (!current.RightInclusive || !range.LeftInclusive)) {
                disjoint = true;
            }
        }
        if (disjoint) {
            mergedRanges.push_back(current);
            current = range;
            continue;
        }

        // expand upper bound if either side is +inf
        if (!current.RightBound.Defined() || !range.RightBound.Defined()) {
            current.Right = Nothing();
            current.RightBound = Nothing();
            current.RightInclusive = false;
        } else {
            i8 cmp = CompareValues(range.RightBound.GetRef(), current.RightBound.GetRef(), columnType);
            if (cmp > 0 || (cmp == 0 && range.RightInclusive)) {
                current.Right = range.Right;
                current.RightBound = range.RightBound;
                current.RightInclusive = range.RightInclusive;
            }
        }

        // if domain is fully covered, then exit
        if (!current.LeftBound.Defined() && !current.RightBound.Defined()) {
            mergedRanges.clear();
            mergedRanges.push_back(current);
            return mergedRanges;
        }
    }

    mergedRanges.push_back(current);
    return mergedRanges;
}

double TPredicateSelectivityComputer::ComputeSelectivity(const std::shared_ptr<TTreeNode>& node, TSet<TString>& tableAliases) {
    if (!node) { // it is empty tree, i.e. no selection predicates
        return 1.0;
    }

    else if (node->Operator == ELogicalOperator::Leaf) {
        TString tableAlias;
        if (node->TableAlias.Defined()) {
            tableAliases.insert(node->TableAlias.GetRef());
            tableAlias = node->TableAlias.GetRef();
        }
        return node->Selectivity;
    }

    else if (node->Operator == ELogicalOperator::And) {
        double resSelectivity = 1.0;
        TMap<TString, TVector<std::shared_ptr<TTreeNode>>> columnSelectivities;

        for (auto& child : node->Children) {
            TString column = child->Column;
            if (!columnSelectivities.contains(column)) {
                TVector<std::shared_ptr<TTreeNode>> ranges;
                columnSelectivities[column] = ranges;
            }

            // group by column and convert point predicates into ranges
            if (child->Operator == ELogicalOperator::Leaf) {
                columnSelectivities[column].push_back(child);
            } else {
                // next conjunction/disjunction
                resSelectivity *= ComputeSelectivity(child, tableAliases);
            }
        }

        // intersect overlaps and re-compute selectivity
        for (auto& entry : columnSelectivities) {
            if (entry.second.size() == 0) {

            } else if (entry.second.size() == 1) {
                resSelectivity *= ComputeSelectivity(entry.second.front(), tableAliases);
            } else {
                TString columnType = entry.second.front()->ColumnType;
                TMaybe<TPredicateRange> mergedRange = IntersectOverlappingConjunctions(entry.second, columnType);
                if (mergedRange.Defined()) {
                    resSelectivity *= ReComputeEstimation(entry.first, mergedRange.GetRef());
                } else {
                    // in case of no overlaps, zero selectivity
                    return 0.0;
                }
            }
        }

        return resSelectivity;
    }

    else if (node->Operator == ELogicalOperator::Or) {
        double resSelectivity = 0.0;
        TMap<TString, TVector<std::shared_ptr<TTreeNode>>> columnSelectivities;

        for (auto& child : node->Children) {
            TString column = child->Column;
            if (!columnSelectivities.contains(column)) {
                TVector<std::shared_ptr<TTreeNode>> ranges;
                columnSelectivities[column] = ranges;
            }

            // group by column and convert point predicates into ranges
            if (child->Operator == ELogicalOperator::Leaf) {
                columnSelectivities[column].push_back(child);
            } else {
                // next conjunction/disjunction
                resSelectivity += ComputeSelectivity(child, tableAliases);
            }
        }

        // union overlaps and re-compute selectivity
        for (auto& entry : columnSelectivities) {
            if (entry.second.size() == 0) {

            } else if (entry.second.size() == 1) {
                resSelectivity += ComputeSelectivity(entry.second.front(), tableAliases);
            } else {
                TString columnType = entry.second.front()->ColumnType;
                TMaybe<TVector<TPredicateRange>> allMergedRanges = UnionOverlappingDisjunctions(entry.second, columnType);
                if (allMergedRanges.Defined()) {
                    for (auto& mergedRange : allMergedRanges.GetRef()) {
                        // domain full coverage
                        if (!mergedRange.LeftBound.Defined() && !mergedRange.RightBound.Defined()) {
                            return 1.0;
                        }
                        resSelectivity += ReComputeEstimation(entry.first, mergedRange);
                    }
                } else {
                    // in case of no overlaps, zero selectivity
                    return 0.0;
                }
            }
        }

        // cap at 1.0 to stop error propagation
        return resSelectivity = std::min(1.0, resSelectivity);
    }

    return 1.0; // fallback
}

double TPredicateSelectivityComputer::Compute(const NNodes::TExprBase& input) {
    std::shared_ptr<TTreeNode> rootNode = ComputeImpl(input, false, CollectConstantMembers || CollectMemberEqualities);
    if (!rootNode) {
        return 1.0;
    }

    TSet<TString> tableAliases;
    double resSelectivity = ComputeSelectivity(rootNode, tableAliases);
    if (tableAliases.size() != 1) {
        YQL_CLOG(TRACE, CoreDq) << "No or multiple table aliases in computing selectivity.";
    }

    return std::min(1.0, resSelectivity);
}

/**
 * ComputeImpl - traverses (depth-first post-order) the predicate tree and computes the selectivity of a predicate using available statistics.
 */
std::shared_ptr<TTreeNode> TPredicateSelectivityComputer::ComputeImpl(
    const TExprBase& input,
    bool underNot,
    bool collectMembers
) {
    // Process OptionalIf, just return the predicate statistics
    if (auto optIf = input.Maybe<TCoOptionalIf>()) {
        return ComputeImpl(optIf.Cast().Predicate(), underNot, collectMembers);
    }

    // Same with Coalesce
    else if (auto coalesce = input.Maybe<TCoCoalesce>()) {
        return ComputeImpl(coalesce.Cast().Predicate(), underNot, collectMembers);
    }

    else if (
        input.Ptr()->IsCallable("FromPg") ||
        input.Ptr()->IsCallable("Exists") ||
        input.Ptr()->IsCallable("AssumeStrict") ||
        input.Ptr()->IsCallable("Apply") ||
        input.Ptr()->IsCallable("Udf")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return ComputeImpl(child, underNot, collectMembers);
    }

    // Process AND, OR, and NOT logical operators.
    // In case of AND we multiply the selectivities, since we assume them to be independent
    // In case of OR we sum them up, again assuming independence and disjointness, but make sure it is at most 1.0
    // In case of NOT we subtract the argument's selectivity from 1.0

    else if (auto andNode = input.Maybe<TCoAnd>()) {
        auto logicalOperator = underNot ? ELogicalOperator::Or : ELogicalOperator::And;
        auto node = std::make_shared<TTreeNode>();
        node->Operator = logicalOperator;

        for (size_t i = 0; i < andNode.Cast().ArgCount(); i++) {
            // collect all conjunctions of a column
            const auto& child = ComputeImpl(andNode.Cast().Arg(i), underNot, !underNot && collectMembers);
            if (child) {
                node->Children.push_back(child);
            }
        }
        return node;
    }

    else if (auto orNode = input.Maybe<TCoOr>()) {
        auto logicalOperator = underNot ? ELogicalOperator::And : ELogicalOperator::Or;
        auto node = std::make_shared<TTreeNode>();
        node->Operator = logicalOperator;

        for (size_t i = 0; i < orNode.Cast().ArgCount(); i++) {
            // collect all disjunctions of a column
            auto child = ComputeImpl(orNode.Cast().Arg(i), underNot, underNot && collectMembers);
            if (child) {
                node->Children.push_back(child);
            }
        }
        return node;
    }

    else if (auto notNode = input.Maybe<TCoNot>()) {
        return ComputeImpl(notNode.Cast().Value(), !underNot, collectMembers);
    }

    // Process equality predicate
    else if (auto equality = input.Maybe<TCoCmpEqual>()) {
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();
        return ConvertEqualityToRange(left, right, underNot, !underNot && collectMembers);
    }

    // Process not equality predicate
    else if (auto notEquality = input.Maybe<TCoCmpNotEqual>()) {
        auto left = notEquality.Cast().Left();
        auto right = notEquality.Cast().Right();
        return ConvertEqualityToRange(left, right, !underNot, underNot && collectMembers);
    }

    // Process all inequality predicates
    else if (auto less = input.Maybe<TCoCmpLess>()) {
        auto left = less.Cast().Left();
        auto right = less.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::Less);
    }

    else if (auto greater = input.Maybe<TCoCmpGreater>()) {
        auto left = greater.Cast().Left();
        auto right = greater.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::Greater);
    }

    else if (auto lessOrEqual = input.Maybe<TCoCmpLessOrEqual>()) {
        auto left = lessOrEqual.Cast().Left();
        auto right = lessOrEqual.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::LessOrEqual);
    }

    else if (auto greaterOrEqual = input.Maybe<TCoCmpGreaterOrEqual>()) {
        auto left = greaterOrEqual.Cast().Left();
        auto right = greaterOrEqual.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::GreaterOrEqual);
    }

    // Process Pg operators
    else if (input.Ptr()->IsCallable("PgResolvedOp") && PgComparisonSigns.contains(input.Ptr()->ChildPtr(0)->Content())){
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        EInequalityPredicateType compareSign = StringToComparisonOperatorMap[input.Ptr()->ChildPtr(0)->Content()];
        if (compareSign == EInequalityPredicateType::Equal) {
            return ConvertEqualityToRange(left, right, underNot, !underNot && collectMembers);
        } else if (compareSign == EInequalityPredicateType::NotEqual) {
            return ConvertEqualityToRange(left, right, !underNot, underNot && collectMembers);
        } else {
            return ConvertInequalityToRange(left, right, underNot, collectMembers, compareSign);
        }
    }

    // Process string predicates
    else if(input.Ptr()->IsCallable("Find") || input.Ptr()->IsCallable("StringContains")) {
        auto left = TExprBase(input.Ptr()->ChildRef(0));
        auto right = TExprBase(input.Ptr()->ChildRef(1));
        return ProcessStringPredicate(left, right, underNot, collectMembers, true);
    }

    else if (auto comparison = input.Maybe<TCoCompare>()) {
        auto left = comparison.Cast().Left();
        auto right = comparison.Cast().Right();
        return ProcessStringPredicate(left, right, underNot, collectMembers, false);
    }

    // Process regular expression
    else if (input.Maybe<TCoAtom>()) {
        auto atom = input.Cast<TCoAtom>();
        // regexp (all starts with Re2)
        if (atom.StringValue().StartsWith("Re2")) {
            return ProcessRegexPredicte(underNot, collectMembers);
        }
    }

    // Process SqlIn
    else if (input.Ptr()->IsCallable("SqlIn")) {
        auto list = input.Ptr()->ChildPtr(0);
        if (list != nullptr) {
            TExprBase left = TExprBase(input.Ptr()->ChildPtr(1)); // left is always column
            return ProcessSetPredicate(left, list, underNot, false);
        }
    }

    else if (auto maybeIfExpr = input.Maybe<TCoIf>()) {
        auto ifExpr = maybeIfExpr.Cast();
        // attr in ('a', 'b', 'c' ...)
        if (ifExpr.Predicate().Maybe<TCoExists>() && ifExpr.ThenValue().Maybe<TCoJust>() && ifExpr.ElseValue().Maybe<TCoNothing>()) {
            auto list = FindNode<TExprList>(ifExpr.ThenValue());
            if (list != nullptr) {
                TExprBase left = ifExpr.Predicate(); // left is always column
                return ProcessSetPredicate(left, list, underNot, false);
            }
        }
    }

    else {
        auto dumped = input.Raw()->Dump();
        YQL_CLOG(TRACE, CoreDq) << "ComputePredicateSelectivity NOT FOUND : " << dumped;
    }

    return nullptr;
}

} // namespace NKikimr::NKqp
