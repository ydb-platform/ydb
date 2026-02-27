#include "dq_opt_stat.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;

namespace {

    using namespace NYql::NDq;

    THashSet<TString> PgComparisonSigns = {
        "<", "<=", ">", ">=", "=", "<>", "!="};

    THashMap<TString, EInequalityPredicateType> StringToComparisonOperatorMap{
        {"<", EInequalityPredicateType::Less},
        {"<=", EInequalityPredicateType::LessOrEqual},
        {">", EInequalityPredicateType::Greater},
        {">=", EInequalityPredicateType::GreaterOrEqual},
        {"=", EInequalityPredicateType::Equal},
        {"<>", EInequalityPredicateType::NotEqual},
        {"!=", EInequalityPredicateType::NotEqual}};

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

    double DefaultEqualitySelectivity(const std::shared_ptr<TOptimizerStatistics>& stats, const TString& attributeName) {
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

    double DefaultInequalitySelectivity(const std::shared_ptr<TOptimizerStatistics>& stats, const TString& attributeName) {
        Y_UNUSED(attributeName);

        if (stats == nullptr) {
            return 1.0;
        }

        return 0.4;
    }

    // Estimates number of rows based on histogram and predicate type.
    template <typename T>
    TMaybe<ui64> EstimatePredicateByType(const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& estimator, T val,
                                                          EInequalityPredicateType predicate) {
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

    // Returns a number of rows based on predicate.
    TMaybe<ui64> EstimateInequalityPredicateByHistogram(NYql::NNodes::TExprBase maybeLiteral, const TString& columnType,
                                            const std::shared_ptr<NKikimr::TEqWidthHistogramEstimator>& eqWidthHistogram,
                                            EInequalityPredicateType predicate) {
        const TMaybe<TString> literal = ExtractLiteral(maybeLiteral);
        if (literal.Defined()) {
            const TString value = literal.GetRef();
            if (columnType == "Uint32") {
                ui32 val = FromString<ui32>(value);
                return EstimatePredicateByType<ui32>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Int32") {
                i32 val = FromString<i32>(value);
                return EstimatePredicateByType<ui32>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Uint64") {
                ui64 val = FromString<ui64>(value);
                return EstimatePredicateByType<ui64>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Int64") {
                i64 val = FromString<i64>(value);
                return EstimatePredicateByType<i64>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Double") {
                double val = FromString<double>(value);
                return EstimatePredicateByType<double>(eqWidthHistogram, val, predicate);
            } else if (columnType == "Date") {
                ui16 val = FromString<ui16>(value);
                return EstimatePredicateByType<ui16>(eqWidthHistogram, val, predicate);
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
            } else if (columnType == "Double") {
                double val = FromString<double>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&val), sizeof(val));
            } else if (columnType == "Date") {
                ui16 val = FromString<ui32>(value);
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

double NYql::NDq::TPredicateSelectivityComputer::ComputeInequalitySelectivity(
    const TExprBase& left,
    const TExprBase& right,
    bool collectMembers,
    EInequalityPredicateType predicate
) {
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeInequalitySelectivity(right, left, collectMembers, GetOppositePredicateType(predicate));
    }

    if (auto attribute = IsAttribute(left)) {
        // It seems like this is not possible in current version.
        if (IsAttribute(right)) {
            return 0.3;
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
                if (!estimation.Defined()) {
                    return DefaultInequalitySelectivity(Stats, attributeName);
                }
                // Should we compare the number of rows in histogram against `Nrows` and adjust `value` based on that.
                Y_ASSERT(Stats->Nrows);
                return estimation.GetRef() / Stats->Nrows;
            }
            return DefaultInequalitySelectivity(Stats, attributeName);
        }
    }

    return 1.0;
}

double NYql::NDq::TPredicateSelectivityComputer::ComputeEqualitySelectivity(
    const TExprBase& left,
    const TExprBase& right,
    bool collectMembers
) {
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeEqualitySelectivity(right, left, collectMembers);
    }

    if (auto attribute = IsAttribute(left)) {
        // In case both arguments refer to an attribute, return 0.3
        if (IsAttribute(right)) {
            if (collectMembers) {
                auto maybeMember = IsMember(left);
                auto maybeAnotherMember = IsMember(right);
                if (maybeMember && maybeAnotherMember) {
                    MemberEqualities.emplace_back(maybeMember.GetRef(), maybeAnotherMember.GetRef());
                }
            }
            return 0.3;
        }

        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        // Currently, with the basic statistics we just return 1/nRows
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
                if (!estimation.Defined()) {
                    return DefaultEqualitySelectivity(Stats, attributeName);
                }
                Y_ASSERT(Stats->Nrows);
                return estimation.GetRef() / Stats->Nrows;
            }
            return DefaultEqualitySelectivity(Stats, attributeName);
        }
    }

    return 1.0;
}

double NYql::NDq::TPredicateSelectivityComputer::ComputeComparisonSelectivity(
    const TExprBase& left,
    const TExprBase& right
) {
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeComparisonSelectivity(right, left);
    }

    if (IsAttribute(left)) {
        // In case both arguments refer to an attribute, return 0.3
        if (IsAttribute(right)) {
            return 0.3;
        }
        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        // Currently, with the basic statistics we just return 0.5
        else if (IsConstantExprWithParams(right.Ptr())) {
            return 0.5;
        }
    }

    return 1.0;
}

double TPredicateSelectivityComputer::Compute(const NNodes::TExprBase& input) {
    return ComputeImpl(input, false, CollectConstantMembers || CollectMemberEqualities);
}

/**
 * ComputeImpl - traverses (depth-first post-order) the predicate tree and computes the selectivity of a predicate using available statistics.
 */
double TPredicateSelectivityComputer::ComputeImpl(
    const TExprBase& input,
    bool underNot,
    bool collectMembers
) {
    TMaybe<double> resSelectivity;

    // Process OptionalIf, just return the predicate statistics
    if (auto optIf = input.Maybe<TCoOptionalIf>()) {
        resSelectivity = ComputeImpl(optIf.Cast().Predicate(), underNot, collectMembers);
    }

    // Same with Coalesce
    else if (auto coalesce = input.Maybe<TCoCoalesce>()) {
        resSelectivity = ComputeImpl(coalesce.Cast().Predicate(), underNot, collectMembers);
    }

    else if (
        input.Ptr()->IsCallable("FromPg") ||
        input.Ptr()->IsCallable("Exists") ||
        input.Ptr()->IsCallable("AssumeStrict") ||
        input.Ptr()->IsCallable("Apply") ||
        input.Ptr()->IsCallable("Udf")
    ) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        resSelectivity = ComputeImpl(child, underNot, collectMembers);
    }

    // Process AND, OR and NOT logical operators.
    // In case of AND we multiply the selectivities, since we assume them to be independent
    // In case of OR we sum them up, again assuming independence and disjointness, but make sure its at most 1.0
    // In case of NOT we subtract the argument's selectivity from 1.0

    else if (auto andNode = input.Maybe<TCoAnd>()) {
        double tmpSelectivity = 1.0;
        for (size_t i = 0; i < andNode.Cast().ArgCount(); i++) {
            tmpSelectivity *= ComputeImpl(andNode.Cast().Arg(i), underNot, !underNot && collectMembers);
        }
        resSelectivity = tmpSelectivity;
    } else if (auto orNode = input.Maybe<TCoOr>()) {
        double tmpSelectivity = 0.0;
        for (size_t i = 0; i < orNode.Cast().ArgCount(); i++) {
            tmpSelectivity += ComputeImpl(orNode.Cast().Arg(i), underNot, underNot && collectMembers);
        }
        resSelectivity = tmpSelectivity;
    } else if (auto notNode = input.Maybe<TCoNot>()) {
        double argSel = ComputeImpl(notNode.Cast().Value(), !underNot, collectMembers);
        resSelectivity = 1.0 - (argSel == 1.0 ? 0.95 : argSel);
    }

    // Process equality predicate
    else if (auto equality = input.Maybe<TCoCmpEqual>()) {
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();
        resSelectivity = ComputeEqualitySelectivity(left, right, !underNot && collectMembers);
    }

    // Process not equality predicate
    else if (auto notEquality = input.Maybe<TCoCmpNotEqual>()) {
        auto left = notEquality.Cast().Left();
        auto right = notEquality.Cast().Right();
        double eqSel = ComputeEqualitySelectivity(left, right, underNot && collectMembers);
        resSelectivity = 1.0 - (eqSel == 1.0 ? 0.95 : eqSel);
    }

    // Process all inequality predicates
    else if (auto less = input.Maybe<TCoCmpLess>()) {
        auto left = less.Cast().Left();
        auto right = less.Cast().Right();
        resSelectivity = ComputeInequalitySelectivity(left, right, collectMembers, EInequalityPredicateType::Less);
    }

    else if (auto greater = input.Maybe<TCoCmpGreater>()) {
        auto left = greater.Cast().Left();
        auto right = greater.Cast().Right();
        resSelectivity = ComputeInequalitySelectivity(left, right, collectMembers, EInequalityPredicateType::Greater);
    }

    else if (auto lessOrEqual = input.Maybe<TCoCmpLessOrEqual>()) {
        auto left = lessOrEqual.Cast().Left();
        auto right = lessOrEqual.Cast().Right();
        resSelectivity = ComputeInequalitySelectivity(left, right, collectMembers, EInequalityPredicateType::LessOrEqual);
    }

    else if (auto greaterOrEqual = input.Maybe<TCoCmpGreaterOrEqual>()) {
        auto left = greaterOrEqual.Cast().Left();
        auto right = greaterOrEqual.Cast().Right();
        resSelectivity = ComputeInequalitySelectivity(left, right, collectMembers, EInequalityPredicateType::GreaterOrEqual);
    }

    // Process Pg operators
    else if (input.Ptr()->IsCallable("PgResolvedOp") && PgComparisonSigns.contains(input.Ptr()->ChildPtr(0)->Content())){
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        EInequalityPredicateType compareSign = StringToComparisonOperatorMap[input.Ptr()->ChildPtr(0)->Content()];
        if (compareSign == EInequalityPredicateType::Equal) {
            resSelectivity = ComputeEqualitySelectivity(left, right, !underNot && collectMembers);
        } else if (compareSign == EInequalityPredicateType::NotEqual) {
            double eqSel = ComputeEqualitySelectivity(left, right, underNot && collectMembers);
            resSelectivity = 1.0 - (eqSel == 1.0 ? 0.95 : eqSel);
        } else {
            resSelectivity = ComputeInequalitySelectivity(left, right, collectMembers, compareSign);
        }
    }

    // Process string predicates
    else if(input.Ptr()->IsCallable("Find") || input.Ptr()->IsCallable("StringContains")) {
        auto member =  TExprBase(input.Ptr()->ChildRef(0));
        auto stringPred = TExprBase(input.Ptr()->ChildRef(1));

        if (IsAttribute(member) && IsConstantExpr(stringPred.Ptr())) {
            resSelectivity = 0.1;
        }
    }

    else if (auto comparison = input.Maybe<TCoCompare>()) {
        auto left = comparison.Cast().Left();
        auto right = comparison.Cast().Right();
        resSelectivity = ComputeComparisonSelectivity(left, right);
    }

    // Process regular expression
    else if (input.Maybe<TCoAtom>()) {
        auto atom = input.Cast<TCoAtom>();
        // regexp (all starts with Re2)
        if (atom.StringValue().StartsWith("Re2")) {
            resSelectivity = 0.5;
        }
    }

    // Process SqlIn
    else if (input.Ptr()->IsCallable("SqlIn")) {
        auto list = input.Ptr()->ChildPtr(0);

        double tmpSelectivity = 0.0;
        auto lhs = TExprBase(input.Ptr()->ChildPtr(1));
        for (const auto& child : list->Children()) {
            TExprBase rhs = TExprBase(child);
            tmpSelectivity += ComputeEqualitySelectivity(lhs, rhs, false);
        }
        resSelectivity = tmpSelectivity;
    }

    else if (auto maybeIfExpr = input.Maybe<TCoIf>()) {
        auto ifExpr = maybeIfExpr.Cast();

        // attr in ('a', 'b', 'c' ...)
        if (ifExpr.Predicate().Maybe<TCoExists>() && ifExpr.ThenValue().Maybe<TCoJust>() && ifExpr.ElseValue().Maybe<TCoNothing>()) {
            auto list = FindNode<TExprList>(ifExpr.ThenValue());

            if (list != nullptr) {
                double tmpSelectivity = 0.0;
                TExprBase lhs = ifExpr.Predicate();
                for (const auto& child: list->Children()) {
                    TExprBase rhs = TExprBase(child);
                    tmpSelectivity += ComputeEqualitySelectivity(lhs, rhs, false);
                }
                resSelectivity = tmpSelectivity;
            }
        }
    }

    if (!resSelectivity.Defined()) {
        auto dumped = input.Raw()->Dump();
        YQL_CLOG(TRACE, CoreDq) << "ComputePredicateSelectivity NOT FOUND : " << dumped;
        return 1.0;
    }

    return std::min(1.0, resSelectivity.GetRef());
}
