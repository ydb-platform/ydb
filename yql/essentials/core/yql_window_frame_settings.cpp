#include "yql_window_frame_settings.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/sql_types/window_frames_collector_params.h>
#include <yql/essentials/core/yql_window_frame_settings_pg.h>
#include <yql/essentials/utils/parse_double.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/generic/overloaded.h>

#include <compare>
#include <expected>
#include <utility>

namespace NYql {

namespace {

using NWindow::EDirection;
using NWindow::TCoreWinFramesCollectorParams;
using NWindow::TInputRow;
using NWindow::TInputRowWindowFrame;
using NWindow::TNumberAndDirection;

using namespace NNodes;

struct TUnsortedTag {};

struct TManyColumnsInSort {};

struct TSorted {
    enum class ESortDir {
        Asc,
        Desc,
    };
    const TTypeAnnotationNode* SortedColumnType;
    ESortDir SortDir;
};

using TSortTraitsInfo = std::variant<TUnsortedTag, TManyColumnsInSort, TSorted>;

TExprNode::TPtr GetSettingByName(const TExprNode::TChildrenType& settings, TStringBuf name) {
    for (const auto& setting : settings) {
        const auto settingName = setting->Head().Content();
        if (settingName == name) {
            return setting->TailPtr();
        }
    }
    return nullptr;
}

ESortOrder GetSortOrder(const TSortTraitsInfo& info) {
    return std::visit(TOverloaded{
                          [&](const TUnsortedTag&) {
                              return ESortOrder::Unimportant;
                          },
                          [&](const TManyColumnsInSort&) {
                              return ESortOrder::Unimportant;
                          },
                          [&](const TSorted& sorted) {
                              switch (sorted.SortDir) {
                                  case TSorted::ESortDir::Asc:
                                      return ESortOrder::Asc;
                                  case TSorted::ESortDir::Desc:
                                      return ESortOrder::Desc;
                              };
                          }}, info);
}

bool CheckRowFrameNeverEmpty(const TWindowFrameSettings::TRowFrame& frame) {
    if (!frame.first) {
        return !frame.second.Defined() || *frame.second >= 0;
    } else if (!frame.second.Defined()) {
        return !frame.first.Defined() || *frame.first <= 0;
    } else {
        return *frame.first <= *frame.second && *frame.first <= 0 && *frame.second >= 0;
    }
}

class TParseFrameBoundResult {
public:
    TParseFrameBoundResult(TExprNodeNumberAndDirection boundNode, TMaybe<TNodeTransform> columnCast, TMaybe<TNodeTransform> boundCast, bool isZero, bool isCurrentRow, TMaybe<ui32> procId)
        : BoundNode_(std::move(boundNode))
        , ColumnCast_(std::move(columnCast))
        , BoundCast_(std::move(boundCast))
        , IsZero_(isZero)
        , IsCurrentRow_(isCurrentRow)
        , ProcId_(procId)
    {
    }

    const TExprNodeNumberAndDirection& GetBoundNode() const {
        return BoundNode_;
    }

    const TMaybe<TNodeTransform>& GetColumnCast() const {
        return ColumnCast_;
    }

    const TMaybe<TNodeTransform>& GetBoundCast() const {
        return BoundCast_;
    }

    bool IsZero() const {
        return IsZero_;
    }

    bool IsCurrentRow() const {
        return IsCurrentRow_;
    }

    const TMaybe<ui32>& GetProcId() const {
        return ProcId_;
    }

private:
    TExprNodeNumberAndDirection BoundNode_;
    TMaybe<TNodeTransform> ColumnCast_;
    TMaybe<TNodeTransform> BoundCast_;
    bool IsZero_;
    bool IsCurrentRow_;
    TMaybe<ui32> ProcId_;
};

template <typename T>
auto FromStringAtom(TStringBuf buf) {
    if constexpr (std::is_same_v<std::decay_t<T>, float>) {
        return FloatFromString(buf);
    } else if constexpr (std::is_same_v<std::decay_t<T>, double>) {
        return DoubleFromString(buf);
    } else {
        return FromString<T>(buf);
    }
}

// Frame is never empty if left <= 0 <= right
// left <= 0: left is Preceding or zero
// right >= 0: right is Following or zero
bool CheckRangeFrameNeverEmpty(const TParseFrameBoundResult& left, const TParseFrameBoundResult& right) {
    bool leftLeZero = left.GetBoundNode().GetDirection() == EDirection::Preceding || left.IsZero();
    bool rightGeZero = right.GetBoundNode().GetDirection() == EDirection::Following || right.IsZero();
    return leftLeZero && rightGeZero;
}

template <typename T, typename U>
bool CmpGreaterNonInf(TExprNodeNumberAndDirection left, TExprNodeNumberAndDirection right) {
    auto l = TNumberAndDirection<T>(FromStringAtom<T>(left.GetUnderlyingValue()->Head().Content()), left.GetDirection());
    auto r = TNumberAndDirection<U>(FromStringAtom<U>(right.GetUnderlyingValue()->Head().Content()), right.GetDirection());

    return l > r;
}

bool CmpGreaterNonInfDecimal(TExprNodeNumberAndDirection left, TExprNodeNumberAndDirection right) {
    auto parseDecimalFromNode = [](const TExprNode::TPtr& node) -> NDecimal::TInt128 {
        YQL_ENSURE(node->ChildrenSize() == 3, "Expected 3 children for Decimal callable");
        ui8 precision = FromString<ui8>(node->Child(1)->Content());
        ui8 scale = FromString<ui8>(node->Child(2)->Content());
        return NDecimal::FromString(node->Head().Content(), precision, scale);
    };
    auto l = TNumberAndDirection<NDecimal::TInt128>(parseDecimalFromNode(left.GetUnderlyingValue()), left.GetDirection());
    auto r = TNumberAndDirection<NDecimal::TInt128>(parseDecimalFromNode(right.GetUnderlyingValue()), right.GetDirection());
    return l > r;
}

template <typename T>
bool CompareWithRightCallable(TExprNodeNumberAndDirection left,
                              TExprNodeNumberAndDirection right) {
    const auto rightCallableName = right.GetUnderlyingValue()->Content();

    if (rightCallableName == "Int8") {
        return CmpGreaterNonInf<T, i8>(left, right);
    } else if (rightCallableName == "Uint8") {
        return CmpGreaterNonInf<T, ui8>(left, right);
    } else if (rightCallableName == "Int16") {
        return CmpGreaterNonInf<T, i16>(left, right);
    } else if (rightCallableName == "Uint16") {
        return CmpGreaterNonInf<T, ui16>(left, right);
    } else if (rightCallableName == "Int32") {
        return CmpGreaterNonInf<T, i32>(left, right);
    } else if (rightCallableName == "Uint32") {
        return CmpGreaterNonInf<T, ui32>(left, right);
    } else if (rightCallableName == "Int64") {
        return CmpGreaterNonInf<T, i64>(left, right);
    } else if (rightCallableName == "Uint64") {
        return CmpGreaterNonInf<T, ui64>(left, right);
    } else if (rightCallableName == "Float") {
        return CmpGreaterNonInf<T, float>(left, right);
    } else if (rightCallableName == "Double") {
        return CmpGreaterNonInf<T, double>(left, right);
    } else if (rightCallableName == "Interval") {
        using TIntervalLayout = NUdf::TDataType<NUdf::TInterval>::TLayout;
        return CmpGreaterNonInf<T, TIntervalLayout>(left, right);
    } else if (rightCallableName == "Interval64") {
        using TInterval64Layout = NUdf::TDataType<NUdf::TInterval64>::TLayout;
        return CmpGreaterNonInf<T, TInterval64Layout>(left, right);
    }

    YQL_ENSURE(false, "Unexpected callable in comparasion.");
}

// Compare two callables: returns true if left > right.
bool CompareCallablesNonInf(TExprNodeNumberAndDirection leftCallable, TExprNodeNumberAndDirection rightCallable) {
    const auto leftCallableName = leftCallable.GetUnderlyingValue()->Content();

    if (leftCallableName == "Int8") {
        return CompareWithRightCallable<i8>(leftCallable, rightCallable);
    } else if (leftCallableName == "Uint8") {
        return CompareWithRightCallable<ui8>(leftCallable, rightCallable);
    } else if (leftCallableName == "Int16") {
        return CompareWithRightCallable<i16>(leftCallable, rightCallable);
    } else if (leftCallableName == "Uint16") {
        return CompareWithRightCallable<ui16>(leftCallable, rightCallable);
    } else if (leftCallableName == "Int32") {
        return CompareWithRightCallable<i32>(leftCallable, rightCallable);
    } else if (leftCallableName == "Uint32") {
        return CompareWithRightCallable<ui32>(leftCallable, rightCallable);
    } else if (leftCallableName == "Int64") {
        return CompareWithRightCallable<i64>(leftCallable, rightCallable);
    } else if (leftCallableName == "Uint64") {
        return CompareWithRightCallable<ui64>(leftCallable, rightCallable);
    } else if (leftCallableName == "Float") {
        return CompareWithRightCallable<float>(leftCallable, rightCallable);
    } else if (leftCallableName == "Double") {
        return CompareWithRightCallable<double>(leftCallable, rightCallable);
    } else if (leftCallableName == "Interval") {
        return CompareWithRightCallable<NUdf::TDataType<NUdf::TInterval>::TLayout>(leftCallable, rightCallable);
    } else if (leftCallableName == "Interval64") {
        return CompareWithRightCallable<NUdf::TDataType<NUdf::TInterval64>::TLayout>(leftCallable, rightCallable);
    } else if (leftCallableName == "Decimal") {
        return CmpGreaterNonInfDecimal(leftCallable, rightCallable);
    }
    YQL_ENSURE(false, "Unexpected callable in comparasion.");
}

bool CheckRangeFrameIsAlwaysEmpty(const TParseFrameBoundResult& left, const TParseFrameBoundResult& right) {
    if (left.GetBoundNode().IsInf() || right.GetBoundNode().IsInf() || left.IsZero() || right.IsZero()) {
        return false;
    }

    return CompareCallablesNonInf(left.GetBoundNode(), right.GetBoundNode());
}

std::expected<bool, TString> CheckRangeFrameIsAlwaysEmptyPg(const TParseFrameBoundResult& left, const TParseFrameBoundResult& right) {
    // If either bound is infinity or zero, frame cannot be always empty.
    if (left.IsZero() || right.IsZero() || left.GetBoundNode().IsInf() || right.GetBoundNode().IsInf()) {
        return false;
    }

    bool isAlwaysNonEmptyByDirection = left.GetBoundNode().GetDirection() == EDirection::Preceding && right.GetBoundNode().GetDirection() == EDirection::Following;
    if (isAlwaysNonEmptyByDirection) {
        return false;
    }
    bool isAlwaysEmptyByDirection = left.GetBoundNode().GetDirection() == EDirection::Following && right.GetBoundNode().GetDirection() == EDirection::Preceding;

    if (isAlwaysEmptyByDirection) {
        return true;
    }

    YQL_ENSURE(left.GetBoundNode().GetDirection() == right.GetBoundNode().GetDirection(), "Expected same direction for left and right bounds");
    if (left.GetBoundNode().GetDirection() == EDirection::Preceding) {
        auto result = PgCompareWithCasts(left.GetBoundNode().GetUnderlyingValue(),
                                         right.GetBoundNode().GetUnderlyingValue(),
                                         NKikimr::NMiniKQL::EPgCompareType::Less);
        if (!result.has_value()) {
            return std::unexpected(result.error());
        }
        return result.value();
    } else {
        auto result = PgCompareWithCasts(left.GetBoundNode().GetUnderlyingValue(),
                                         right.GetBoundNode().GetUnderlyingValue(),
                                         NKikimr::NMiniKQL::EPgCompareType::Greater);
        if (!result.has_value()) {
            return std::unexpected(result.error());
        }
        return result.value();
    }
}

TString SerializeActualNodeForError(const TExprNode& node) {
    const TTypeAnnotationNode* type = node.GetTypeAnn();
    TStringBuilder errMsg;
    if (!type) {
        errMsg << "lambda";
    } else if (node.IsCallable()) {
        errMsg << node.Content() << " with type " << *type;
    } else {
        errMsg << *type;
    }
    return TString(errMsg);
}

std::expected<std::strong_ordering, TString> ParseCallable(const TExprNode::TPtr& callable) {
    if (!callable->IsCallable()) {
        return std::unexpected(TString("Expected callable node"));
    }
    if (callable->ChildrenSize() == 0) {
        return std::unexpected(TString("Expected at least one child"));
    }

    const auto& valNode = callable->Head();
    if (!valNode.IsAtom()) {
        return std::unexpected(TString("Expected atom as first child"));
    }

    const auto callableName = callable->Content();

    if (callableName == "Int8") {
        i8 value = FromStringAtom<i8>(valNode.Content());
        return value <=> static_cast<i8>(0);
    } else if (callableName == "Uint8") {
        ui8 value = FromStringAtom<ui8>(valNode.Content());
        return value <=> static_cast<ui8>(0);
    } else if (callableName == "Int16") {
        i16 value = FromStringAtom<i16>(valNode.Content());
        return value <=> static_cast<i16>(0);
    } else if (callableName == "Uint16") {
        ui16 value = FromStringAtom<ui16>(valNode.Content());
        return value <=> static_cast<ui16>(0);
    } else if (callableName == "Int32") {
        i32 value = FromStringAtom<i32>(valNode.Content());
        return value <=> static_cast<i32>(0);
    } else if (callableName == "Uint32") {
        ui32 value = FromStringAtom<ui32>(valNode.Content());
        return value <=> static_cast<ui32>(0);
    } else if (callableName == "Int64") {
        i64 value = FromStringAtom<i64>(valNode.Content());
        return value <=> static_cast<i64>(0);
    } else if (callableName == "Uint64") {
        ui64 value = FromStringAtom<ui64>(valNode.Content());
        return value <=> static_cast<ui64>(0);
    } else if (callableName == "Float") {
        float value = FromStringAtom<float>(valNode.Content());
        if (std::isnan(value)) {
            return std::unexpected(TString("NaN is not allowed for RANGE frame bounds"));
        }
        if (std::isinf(value)) {
            return std::unexpected(TString("Inf is not allowed for RANGE frame bounds"));
        }
        if (value < 0.0f) {
            return std::strong_ordering::less;
        }
        if (value > 0.0f) {
            return std::strong_ordering::greater;
        }
        return std::strong_ordering::equal;
    } else if (callableName == "Double") {
        double value = FromStringAtom<double>(valNode.Content());
        if (std::isnan(value)) {
            return std::unexpected(TString("NaN is not allowed for RANGE frame bounds"));
        }
        if (std::isinf(value)) {
            return std::unexpected(TString("Inf is not allowed for RANGE frame bounds"));
        }
        if (value < 0.0) {
            return std::strong_ordering::less;
        }
        if (value > 0.0) {
            return std::strong_ordering::greater;
        }
        return std::strong_ordering::equal;
    } else if (callableName == "Interval") {
        NUdf::TDataType<NUdf::TInterval>::TLayout value = FromString<NUdf::TDataType<NUdf::TInterval>::TLayout>(valNode.Content());
        return value <=> static_cast<NUdf::TDataType<NUdf::TInterval>::TLayout>(0);
    } else if (callableName == "Interval64") {
        NUdf::TDataType<NUdf::TInterval64>::TLayout value = FromString<NUdf::TDataType<NUdf::TInterval64>::TLayout>(valNode.Content());
        return value <=> static_cast<NUdf::TDataType<NUdf::TInterval64>::TLayout>(0);
    } else if (callableName == "Decimal") {
        if (callable->ChildrenSize() != 3) {
            return std::unexpected(TString("Expected 3 children for Decimal callable"));
        }
        ui8 precision = FromString<ui8>(callable->Child(1)->Content());
        ui8 scale = FromString<ui8>(callable->Child(2)->Content());
        NDecimal::TInt128 value = NDecimal::FromString(valNode.Content(), precision, scale);
        if (NDecimal::IsNan(value)) {
            return std::unexpected(TString("NaN is not allowed for RANGE frame bounds"));
        }
        if (NDecimal::IsInf(value)) {
            return std::unexpected(TString("Inf is not allowed for RANGE frame bounds"));
        }
        return value <=> NDecimal::TInt128(0);
    }

    return std::unexpected(TStringBuilder() << "Unsupported callable type for RANGE bound: " << callableName);
}

TSorted::ESortDir ExtractSortDirectionFromBool(TExprNode::TPtr sortDirection) {
    YQL_ENSURE(sortDirection->IsAtom());
    auto direction = sortDirection->Content();
    YQL_ENSURE(direction == "true" || direction == "false");
    return (direction == "true") ? TSorted::ESortDir::Asc : TSorted::ESortDir::Desc;
}

TSorted::ESortDir ExtractSortDirection(TExprNode::TPtr sortDirections) {
    if (sortDirections->IsCallable("Bool")) {
        YQL_ENSURE(sortDirections->ChildrenSize() > 0);
        return ExtractSortDirectionFromBool(sortDirections->HeadPtr());
    } else {
        YQL_ENSURE(sortDirections->IsList(), "List or bool expected.");
        YQL_ENSURE(sortDirections->ChildrenSize() > 0, "At least one child expected.");
        return ExtractSortDirection(sortDirections->ChildPtr(0));
    }
}

TSortTraitsInfo ExtractSortTraitsInfo(const TExprNode::TPtr& sortTraits) {
    if (!sortTraits || sortTraits->IsCallable("Void")) {
        return TUnsortedTag{};
    }

    YQL_ENSURE(sortTraits->IsCallable("SortTraits"), "Expected SortTraits or Void.");
    YQL_ENSURE(sortTraits->ChildrenSize() == 3, "Expected exactly three arguments.");

    auto sortDirections = sortTraits->ChildPtr(1);
    auto sortKeyLambda = sortTraits->ChildPtr(2);

    YQL_ENSURE(sortKeyLambda->IsLambda(), "Expected lambda as sort traits.");

    const TTypeAnnotationNode* lambdaType = sortKeyLambda->GetTypeAnn();
    YQL_ENSURE(lambdaType, "Expected to have non null lambda type.");
    const TTypeAnnotationNode* firstColumnType = lambdaType;
    if (lambdaType->GetKind() == ETypeAnnotationKind::Tuple) {
        return TManyColumnsInSort{};
    }

    return TSorted{.SortedColumnType = firstColumnType,
                   .SortDir = ExtractSortDirection(sortDirections)};
}

bool IsUniversal(const TExprNode::TPtr& frameSpec) {
    auto bounds = {GetSettingByName(frameSpec->Children(), "begin"),
                   GetSettingByName(frameSpec->Children(), "end"),
                   GetSettingByName(frameSpec->Children(), "sortSpec")};
    for (auto bound : bounds) {
        if (!bound) {
            continue;
        }

        if (bound->GetTypeAnn() && bound->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
            return true;
        }

        if (bound->IsList() && bound->ChildrenSize() >= 2 && bound->Child(1)->GetTypeAnn() && bound->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
            return true;
        }
    }
    return false;
}

std::expected<TMaybe<i32>, TIssue> ParseFrameRowsBounds(TExprNode::TPtr setting, TExprContext& ctx) {
    if (setting->IsCallable("Int32")) {
        auto& valNode = setting->Head();
        YQL_ENSURE(valNode.IsAtom());
        i32 value;
        YQL_ENSURE(TryFromString(valNode.Content(), value));
        return value;
    }

    if (setting->IsCallable("Void")) {
        return TMaybe<i32>();
    }

    return std::unexpected(TIssue(ctx.GetPosition(setting->Tail().Pos()),
                                  TStringBuilder() << "Invalid "
                                                   << setting->Head().Content()
                                                   << " frame bound - expecting Void or Int32 callable, but got: "
                                                   << SerializeActualNodeForError(*setting->TailPtr())));
}

enum class EType {
    YQL,
    PG,
    NonNumeric,
};

template <EType DispatchType>
class TParseFrameRangeBound {
public:
    consteval static EType Type() {
        return DispatchType;
    }

    static std::expected<TParseFrameBoundResult, TIssue> SingleBound(const TTypeAnnotationNode* sortColumnType, TExprNode::TPtr frameBound, TExprContext& ctx) {
        YQL_ENSURE(frameBound->IsList(), "List expected");

        if (!EnsureTupleMinSize(*frameBound, 1, ctx)) {
            return std::unexpected(TIssue(ctx.GetPosition(frameBound->Pos()), "Expected tuple with at least one size."));
        }
        if (!EnsureAtom(frameBound->Head(), ctx)) {
            return std::unexpected(TIssue(ctx.GetPosition(frameBound->Pos()), "Head must be an atom."));
        }

        auto type = frameBound->Head().Content();
        if (type == "currentRow") {
            if (frameBound->ChildrenSize() == 1) {
                return TParseFrameBoundResult(TExprNodeNumberAndDirection::Zero(), Nothing(), Nothing(), /*isZero=*/true, /*isCurrentRow=*/true, Nothing());
            }
            return std::unexpected(TIssue(ctx.GetPosition(frameBound->Pos()), TStringBuilder() << "Expecting no value for '" << type << "'"));
        }

        if (!(type == "preceding" || type == "following")) {
            return std::unexpected(TIssue(ctx.GetPosition(frameBound->Pos()), TStringBuilder() << "Expecting preceding or following, but got '" << type << "'"));
        }

        EDirection direction = (type == "preceding") ? EDirection::Preceding : EDirection::Following;

        if (!EnsureTupleSize(*frameBound, 2, ctx)) {
            return std::unexpected(TIssue(ctx.GetPosition(frameBound->Pos()), "Expected tuple with at least 2 size for frame bounds."));
        }

        auto boundValue = frameBound->ChildPtr(1);
        if (boundValue->IsAtom()) {
            if (boundValue->Content() == "unbounded") {
                auto node = TExprNodeNumberAndDirection::Inf(direction);
                return TParseFrameBoundResult(node, Nothing(), Nothing(), /*isZero=*/false, /*isCurrentRow=*/false, Nothing());
            }
            return std::unexpected(TIssue(ctx.GetPosition(boundValue->Pos()), TStringBuilder() << "Expecting unbounded, but got '" << boundValue->Content() << "'"));
        }

        if constexpr (Type() == EType::NonNumeric) {
            return std::unexpected(TIssue(ctx.GetPosition(boundValue->Pos()), TStringBuilder() << "Offset specifing is not allowed here since that column type does not support for RANGE mode."));
        }
        YQL_ENSURE(sortColumnType, "Sorted column expected");
        auto message = TStringBuilder() << "Error while processing RANGE bound for column type: " << *sortColumnType << " and offset type: " << *boundValue->GetTypeAnn();
        auto issue = TIssue(ctx.GetPosition(boundValue->Pos()), message);
        if constexpr (Type() == EType::YQL) {
            auto addAllowed = IsAddAllowedYqlTypes(sortColumnType, boundValue->GetTypeAnn(), ctx);
            if (!addAllowed.has_value()) {
                return std::unexpected(issue.AddSubIssue(MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), addAllowed.error())));
            }

            auto parseCallable = ParseCallable(boundValue);
            if (!parseCallable.has_value()) {
                return std::unexpected(issue.AddSubIssue(MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), parseCallable.error())));
            }

            auto parseCallableResult = parseCallable.value();
            if (parseCallableResult == std::strong_ordering::less) {
                return std::unexpected(issue.AddSubIssue(MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), TStringBuilder() << "Expected positive literal value")));
            }
            auto node = TExprNodeNumberAndDirection(boundValue, direction);
            return TParseFrameBoundResult(
                node,
                Nothing(),
                Nothing(),
                /*isZero=*/parseCallableResult == std::strong_ordering::equal,
                /*isCurrentRow=*/false,
                /*procId=*/Nothing());
        } else if constexpr (Type() == EType::PG) {
            auto sign = PgSign(boundValue);
            if (!sign.has_value()) {
                return std::unexpected(issue.AddSubIssue(MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), sign.error())));
            }
            if (sign.value() == std::strong_ordering::less) {
                return std::unexpected(issue.AddSubIssue(MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), "Expected positive literal value")));
            }
            auto inRangeCasts = LookupInRangeCasts(sortColumnType, boundValue->GetTypeAnn(), boundValue->Pos(), ctx);
            if (!inRangeCasts.has_value()) {
                auto message = TStringBuilder() << "Range column and offset types are not compatible";
                auto subIssue = MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), inRangeCasts.error());
                return std::unexpected(issue.AddSubIssue(MakeIntrusive<TIssue>(ctx.GetPosition(boundValue->Pos()), message)).AddSubIssue(subIssue));
            }
            return TParseFrameBoundResult(
                TExprNodeNumberAndDirection(boundValue, direction),
                inRangeCasts->ColumnCast,
                inRangeCasts->OffsetCast,
                /*isZero=*/sign.value() == std::strong_ordering::equal,
                /*isCurrentRow=*/false,
                /*procId=*/inRangeCasts->ProcId);
        }
    }

    static TMaybe<TWindowFrameSettings> Bounds(const TTypeAnnotationNode* sortColumnType, TExprNode::TPtr frameSpec, TExprContext& ctx) {
        auto begin = GetSettingByName(frameSpec->Children(), "begin");
        auto end = GetSettingByName(frameSpec->Children(), "end");
        if (!begin || !end) {
            ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()),
                                TStringBuilder() << "Expected begin and end for row frames."));
            return {};
        }
        auto beginParse = SingleBound(sortColumnType, begin, ctx);
        if (!beginParse.has_value()) {
            ctx.AddError(beginParse.error());
            return {};
        }
        auto endParse = SingleBound(sortColumnType, end, ctx);
        if (!endParse.has_value()) {
            ctx.AddError(endParse.error());
            return {};
        }
        bool isAlwaysEmpty = false;
        bool isNeverEmpty = true;
        if constexpr (Type() == EType::YQL) {
            isAlwaysEmpty = CheckRangeFrameIsAlwaysEmpty(beginParse.value(), endParse.value());
            isNeverEmpty = CheckRangeFrameNeverEmpty(beginParse.value(), endParse.value());
        } else if constexpr (Type() == EType::NonNumeric) {
            isAlwaysEmpty = false;
            isNeverEmpty = true;
        } else {
            static_assert(Type() == EType::PG, "Invalid type");
            auto checkEmpty = CheckRangeFrameIsAlwaysEmptyPg(beginParse.value(), endParse.value());
            if (!checkEmpty.has_value()) {
                ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()), checkEmpty.error()));
                return {};
            }
            isAlwaysEmpty = checkEmpty.value();
            isNeverEmpty = CheckRangeFrameNeverEmpty(beginParse.value(), endParse.value());
        }
        auto sortTraits = ExtractSortTraitsInfo(GetSettingByName(frameSpec->Children(), "sortSpec"));
        auto convertToFrameBound = [](const TParseFrameBoundResult& result) -> TWindowFrameSettingBound {
            return std::visit(TOverloaded{
                                  [&](const TExprNodeNumberAndDirection::TUnbounded) {
                                      return TWindowFrameSettingBound::Inf(result.GetBoundNode().GetDirection());
                                  },
                                  [&](const TExprNodeNumberAndDirection::TZero) {
                                      return TWindowFrameSettingBound::Zero();
                                  },
                                  [&](const TExprNode::TPtr& boundNode) {
                                      return TWindowFrameSettingBound(
                                          TWindowFrameSettingWithOffset(boundNode,
                                                                        result.GetColumnCast(),
                                                                        result.GetBoundCast(),
                                                                        result.GetProcId()), result.GetBoundNode().GetDirection());
                                  },
                              }, result.GetBoundNode().GetValue());
        };

        auto range = TWindowFrameSettings::TRangeFrame(
            {convertToFrameBound(beginParse.value()),
             convertToFrameBound(endParse.value())},
            /*isNumeric=*/Type() != EType::NonNumeric,
            /*sortOrder=*/GetSortOrder(sortTraits),
            /*isRightCurrentRow=*/endParse->IsCurrentRow());

        return TWindowFrameSettings{range,
                                    /*neverEmpty=*/isNeverEmpty,
                                    /*compact=*/GetSettingByName(frameSpec->Children(), "compact") != nullptr,
                                    /*isAlwaysEmpty=*/isAlwaysEmpty};
    }
};

constexpr TStringBuf ErrorNonNumeric = "Range frame for not sorted frames is only allowed to be UNBOUNDED PRECEDING AND CURRENT ROW.";
constexpr TStringBuf ErrorMultipleColumns = "Range frame for multiple expressions is only allowed to be UNBOUNDED PRECEDING AND CURRENT ROW.";
constexpr TStringBuf ErrorNonNumericSingleColumn = "Range frame for non numeric expressions is only allowed to be UNBOUNDED PRECEDING AND CURRENT ROW.";

bool VerifySettings(const TExprNode::TChildrenType& settings, TExprContext& ctx) {
    for (const auto& setting : settings) {
        if (!EnsureTupleMinSize(*setting, 1, ctx)) {
            return false;
        }

        if (!EnsureAtom(setting->Head(), ctx)) {
            return false;
        }
    }
    return true;
}

TMaybe<TWindowFrameSettings> TryParseRangeForNotNumericFrameSettings(TExprNode::TPtr frameSpec, TStringBuf error, TExprContext& ctx) {
    auto result = TParseFrameRangeBound<EType::NonNumeric>::Bounds(nullptr, frameSpec, ctx);
    if (!result) {
        return result;
    }
    auto left = result->GetRangeFrame().GetFirst();
    auto right = result->GetRangeFrame().GetLast();

    if (left.IsInf() && right.IsZero()) {
        return result;
    }

    ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()), error));
    return {};
}

TMaybe<TWindowFrameSettings> TryParseRangeWindowFrameSettings(TExprNode::TPtr frameSpec, TExprContext& ctx) {
    auto sortTraits = ExtractSortTraitsInfo(GetSettingByName(frameSpec->Children(), "sortSpec"));
    if (std::holds_alternative<TUnsortedTag>(sortTraits)) {
        return TryParseRangeForNotNumericFrameSettings(frameSpec, ErrorNonNumeric, ctx);
    } else if (std::holds_alternative<TManyColumnsInSort>(sortTraits)) {
        return TryParseRangeForNotNumericFrameSettings(frameSpec, ErrorMultipleColumns, ctx);
    }
    YQL_ENSURE(std::holds_alternative<TSorted>(sortTraits));
    auto sortedTraits = std::get<TSorted>(sortTraits);
    auto* type = sortedTraits.SortedColumnType;
    if (type->GetKind() == ETypeAnnotationKind::Pg) {
        return TParseFrameRangeBound<EType::PG>::Bounds(sortedTraits.SortedColumnType, frameSpec, ctx);
    }
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    if (type->GetKind() == ETypeAnnotationKind::Data) {
        switch (type->Cast<TDataExprType>()->GetSlot()) {
            case NUdf::EDataSlot::Int8:
            case NUdf::EDataSlot::Uint8:
            case NUdf::EDataSlot::Int16:
            case NUdf::EDataSlot::Uint16:
            case NUdf::EDataSlot::Int32:
            case NUdf::EDataSlot::Uint32:
            case NUdf::EDataSlot::Int64:
            case NUdf::EDataSlot::Uint64:
            case NUdf::EDataSlot::Double:
            case NUdf::EDataSlot::Float:
            case NUdf::EDataSlot::Date:
            case NUdf::EDataSlot::Datetime:
            case NUdf::EDataSlot::Timestamp:
            case NUdf::EDataSlot::Interval:
            case NUdf::EDataSlot::TzDate:
            case NUdf::EDataSlot::TzDatetime:
            case NUdf::EDataSlot::TzTimestamp:
            case NUdf::EDataSlot::Date32:
            case NUdf::EDataSlot::Datetime64:
            case NUdf::EDataSlot::Timestamp64:
            case NUdf::EDataSlot::Interval64:
            case NUdf::EDataSlot::TzDate32:
            case NUdf::EDataSlot::TzDatetime64:
            case NUdf::EDataSlot::TzTimestamp64:
            case NUdf::EDataSlot::Decimal:
                return TParseFrameRangeBound<EType::YQL>::Bounds(sortedTraits.SortedColumnType, frameSpec, ctx);
            default:
                return TryParseRangeForNotNumericFrameSettings(frameSpec, ErrorNonNumericSingleColumn, ctx);
        }
    }
    return TryParseRangeForNotNumericFrameSettings(frameSpec, ErrorNonNumericSingleColumn, ctx);
}

TMaybe<TWindowFrameSettings> TryParseWindowFrameSettingsFromList(const TExprNode& node, TExprContext& ctx) {
    auto frameSpec = node.Child(0);
    bool isCompact = GetSettingByName(frameSpec->Children(), "compact") != nullptr;

    if (node.IsCallable({"WinOnRows", "WinFilter"})) {
        if (!GetSettingByName(frameSpec->Children(), "begin") || !GetSettingByName(frameSpec->Children(), "end")) {
            ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()),
                                TStringBuilder() << "Expected begin and end for row frames."));
            return {};
        }
        auto leftParse = ParseFrameRowsBounds(GetSettingByName(frameSpec->Children(), "begin"), ctx);
        if (!leftParse.has_value()) {
            ctx.AddError(leftParse.error());
            return {};
        }

        auto rightParse = ParseFrameRowsBounds(GetSettingByName(frameSpec->Children(), "end"), ctx);
        if (!rightParse.has_value()) {
            ctx.AddError(rightParse.error());
            return {};
        }

        auto frame = TWindowFrameSettings::TRowFrame{leftParse.value(), rightParse.value()};
        return TWindowFrameSettings(frame, /*neverEmpty=*/CheckRowFrameNeverEmpty(frame), /*compact=*/isCompact, /*isAlwaysEmpty=*/CheckRowFrameIsAlwaysEmpty(frame));
    } else if (node.IsCallable("WinOnRange")) {
        return TryParseRangeWindowFrameSettings(frameSpec, ctx);
    } else {
        YQL_ENSURE(node.IsCallable("WinOnGroups"));
        TWindowFrameSettings::TGroupsFrame frame{};
        return TWindowFrameSettings(frame, /*neverEmpty=*/false, /*compact=*/isCompact, /*isAlwaysEmpty=*/false);
    }
}

} // namespace

TWindowFrameSettings::TWindowFrameSettings(TFrame frameBounds, bool neverEmpty, bool compact, bool isAlwaysEmpty)
    : FrameBounds_(std::move(frameBounds))
    , NeverEmpty_(neverEmpty)
    , Compact_(compact)
    , IsAlwaysEmpty_(isAlwaysEmpty)
{
}

TWindowFrameSettings TWindowFrameSettings::Parse(const TExprNode& node, TExprContext& ctx) {
    bool isUniversal;
    auto maybeSettings = TryParse(node, ctx, isUniversal);
    YQL_ENSURE(maybeSettings && !isUniversal);
    return *maybeSettings;
}

TMaybe<TWindowFrameSettings> TWindowFrameSettings::TryParse(const TExprNode& node, TExprContext& ctx, bool& isUniversal) {
    auto frameSpec = node.Child(0);
    isUniversal = false;
    if (frameSpec->Type() == TExprNode::List) {
        if (!VerifySettings(frameSpec->Children(), ctx)) {
            return {};
        }
        isUniversal = IsUniversal(frameSpec);
        if (isUniversal) {
            return {};
        }
        return TryParseWindowFrameSettingsFromList(node, ctx);
    } else {
        const TTypeAnnotationNode* type = frameSpec->GetTypeAnn();
        ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()),
                            TStringBuilder() << "Invalid window frame - expecting Tuple, but got: " << (type ? FormatType(type) : "lambda")));
        return {};
    }
}

TExprNode::TPtr TWindowFrameSettings::GetSortSpec(const TExprNode& node, TExprContext& ctx) {
    auto frameSpec = node.Child(0);
    if (frameSpec->Type() != TExprNode::List) {
        return nullptr;
    }
    if (!VerifySettings(frameSpec->Children(), ctx)) {
        return nullptr;
    }
    return GetSettingByName(frameSpec->Children(), "sortSpec");
}

bool TWindowFrameSettings::IsFullPartition() const {
    return IsLeftInf() && IsRightInf();
}

EFrameType TWindowFrameSettings::GetFrameType() const {
    return std::visit(TOverloaded{
                          [&](const TRowFrame&) {
                              return FrameByRows;
                          },
                          [&](const TRangeFrame&) {
                              return FrameByRange;
                          },
                          [&](const TGroupsFrame&) {
                              return FrameByGroups;
                          },
                      }, FrameBounds_);
}

bool TWindowFrameSettings::IsLeftInf() const {
    return std::visit(TOverloaded{
                          [&](const TRowFrame& rowFrame) {
                              return !rowFrame.first.Defined();
                          },
                          [&](const TRangeFrame& rangeFrame) {
                              return rangeFrame.GetFirst().IsInf();
                          },
                          [&](const TGroupsFrame&) {
                              YQL_ENSURE(0, "Not implemented.");
                              return false;
                          },
                      }, FrameBounds_);
}

bool TWindowFrameSettings::IsRightInf() const {
    return std::visit(TOverloaded{
                          [&](const TRowFrame& rowFrame) {
                              return !rowFrame.second.Defined();
                          },
                          [&](const TRangeFrame& rangeFrame) {
                              return rangeFrame.GetLast().IsInf();
                          },
                          [&](const TGroupsFrame&) {
                              YQL_ENSURE(0, "Not implemented.");
                              return false;
                          },
                      }, FrameBounds_);
}

bool TWindowFrameSettings::IsRightCurrent() const {
    return std::visit(TOverloaded{
                          [&](const TRowFrame& rowFrame) {
                              return rowFrame.second.Defined() && rowFrame.second == 0;
                          },
                          [&](const TRangeFrame& rangeFrame) {
                              return rangeFrame.IsRightCurrentRow();
                          },
                          [&](const TGroupsFrame&) {
                              YQL_ENSURE(0, "Not implemented.");
                              return false;
                          },
                      }, FrameBounds_);
}

bool CheckRowFrameIsAlwaysEmpty(const TWindowFrameSettings::TRowFrame& frame) {
    return frame.first.Defined() && frame.second.Defined() && *frame.first > *frame.second;
};

EFrameBoundsNewType GetFrameTypeNew(const TWindowFrameSettings& frameSettings) {
    if (frameSettings.IsFullPartition()) {
        return EFrameBoundsNewType::FULL;
    }
    if (frameSettings.IsAlwaysEmpty()) {
        return EFrameBoundsNewType::EMPTY;
    }
    if (frameSettings.IsLeftInf() && !frameSettings.IsRightInf()) {
        return EFrameBoundsNewType::INCREMENTAL;
    }
    return EFrameBoundsNewType::GENERIC;
}

} // namespace NYql
