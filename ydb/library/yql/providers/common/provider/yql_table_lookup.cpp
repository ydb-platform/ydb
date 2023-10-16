#include "yql_table_lookup.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <array>

namespace NYql {
namespace NCommon {

using namespace NNodes;

namespace {

template<typename TCompare>
TExprBase BuildColumnCompare(TExprBase row, const TString& columnName, TExprBase value, TExprContext& ctx) {
    auto compare = Build<TCompare>(ctx, row.Pos())
        .template Left<TCoMember>()
            .Struct(row)
            .Name().Build(columnName)
            .Build()
        .Right(value)
        .Done();

    TMaybeNode<TExprBase> ret = compare;

    auto rowType = row.Ref().GetTypeAnn()->Cast<TStructExprType>();
    auto columnType = rowType->GetItems()[*rowType->FindItem(columnName)]->GetItemType();

    if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
        ret = Build<TCoCoalesce>(ctx, row.Pos())
            .Predicate(compare)
            .template Value<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Done();
    }

    return ret.Cast();
}

TMaybeNode<TExprBase> CombinePredicatesAnd(TMaybeNode<TExprBase> left, TMaybeNode<TExprBase> right, TExprContext& ctx) {
    if (left && right) {
        return Build<TCoAnd>(ctx, left.Cast().Pos())
            .Add({left.Cast(), right.Cast()})
            .Done();
    } else if (left) {
        return left;
    } else if (right) {
        return right;
    }

    return TMaybeNode<TExprBase>();
}

TMaybeNode<TExprBase> CombinePredicatesOr(TMaybeNode<TExprBase> left, TMaybeNode<TExprBase> right, TExprContext& ctx) {
    if (left && right) {
        return Build<TCoOr>(ctx, left.Cast().Pos())
            .Add({left.Cast(), right.Cast()})
            .Done();
    }

    return TMaybeNode<TExprBase>();
}

template<typename TItem, typename TFunc>
TMaybeNode<TExprBase> CombinePredicateListOr(const TVector<TItem>& list, TExprContext& ctx,
    TFunc getPredicateFunc)
{
    if (list.empty()) {
        return {};
    }

    TMaybeNode<TExprBase> residualPredicate = getPredicateFunc(list[0]);
    for (size_t i = 1; i < list.size(); ++i) {
        auto predicate = getPredicateFunc(list[i]);
        residualPredicate = CombinePredicatesOr(residualPredicate, predicate, ctx);
    }

    return residualPredicate;
}

TMaybeNode<TExprBase> BuildColumnRangePredicate(const TString& column, const TColumnRange& range,
    TExprBase row, TExprContext& ctx)
{
    if (range.IsNull()) {
        return Build<TCoNot>(ctx, row.Pos())
            .Value<TCoExists>()
                .Optional<TCoMember>()
                    .Struct(row)
                    .Name().Build(column)
                    .Build()
                .Build()
            .Done();
    }

    if (range.IsPoint()) {
        return BuildColumnCompare<TCoCmpEqual>(row, column, range.GetFrom().GetValue(), ctx);
    }

    TMaybeNode<TExprBase> fromPredicate;
    if (range.GetFrom().IsDefined()) {
        fromPredicate = range.GetFrom().IsInclusive()
            ? BuildColumnCompare<TCoCmpGreaterOrEqual>(row, column, range.GetFrom().GetValue(), ctx)
            : BuildColumnCompare<TCoCmpGreater>(row, column, range.GetFrom().GetValue(), ctx);
    }

    TMaybeNode<TExprBase> toPredicate;
    if (range.GetTo().IsDefined()) {
        toPredicate = range.GetTo().IsInclusive()
            ? BuildColumnCompare<TCoCmpLessOrEqual>(row, column, range.GetTo().GetValue(), ctx)
            : BuildColumnCompare<TCoCmpLess>(row, column, range.GetTo().GetValue(), ctx);
    }

    if (fromPredicate && toPredicate) {
        return Build<TCoAnd>(ctx, row.Pos())
            .Add({fromPredicate.Cast(), toPredicate.Cast()})
            .Done();
    } else if (fromPredicate) {
        return fromPredicate.Cast();
    } else if (toPredicate){
        return toPredicate.Cast();
    }

    return TMaybeNode<TExprBase>();
}

TMaybeNode<TExprBase> BuildKeyRangePredicate(const TVector<TString>& columns, const TKeyRange& keyRange,
    TExprBase row, TExprContext& ctx)
{
    YQL_ENSURE(columns.size() == keyRange.GetColumnRangesCount());

    TMaybeNode<TExprBase> predicate = keyRange.GetResidualPredicate();
    for (size_t i = 0; i < columns.size(); ++i) {
        auto& column = columns[i];
        auto& range = keyRange.GetColumnRange(i);

        auto rangePredicate = BuildColumnRangePredicate(column, range, row, ctx);
        predicate = CombinePredicatesAnd(predicate, rangePredicate, ctx);
    }

    return predicate;
}

struct TLookupContext {
    TLookupContext(TExprContext& exprCtx, TTableLookup::TGetValueFunc getValueFunc,
        TTableLookup::TCanCompareFunc canCompareFunc, TTableLookup::TCompareFunc compareFunc)
        : ExprCtx(exprCtx)
        , GetValueFunc(getValueFunc)
        , CanCompareFunc(canCompareFunc)
        , CompareFunc(compareFunc) {}

    TRangeBound MinBound(const TRangeBound& a, const TRangeBound& b) const {
        YQL_ENSURE(a.IsDefined() && b.IsDefined());

        if (!CanCompareFunc(a.GetValue()) || !CanCompareFunc(b.GetValue())) {
            return TRangeBound::MakeUndefined();
        }

        switch (CompareFunc(a.GetValue(), b.GetValue()).Result) {
            case TTableLookup::TCompareResult::Less:
                return a;
            case TTableLookup::TCompareResult::Greater:
                return b;
            case TTableLookup::TCompareResult::Equal:
                return a.IsInclusive() && b.IsInclusive() ? a : TRangeBound(a.GetValue(), false);
        }

        YQL_ENSURE(false);
    }

    TRangeBound MaxBound(const TRangeBound& a, const TRangeBound& b) const {
        YQL_ENSURE(a.IsDefined() && b.IsDefined());

        if (!CanCompareFunc(a.GetValue()) || !CanCompareFunc(b.GetValue())) {
            return TRangeBound::MakeUndefined();
        }

        switch (CompareFunc(a.GetValue(), b.GetValue()).Result) {
            case TTableLookup::TCompareResult::Less:
                return b;
            case TTableLookup::TCompareResult::Greater:
                return a;
            case TTableLookup::TCompareResult::Equal:
                return a.IsInclusive() && b.IsInclusive() ? a : TRangeBound(a.GetValue(), false);
        }

        YQL_ENSURE(false);
    }

    TExprContext& ExprCtx;
    TTableLookup::TGetValueFunc GetValueFunc;
    TTableLookup::TCanCompareFunc CanCompareFunc;
    TTableLookup::TCompareFunc CompareFunc;
    bool AllowNullCompare = false;
};

TColumnRange MakeColumnRange(TRangeBound left, TRangeBound right, const TLookupContext& ctx) {
    if (!left.IsDefined() || !right.IsDefined()) {
        return TColumnRange(left, right);
    }

    if (!ctx.CanCompareFunc(left.GetValue()) || !ctx.CanCompareFunc(right.GetValue())) {
        return TColumnRange(left, right);
    }

    auto cmp = ctx.CompareFunc(left.GetValue(), right.GetValue());
    switch (cmp.Result) {
        case TTableLookup::TCompareResult::Less:
            if (cmp.AreAdjacent()) {
                if (left.IsInclusive() && !right.IsInclusive()) {
                    return TColumnRange::MakePoint(left.GetValue());
                } else if (!left.IsInclusive() && right.IsInclusive()) {
                    return TColumnRange::MakePoint(right.GetValue());
                }
            }
            break;

        case TTableLookup::TCompareResult::Greater:
            break;

        case TTableLookup::TCompareResult::Equal:
            if (left.IsInclusive() && right.IsInclusive()) {
                return TColumnRange::MakePoint(left.GetValue());
            }
            break;
    }

    return TColumnRange(left, right);
}

class TKeyRangeBuilder {
public:
    TKeyRangeBuilder(const TVector<TString>& keyColumns)
        : KeyColumns(keyColumns)
    {
        for (auto& keyColumn : keyColumns) {
            ColumnRanges[keyColumn] = TColumnRange::MakeUnbounded();
        }
    }

    TKeyRangeBuilder(const TKeyRangeBuilder& builder, TMaybeNode<TExprBase> residualPredicate)
        : KeyColumns(builder.KeyColumns)
        , ColumnRanges(builder.ColumnRanges)
        , ResidualPredicate(residualPredicate) {}

    bool IsFullScan() const {
        return std::find_if(ColumnRanges.begin(), ColumnRanges.end(),
            [] (const std::pair<TString, TColumnRange>& pair) {
                return pair.second.IsDefined();
            }) == ColumnRanges.end();
    }

    const TVector<TString>& GetKeyColumns() const {
        return KeyColumns;
    }

    TColumnRange GetColumnRange(const TStringBuf& column) const {
        auto* keyRange = ColumnRanges.FindPtr(column);
        YQL_ENSURE(keyRange);

        return *keyRange;
    }

    bool HasColumnRange(const TStringBuf& column) const {
        auto* keyRange = ColumnRanges.FindPtr(column);
        return keyRange != nullptr;
    }

    void SetColumnRange(const TStringBuf& column, const TColumnRange& range) {
        auto it = ColumnRanges.find(column);
        YQL_ENSURE(it != ColumnRanges.end());

        it->second = range;
    }

    TMaybeNode<TExprBase> GetResidualPredicate() const {
        return ResidualPredicate;
    }

    void SetResidualPredicate(TMaybeNode<TExprBase> predicate) {
        ResidualPredicate = predicate;
    }

    TMaybeNode<TExprBase> BuildPredicate(const TVector<TString>& columns,
        TExprBase row, TExprContext& ctx) const
    {
        auto keyRange = BuildKeyRange(row, ctx);
        return BuildKeyRangePredicate(columns, keyRange, row, ctx);
    }

    TKeyRange BuildKeyRange(TExprBase row, TExprContext& ctx) const {
        if (IsFullScan()) {
            return TKeyRange(ctx, KeyColumns.size(), ResidualPredicate);
        }

        auto newResidualPredicate = ResidualPredicate;
        TVector<TColumnRange> newColumnRanges;

        bool keyRangeFinished = false;
        for (const TString& column : KeyColumns) {
            auto columnRange = GetColumnRange(column);

            if (keyRangeFinished) {
                if (columnRange.IsDefined()) {
                    auto predicate = BuildColumnRangePredicate(column, columnRange, row, ctx);
                    newResidualPredicate = CombinePredicatesAnd(newResidualPredicate, predicate, ctx);
                }
                newColumnRanges.push_back(TColumnRange::MakeUnbounded());
            } else {
                keyRangeFinished = !columnRange.IsPoint();
                newColumnRanges.push_back(columnRange);
            }
        }

        return TKeyRange(ctx, newColumnRanges, newResidualPredicate);
    }

private:
    TVector<TString> KeyColumns;
    THashMap<TString, TColumnRange> ColumnRanges;
    TMaybeNode<TExprBase> ResidualPredicate;
};

class TTableLookupBuilder {
public:
    TTableLookupBuilder(const TVector<TString>& keyColumns, const TVector<TKeyRangeBuilder>& keyRangeBuilders)
        : KeyColumns(keyColumns)
        , KeyRangeBuilders(keyRangeBuilders) {}

    TTableLookupBuilder(const TVector<TString>& keyColumns)
        : TTableLookupBuilder(keyColumns, {}) {}

    TTableLookupBuilder(const TVector<TString>& keyColumns, const TKeyRangeBuilder& keyRangeBuilder)
        : TTableLookupBuilder(keyColumns, TVector<TKeyRangeBuilder>{keyRangeBuilder}) {}

    bool IsSingleRange() const {
        return KeyRangeBuilders.size() == 1;
    }

    bool HasNonFullscanRanges() const {
        for (auto& range : KeyRangeBuilders) {
            if (!range.IsFullScan()) {
                return true;
            }
        }

        return false;
    }

    const TVector<TKeyRangeBuilder>& GetKeyRangeBuilders() const {
        return KeyRangeBuilders;
    }

    const TKeyRangeBuilder& GetKeyRangeBuilder() const {
        YQL_ENSURE(IsSingleRange());
        return KeyRangeBuilders.front();
    }

    TTableLookup BuildTableLookup(TExprBase row, const TLookupContext& ctx) const {
        TVector<TKeyRange> keyRanges;
        for (auto& keyRangeBuilder : KeyRangeBuilders) {
            keyRanges.push_back(keyRangeBuilder.BuildKeyRange(row, ctx.ExprCtx));
        }

        if (!IsSingleRange()) {
            if (CheckIndependentRanges(keyRanges, ctx)) {
                return TTableLookup(KeyColumns, keyRanges);
            }

            if (TryBuildPointRanges(keyRanges, ctx)) {
                return TTableLookup(KeyColumns, keyRanges);
            }

            auto keyRange = BuildBoundingKeyRange(keyRanges, row, ctx.ExprCtx);
            return TTableLookup(KeyColumns, {keyRange});
        }

        return TTableLookup(KeyColumns, keyRanges);
    }

private:
    static bool TryBuildPointRanges(TVector<TKeyRange>& keyRanges, const TLookupContext& ctx) {
        YQL_ENSURE(keyRanges.size() > 1);

        const auto& firstRange = keyRanges[0];
        ui32 numDefined = firstRange.GetNumDefined();

        for (const auto& range : keyRanges) {
            if (range.GetNumDefined() != numDefined) {
                return false;
            }

            for (size_t i = 0; i < numDefined; ++i) {
                auto& columnRange = range.GetColumnRange(i);
                if (!columnRange.IsPoint()) {
                    return false;
                }

                if (columnRange.IsNull()) {
                    return false;
                }

                if (!ctx.CanCompareFunc(columnRange.GetFrom().GetValue())) {
                    return false;
                }
            }
        }

        auto less = [&ctx, numDefined] (const TKeyRange &a, const TKeyRange& b) {
            for (size_t i = 0; i < numDefined; ++i) {
                auto cmpResult = ctx.CompareFunc(
                    a.GetColumnRange(i).GetFrom().GetValue(),
                    b.GetColumnRange(i).GetFrom().GetValue());

                switch (cmpResult.Result) {
                    case TTableLookup::TCompareResult::Less:
                        return true;
                    case TTableLookup::TCompareResult::Greater:
                        return false;
                    case TTableLookup::TCompareResult::Equal:
                        break;
                }
            }

            return false;
        };

        std::stable_sort(keyRanges.begin(), keyRanges.end(), less);

        TMaybeNode<TExprBase> residualPredicate;
        size_t curIndex = 0;
        for (size_t i = 0; i < keyRanges.size(); ++i) {
            auto predicate = keyRanges[i].GetResidualPredicate();

            residualPredicate = residualPredicate
                ? CombinePredicatesOr(residualPredicate, predicate, ctx.ExprCtx)
                : predicate;

            if (i == keyRanges.size() - 1 || less(keyRanges[i], keyRanges[i + 1])) {
                TKeyRange keyRange(ctx.ExprCtx, keyRanges[i].GetColumnRanges(), residualPredicate);
                keyRanges[curIndex] = keyRange;
                ++curIndex;

                residualPredicate = {};
            }
        }

        keyRanges.erase(keyRanges.begin() + curIndex, keyRanges.end());
        return true;
    }

    static bool CheckIndependentRanges(TVector<TKeyRange>& keyRanges, const TLookupContext& ctx) {
        for (auto& keyRange : keyRanges) {
            for (auto& columnRange : keyRange.GetColumnRanges()) {
                if (columnRange.GetFrom().IsDefined() && !ctx.CanCompareFunc(columnRange.GetFrom().GetValue())) {
                    return false;
                }

                if (columnRange.GetTo().IsDefined() && !ctx.CanCompareFunc(columnRange.GetTo().GetValue())) {
                    return false;
                }
            }
        }

        auto less = [&ctx] (const TKeyRange &a, const TKeyRange& b) {
            return KeyTupleLess(a.GetFromTuple(), b.GetFromTuple(), ctx.CompareFunc);
        };

        std::stable_sort(keyRanges.begin(), keyRanges.end(), less);

        for (size_t i = 0; i < keyRanges.size() - 1; ++i) {
            if (!KeyTupleLess(keyRanges[i].GetToTuple(), keyRanges[i + 1].GetFromTuple(), ctx.CompareFunc)) {
                return false;
            }
        }

        return true;
    }

    TKeyRange BuildBoundingKeyRange(const TVector<TKeyRange> &keyRanges,
        TExprBase row, TExprContext& ctx) const
    {
        YQL_ENSURE(keyRanges.size() > 0);

        auto residualPredicate = CombinePredicateListOr(keyRanges, ctx,
            [this, row, &ctx] (const TKeyRange& keyRange) {
                return BuildKeyRangePredicate(KeyColumns, keyRange, row, ctx);
            });

        // TODO: Compute actual bounding range.
        return TKeyRange(ctx, KeyColumns.size(), residualPredicate);
    }

private:
    TVector<TString> KeyColumns;
    TVector<TKeyRangeBuilder> KeyRangeBuilders;
};

TKeyRangeBuilder CombineKeyRangesAnd(TExprBase row, const TVector<TString>& keyColumns, const TKeyRangeBuilder& left,
    const TKeyRangeBuilder& right, const TLookupContext& ctx)
{
    TKeyRangeBuilder combinedLookup(keyColumns);

    auto rp = CombinePredicatesAnd(left.GetResidualPredicate(), right.GetResidualPredicate(), ctx.ExprCtx);

    for (const TString& column : left.GetKeyColumns()) {
        auto columnLeft = left.GetColumnRange(column);
        auto columnRight = right.GetColumnRange(column);

        TColumnRange columnRange;

        if (columnLeft.IsNull()) {
            columnRange = columnLeft;
            if (columnRight.IsDefined()) {
                auto predicate = BuildColumnRangePredicate(column, columnRight, row, ctx.ExprCtx);
                rp = CombinePredicatesAnd(rp, predicate, ctx.ExprCtx);
            }
        } else if (columnRight.IsNull()) {
            columnRange = columnRight;
            if (columnLeft.IsDefined()) {
                auto predicate = BuildColumnRangePredicate(column, columnLeft, row, ctx.ExprCtx);
                rp = CombinePredicatesAnd(rp, predicate, ctx.ExprCtx);
            }
        } else {
            TRangeBound combinedFrom;
            TRangeBound combinedTo;

            if (columnLeft.GetFrom().IsDefined() && columnRight.GetFrom().IsDefined()) {
                combinedFrom = ctx.MaxBound(columnLeft.GetFrom(), columnRight.GetFrom());
                if (!combinedFrom.IsDefined()) {
                    combinedFrom = columnLeft.GetFrom();
                    rp = CombinePredicatesAnd(rp,
                        BuildColumnRangePredicate(column, columnRight, row, ctx.ExprCtx),
                        ctx.ExprCtx);
                }
            } else if (columnLeft.GetFrom().IsDefined()) {
                combinedFrom = columnLeft.GetFrom();
            } else if (columnRight.GetFrom().IsDefined()) {
                combinedFrom = columnRight.GetFrom();
            }

            if (columnLeft.GetTo().IsDefined() && columnRight.GetTo().IsDefined()) {
                combinedTo = ctx.MinBound(columnLeft.GetTo(), columnRight.GetTo());
                if (!combinedTo.IsDefined()) {
                    combinedTo = columnLeft.GetTo();
                    rp = CombinePredicatesAnd(rp,
                        BuildColumnRangePredicate(column, columnRight, row, ctx.ExprCtx),
                        ctx.ExprCtx);
                }
            } else if (columnLeft.GetTo().IsDefined()) {
                combinedTo = columnLeft.GetTo();
            } else if (columnRight.GetTo().IsDefined()) {
                combinedTo = columnRight.GetTo();
            }

            columnRange = MakeColumnRange(combinedFrom, combinedTo, ctx);
        }

        combinedLookup.SetColumnRange(column, columnRange);
    }

    combinedLookup.SetResidualPredicate(rp);

    return combinedLookup;
}

TTableLookupBuilder CombineLookupsAnd(TExprBase row, const TVector<TString>& keyColumns,
    const TTableLookupBuilder* builders, size_t size, const TLookupContext& ctx)
{
    switch (size) {
        case 0U: Y_ABORT("Wrong case");
        case 1U: return *builders;
        case 2U: {
            const auto& left = builders[0U];
            const auto& right = builders[1U];
            if (left.IsSingleRange() && right.IsSingleRange()) {
                auto combinedKeyRange = CombineKeyRangesAnd(row, keyColumns, left.GetKeyRangeBuilder(),
                    right.GetKeyRangeBuilder(), ctx);
                return TTableLookupBuilder(keyColumns, combinedKeyRange);
            } else {
                auto combineRanges =
                    [&keyColumns, row, &ctx] (const TTableLookupBuilder& lookup, const TTableLookupBuilder& residual) {
                        auto& residualKeyRanges = residual.GetKeyRangeBuilders();
                        auto residualPredicate = CombinePredicateListOr(residualKeyRanges, ctx.ExprCtx,
                            [&keyColumns, row, &ctx](const TKeyRangeBuilder& keyRange) {
                                return keyRange.BuildPredicate(keyColumns, row, ctx.ExprCtx);
                            });

                        TVector<TKeyRangeBuilder> newKeyRanges;
                        for (auto& keyRange : lookup.GetKeyRangeBuilders()) {
                            auto newResidualPredicate = CombinePredicatesAnd(keyRange.GetResidualPredicate(),
                                residualPredicate, ctx.ExprCtx);
                            auto newRange = TKeyRangeBuilder(keyRange, newResidualPredicate);
                            newKeyRanges.push_back(newRange);
                        }

                        return TTableLookupBuilder(keyColumns, newKeyRanges);
                    };

                const bool keepRight = right.HasNonFullscanRanges() && !left.HasNonFullscanRanges();
                return keepRight ? combineRanges(right, left) : combineRanges(left, right);
            }
        }
        default: break;
    }

    const auto half = (size + 1U) >> 1U;
    const std::array<TTableLookupBuilder, 2U> pair = {{
        CombineLookupsAnd(row, keyColumns, builders, half, ctx),
        CombineLookupsAnd(row, keyColumns, builders + half, size - half, ctx)
    }};
    return CombineLookupsAnd(row, keyColumns, pair.data(), pair.size(), ctx);
}

TTableLookupBuilder CombineLookupsOr(TExprBase, const TVector<TString>& keyColumns,
    const TTableLookupBuilder* builders, size_t size, const TLookupContext&)
{
    TVector<TKeyRangeBuilder> newKeyRanges;
    newKeyRanges.reserve(size);
    for (size_t i = 0U; i < size; ++i) {
        newKeyRanges.insert(newKeyRanges.end(), builders[i].GetKeyRangeBuilders().cbegin(), builders[i].GetKeyRangeBuilders().cend());
    }
    return TTableLookupBuilder(keyColumns, newKeyRanges);
}

TTableLookupBuilder CollectLookups(TExprBase row, TExprBase predicate,
    const TVector<TString>& keyColumns, const TLookupContext& ctx)
{
    if (const auto maybeAnd = predicate.Maybe<TCoAnd>()) {
        const auto size = maybeAnd.Cast().Args().size();
        std::vector<TTableLookupBuilder> builders;
        builders.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            builders.emplace_back(CollectLookups(row, maybeAnd.Cast().Arg(i), keyColumns, ctx));
        }
        return CombineLookupsAnd(row, keyColumns, builders.data(), builders.size(), ctx);
    }

    if (const auto maybeOr = predicate.Maybe<TCoOr>()) {
        const auto size = maybeOr.Cast().Args().size();
        std::vector<TTableLookupBuilder> builders;
        builders.reserve(size);
        for (auto i = 0U; i < size; ++i) {
            builders.emplace_back(CollectLookups(row, maybeOr.Cast().Arg(i), keyColumns, ctx));
        }
        return CombineLookupsOr(row, keyColumns, builders.data(), builders.size(), ctx);
    }

    TMaybeNode<TCoCompare> maybeCompare = predicate.Maybe<TCoCompare>();
    if (auto maybeLiteral = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
        if (maybeLiteral.Cast().Value() == "false") {
            maybeCompare = predicate.Cast<TCoCoalesce>().Predicate().Maybe<TCoCompare>();
        }
    }

    TTableLookupBuilder fullScan(keyColumns, TKeyRangeBuilder(keyColumns, predicate));

    auto getRowMember = [row] (TExprBase expr) {
        if (auto maybeMember = expr.Maybe<TCoMember>()) {
            if (maybeMember.Cast().Struct().Raw() == row.Raw()) {
                return maybeMember;
            }
        }

        return TMaybeNode<TCoMember>();
    };

    auto getTableLookup = [&keyColumns, fullScan] (const TStringBuf& column, const TColumnRange& range,
        TMaybeNode<TExprBase> residualPredicate)
    {
        TKeyRangeBuilder keyRange(keyColumns, residualPredicate);
        if (keyRange.HasColumnRange(column)) {
            keyRange.SetColumnRange(column, range);
            return TTableLookupBuilder(keyColumns, keyRange);
        }

        return fullScan;
    };

    auto getNullComparePredicate = [&ctx](TExprBase value, TExprBase rowValue) -> TMaybeNode<TExprBase> {
        if (ctx.AllowNullCompare || rowValue.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
            return TMaybeNode<TExprBase>();
        }

        return Build<TCoExists>(ctx.ExprCtx, rowValue.Pos())
            .Optional(value)
            .Done();
    };

    if (maybeCompare) {
        auto left = maybeCompare.Cast().Left();
        auto right = maybeCompare.Cast().Right();

        TMaybeNode<TCoMember> maybeLeftMember = getRowMember(left);
        TMaybeNode<TCoMember> maybeRightMember = getRowMember(right);

        TMaybeNode<TCoMember> maybeMember;
        TMaybeNode<TExprBase> maybeValue;
        bool reverseCompare = false;
        if (maybeLeftMember) {
            maybeMember = maybeLeftMember;
            maybeValue = ctx.GetValueFunc(right, maybeMember.Cast().Ref().GetTypeAnn(), ctx.ExprCtx);
        } else if (maybeRightMember) {
            maybeMember = maybeRightMember;
            maybeValue = ctx.GetValueFunc(left, maybeMember.Cast().Ref().GetTypeAnn(), ctx.ExprCtx);
            reverseCompare = true;
        }

        if (!maybeValue) {
            return fullScan;
        }

        auto value = maybeValue.Cast();
        auto column = maybeMember.Cast().Name().Value();

        TMaybe<TColumnRange> columnRange;

        if (maybeCompare.Maybe<TCoCmpEqual>()) {
            columnRange = TColumnRange::MakePoint(value);
        }

        if (maybeCompare.Maybe<TCoCmpLess>()) {
            columnRange = reverseCompare
                ? MakeColumnRange(TRangeBound::MakeExclusive(value), TRangeBound::MakeUndefined(), ctx)
                : MakeColumnRange(TRangeBound::MakeUndefined(), TRangeBound::MakeExclusive(value), ctx);
        }

        if (maybeCompare.Maybe<TCoCmpLessOrEqual>()) {
            columnRange = reverseCompare
                ? MakeColumnRange(TRangeBound::MakeInclusive(value), TRangeBound::MakeUndefined(), ctx)
                : MakeColumnRange(TRangeBound::MakeUndefined(), TRangeBound::MakeInclusive(value), ctx);
        }

        if (maybeCompare.Maybe<TCoCmpGreater>()) {
            columnRange = reverseCompare
                ? MakeColumnRange(TRangeBound::MakeUndefined(), TRangeBound::MakeExclusive(value), ctx)
                : MakeColumnRange(TRangeBound::MakeExclusive(value), TRangeBound::MakeUndefined(), ctx);
        }

        if (maybeCompare.Maybe<TCoCmpGreaterOrEqual>()) {
            columnRange = reverseCompare
                ? MakeColumnRange(TRangeBound::MakeUndefined(), TRangeBound::MakeInclusive(value), ctx)
                : MakeColumnRange(TRangeBound::MakeInclusive(value), TRangeBound::MakeUndefined(), ctx);
        }

        if (columnRange) {
            return getTableLookup(column, *columnRange, getNullComparePredicate(value, maybeMember.Cast()));
        }
    }

    auto maybeLookup = TMaybeNode<TCoLookupBase>();
    if (auto maybeLiteral = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
        if (maybeLiteral.Cast().Value() == "false") {
            maybeLookup = predicate.Maybe<TCoCoalesce>().Predicate().Maybe<TCoLookupBase>();
        }
    } else {
        maybeLookup = predicate.Maybe<TCoLookupBase>();
    }

    if (maybeLookup) {
        auto lookup = maybeLookup.Cast();

        if (!lookup.Lookup().Maybe<TCoMember>()) {
            return fullScan;
        }

        auto member = lookup.Lookup().Cast<TCoMember>();
        auto column = member.Name().Value();
        if (member.Struct().Raw() != row.Raw()) {
            return fullScan;
        }

        TExprNode::TPtr collection;
        if (lookup.Collection().Ref().IsList()) {
            collection = lookup.Collection().Ptr();
        } else if (auto maybeDictFromKeys = lookup.Collection().Maybe<TCoDictFromKeys>()) {
            collection = maybeDictFromKeys.Cast().Keys().Ptr();
        } else {
            return fullScan;
        }

        auto size = collection->ChildrenSize();
        if (!size) {
            return fullScan;
        }

        TVector<TKeyRangeBuilder> keyRanges;
        keyRanges.reserve(size);
        for (const auto& key : collection->Children()) {
            auto maybeValue = ctx.GetValueFunc(TExprBase(key), member.Ref().GetTypeAnn(), ctx.ExprCtx);
            if (!maybeValue) {
                return fullScan;
            }

            TKeyRangeBuilder keyRange(keyColumns);
            if (!keyRange.HasColumnRange(column)) {
                return fullScan;
            }
            keyRange.SetColumnRange(column, TColumnRange::MakePoint(maybeValue.Cast()));

            keyRanges.push_back(keyRange);
        }

        return TTableLookupBuilder(keyColumns, keyRanges);
    }

    if (auto maybeBool = predicate.Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
        if (maybeBool.Cast().Value() == "false") {
            TExprBase value = predicate.Cast<TCoCoalesce>().Predicate();
            TMaybeNode<TCoMember> maybeMember = getRowMember(value);
            if (maybeMember) {
                auto column = maybeMember.Cast().Name().Value();
                auto columnRange = TColumnRange::MakePoint(
                    Build<TCoBool>(ctx.ExprCtx, row.Pos()).Literal().Build("true").Done());
                return getTableLookup(column, columnRange, {});
            }
        }
    }

    if (auto maybeBool = predicate.Maybe<TCoNot>().Value().Maybe<TCoCoalesce>().Value().Maybe<TCoBool>().Literal()) {
        if (maybeBool.Cast().Value() == "true") {
            TExprBase value = predicate.Cast<TCoNot>().Value().Cast<TCoCoalesce>().Predicate();
            TMaybeNode<TCoMember> maybeMember = getRowMember(value);
            if (maybeMember) {
                auto column = maybeMember.Cast().Name().Value();
                auto columnRange = TColumnRange::MakePoint(
                    Build<TCoBool>(ctx.ExprCtx, row.Pos()).Literal().Build("false").Done());
                return getTableLookup(column, columnRange, {});
            }
        }
    }

    if (auto maybeExists = predicate.Maybe<TCoNot>().Value().Maybe<TCoExists>()) {
        TMaybeNode<TCoMember> maybeMember = getRowMember(maybeExists.Cast().Optional());
        if (maybeMember) {
            auto column = maybeMember.Cast().Name().Value();
            auto columnRange = TColumnRange::MakeNull(ctx.ExprCtx);
            return getTableLookup(column, columnRange, {});
        }
    }

    return fullScan;
}

} // namespace

TColumnRange TColumnRange::MakeNull(TExprContext& ctx) {
    auto nullValue = Build<TCoNull>(ctx, TPositionHandle()).Done();
    TColumnRange range = MakePoint(nullValue);
    range.Null = true;
    return range;
}

TKeyRange::TKeyRange(TExprContext& ctx, const TVector<TColumnRange>& columnRanges, TMaybeNode<NNodes::TExprBase> residualPredicate)
    : Ctx(&ctx)
    , ColumnRanges(columnRanges)
    , ResidualPredicate(residualPredicate)
    , NumDefined(0)
{
    EquiRange = true;

    TVector<NNodes::TMaybeNode<NNodes::TExprBase>> fromValues(columnRanges.size());
    bool fromInclusive = true;

    TVector<NNodes::TMaybeNode<NNodes::TExprBase>> toValues(columnRanges.size());
    bool toInclusive = true;

    for (size_t i = 0; i < columnRanges.size(); ++i) {
        const auto& range = columnRanges[i];

        if (!range.IsPoint()) {
            if (range.IsDefined()) {
                YQL_ENSURE(EquiRange);
            }

            EquiRange = false;
        }

        if (range.IsDefined()) {
            ++NumDefined;
        }

        if (range.GetFrom().IsDefined()) {
            fromValues[i] = range.GetFrom().GetValue();
            fromInclusive = range.GetFrom().IsInclusive();
        }

        if (range.GetTo().IsDefined()) {
            toValues[i] = range.GetTo().GetValue();
            toInclusive = range.GetTo().IsInclusive();
        }
    }

    FromTuple = TKeyTuple(Ctx, fromValues, true, fromInclusive);
    ToTuple = TKeyTuple(Ctx, toValues, false, toInclusive);
}

void TTableLookup::Print(IOutputStream& output) const {
    output << Endl << "[" << Endl;
    for (auto& keyRange : KeyRanges) {
        keyRange.Print(output);
    }
    output << "]" << Endl;
}

void TKeyRange::Print(IOutputStream& output) const {
    auto printExpr = [ctx = Ctx] (TExprBase node, IOutputStream& output) {
        auto ast = ConvertToAst(node.Ref(), *ctx, TExprAnnotationFlags::None, true);
        ast.Root->PrintTo(output);
    };

    output << "{" << Endl;
    for (size_t i = 0; i < GetColumnRangesCount(); ++i) {
        auto& range = GetColumnRange(i);

        output << "  " << i << " ";

        if (range.IsNull()) {
            output << "NULL" << Endl;
            continue;
        }

        if (range.GetFrom().IsInclusive()) {
            output << "[";
        } else {
            output << "(";
        }

        if (range.GetFrom().IsDefined()) {
            printExpr(range.GetFrom().GetValue(), output);
        } else {
            output << "*";
        }

        output << "; ";

        if (range.GetTo().IsDefined()) {
            printExpr(range.GetTo().GetValue(), output);
        } else {
            output << "*";
        }

        if (range.GetTo().IsInclusive()) {
            output << "]";
        } else {
            output << ")";
        }

        output << Endl;
    }

    auto residualPredicate = GetResidualPredicate();
    output << "  Residual: ";
    if (residualPredicate) {
        printExpr(residualPredicate.Cast(), output);
    } else {
        output << "None";
    }

    output << Endl << "}" << Endl;
}

void TKeyTuple::Print(IOutputStream& output) const {
    auto printExpr = [] (TExprContext& ctx, TExprBase node, IOutputStream& output) {
        auto ast = ConvertToAst(node.Ref(), ctx, TExprAnnotationFlags::None, true);
        ast.Root->PrintTo(output);
    };

    output << "\"";
    output << (IsFrom()
        ? IsInclusive() ? "[" : "("
        : IsInclusive() ? "]" : ")");
    output << "\" (";

    for (size_t i = 0; i < Size(); ++i) {
        auto value = GetValue(i);
        if (value) {
            YQL_ENSURE(Ctx);
            printExpr(*Ctx, value.Cast(), output);
        } else {
            output << (IsFrom()
                ? IsInclusive() ? "-Inf" : "+Inf"
                : IsInclusive() ? "+Inf" : "-Inf");
        }

        if (i < Size() - 1) {
            output << ", ";
        }
    }

    output << ")";
}

bool KeyTupleLess(const TKeyTuple& left, const TKeyTuple& right, const TTableLookup::TCompareFunc& cmpFunc) {
    YQL_ENSURE(left.Size() == right.Size());

    TTableLookup::TCompareResult::TResult cmpResult = TTableLookup::TCompareResult::Equal;
    for (size_t i = 0; i < left.Size(); ++i) {
        auto leftValue = left.GetValue(i);
        auto rightValue = right.GetValue(i);

        if (leftValue && rightValue) {
            cmpResult = cmpFunc(leftValue.Cast(),rightValue.Cast()).Result;
        } else if (!leftValue) {
            if (left.IsFrom() == left.IsInclusive()) {
                if (rightValue || right.IsFrom() != right.IsInclusive()) {
                    cmpResult = TTableLookup::TCompareResult::Less;
                }
            } else {
                if (rightValue || right.IsFrom() == right.IsInclusive()) {
                    cmpResult = TTableLookup::TCompareResult::Greater;
                }
            }
        } else {
            cmpResult = right.IsFrom() == right.IsInclusive()
                ? TTableLookup::TCompareResult::Greater
                : TTableLookup::TCompareResult::Less;
        }

        if (cmpResult != TTableLookup::TCompareResult::Equal) {
            break;
        }
    }

    if (cmpResult == TTableLookup::TCompareResult::Equal) {
        if (left.IsFrom() && right.IsFrom() && left.IsInclusive() && !right.IsInclusive()) {
            return true;
        }

        if (!left.IsFrom() && right.IsFrom() && !(left.IsInclusive() && right.IsInclusive())) {
            return true;
        }

        if (!left.IsFrom() && !right.IsFrom() && !left.IsInclusive() && right.IsInclusive()) {
            return true;
        }
    }

    return cmpResult == TTableLookup::TCompareResult::Less;
}

TTableLookup ExtractTableLookup(
    TExprBase row,
    TExprBase predicate,
    const TVector<TString>& keyColumns,
    TTableLookup::TGetValueFunc getValueFunc,
    TTableLookup::TCanCompareFunc canCompareFunc,
    TTableLookup::TCompareFunc compareFunc,
    TExprContext& exprCtx,
    bool allowNullCompare)
{
    TLookupContext ctx(exprCtx, getValueFunc, canCompareFunc, compareFunc);
    ctx.AllowNullCompare = allowNullCompare;

    auto tableLookupBuilder = CollectLookups(row, predicate, keyColumns, ctx);
    auto tableLookup = tableLookupBuilder.BuildTableLookup(row, ctx);

    return tableLookup;
}

} // namespace NCommon
} // namespace NYql
