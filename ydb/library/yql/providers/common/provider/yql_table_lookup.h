#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <functional>

namespace NYql {
namespace NCommon {

class TRangeBound
{
public:
    TRangeBound(NNodes::TMaybeNode<NNodes::TExprBase> value, bool isInclusive)
        : Value(value)
        , Inclusive(isInclusive) {}

    TRangeBound()
        : TRangeBound(NNodes::TMaybeNode<NNodes::TExprBase>(), false) {}

    bool IsDefined() const {
        return Value.IsValid();
    }

    NNodes::TExprBase GetValue() const {
        YQL_ENSURE(IsDefined());
        return Value.Cast();
    }

    bool IsInclusive() const {
        return Inclusive;
    }

    static TRangeBound MakeUndefined() {
        return TRangeBound();
    }

    static TRangeBound MakeInclusive(NNodes::TExprBase value) {
        return TRangeBound(value, true);
    }

    static TRangeBound MakeExclusive(NNodes::TExprBase value) {
        return TRangeBound(value, false);
    }

private:
    NNodes::TMaybeNode<NNodes::TExprBase> Value;
    bool Inclusive;
};

class TColumnRange {
public:
    TColumnRange(const TRangeBound& from, const TRangeBound& to)
        : From(from)
        , To(to)
        , Defined(From.IsDefined() || To.IsDefined())
    {
        Point = From.IsDefined() && To.IsDefined() &&
            From.IsInclusive() && To.IsInclusive() &&
            From.GetValue().Raw() == To.GetValue().Raw();
    }

    TColumnRange()
        : TColumnRange(TRangeBound(), TRangeBound()) {}

    TRangeBound GetFrom() const { return From; }
    TRangeBound GetTo() const { return To; }

    bool IsDefined() const {
        return Defined;
    }

    bool IsPoint() const {
        return Point;
    }

    bool IsNull() const {
        return Null;
    }

    static TColumnRange MakeUnbounded() {
        return TColumnRange(TRangeBound(), TRangeBound());
    }

    static TColumnRange MakePoint(NNodes::TExprBase value) {
        return TColumnRange(TRangeBound(value, true), TRangeBound(value, true));
    }

    static TColumnRange MakeNull(TExprContext& ctx);

private:
    TRangeBound From;
    TRangeBound To;
    bool Defined;
    bool Point;
    bool Null = false;
};

class TKeyTuple {
public:
    TKeyTuple(TExprContext* ctx, const TVector<NNodes::TMaybeNode<NNodes::TExprBase>>& values, bool isFrom, bool isInclusive)
        : Ctx(ctx)
        , Values(values)
        , From(isFrom)
        , Inclusive(isInclusive) {}

    TKeyTuple()
        : TKeyTuple(nullptr, {}, false, false) {}

    size_t Size() const { return Values.size(); }
    bool IsFrom() const { return From; }
    bool IsInclusive() const { return Inclusive; }
    NNodes::TMaybeNode<NNodes::TExprBase> GetValue(size_t index) const { return Values[index]; }

    void Print(IOutputStream& output) const;

private:
    TExprContext* Ctx;
    TVector<NNodes::TMaybeNode<NNodes::TExprBase>> Values;
    bool From;
    bool Inclusive;
};

class TKeyRange {
public:
    TKeyRange(TExprContext& ctx, const TVector<TColumnRange>& columnRanges, NNodes::TMaybeNode<NNodes::TExprBase> residualPredicate);

    TKeyRange(TExprContext& ctx, size_t keySize, NNodes::TMaybeNode<NNodes::TExprBase> residualPredicate)
        : TKeyRange(ctx, TVector<TColumnRange>(keySize, TColumnRange()), residualPredicate) {}

    size_t GetColumnRangesCount() const {
        return ColumnRanges.size();
    }

    const TColumnRange& GetColumnRange(size_t index) const {
        return ColumnRanges[index];
    }

    const TVector<TColumnRange>& GetColumnRanges() const {
        return ColumnRanges;
    }

    bool IsFullScan() const {
        return NumDefined == 0;
    }

    bool IsEquiRange() const {
        return EquiRange;
    }

    size_t GetNumDefined() const {
        return NumDefined;
    }

    bool HasResidualPredicate() const {
        return ResidualPredicate.IsValid();
    }

    NNodes::TMaybeNode<NNodes::TExprBase> GetResidualPredicate() const {
        return ResidualPredicate;
    }

    const TKeyTuple& GetFromTuple() const {
        return FromTuple;
    }

    const TKeyTuple& GetToTuple() const {
        return ToTuple;
    }

    void Print(IOutputStream& output) const;

private:
    TExprContext* Ctx;
    TVector<TColumnRange> ColumnRanges;
    NNodes::TMaybeNode<NNodes::TExprBase> ResidualPredicate;
    bool EquiRange;
    size_t NumDefined;
    TKeyTuple FromTuple;
    TKeyTuple ToTuple;
};

class TTableLookup {
public:
    struct TCompareResult
    {
        enum TResult {
            Equal,
            Less,
            Greater
        };

        TCompareResult(TResult result, TMaybe<bool> adjacent = {})
            : Result(result)
            , Adjacent(adjacent) {}

        bool AreAdjacent() const {
            return Adjacent.Defined() && *Adjacent;
        }

        TResult Result;
        TMaybe<bool> Adjacent;
    };

    typedef std::function<NNodes::TMaybeNode<NNodes::TExprBase>(NNodes::TExprBase,
        const TTypeAnnotationNode*, TExprContext& ctx)> TGetValueFunc;
    typedef std::function<bool(NNodes::TExprBase)> TCanCompareFunc;
    typedef std::function<TCompareResult(NNodes::TExprBase, NNodes::TExprBase)> TCompareFunc;

    TTableLookup(const TVector<TString>& keyColumns, const TVector<TKeyRange>& keyRanges)
        : KeyColumns(keyColumns)
        , KeyRanges(keyRanges) {}

    const TVector<TString>& GetKeyColumns() const {
        return KeyColumns;
    }

    const TVector<TKeyRange>& GetKeyRanges() const {
        return KeyRanges;
    }

    bool IsSingleRange() const {
        return KeyRanges.size() == 1;
    }

    bool IsFullScan() const {
        return IsSingleRange() && KeyRanges.front().IsFullScan();
    }

    void Print(IOutputStream& output) const;

private:
    TVector<TString> KeyColumns;
    TVector<TKeyRange> KeyRanges;
};

TTableLookup ExtractTableLookup(
    NNodes::TExprBase row,
    NNodes::TExprBase predicate,
    const TVector<TString>& keyColumns,
    TTableLookup::TGetValueFunc getValueFunc,
    TTableLookup::TCanCompareFunc canCompareFunc,
    TTableLookup::TCompareFunc compareFunc,
    TExprContext& exprCtx,
    bool allowNullCompare);

bool KeyTupleLess(const TKeyTuple& left, const TKeyTuple& right, const TTableLookup::TCompareFunc& cmpFunc);

} // namespace NCommon
} // namespace NYql
