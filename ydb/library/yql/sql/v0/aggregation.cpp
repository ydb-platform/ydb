#include "node.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_type_string.h>

#include <library/cpp/charset/ci_string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <array>

using namespace NYql;

namespace NSQLTranslationV0 {

class TAggregationFactory : public IAggregation {
public:
    TAggregationFactory(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi = false)
        : IAggregation(pos, name, func, aggMode), Factory(!func.empty() ?
            BuildBind(Pos, aggMode == EAggregateMode::OverWindow ? "window_module" : "aggregate_module", func) : nullptr),
        DynamicFactory(!Factory), Multi(multi)
    {
        if (!Factory) {
            FakeSource = BuildFakeSource(pos);
        }
    }

protected:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        ui32 expectedArgs = !Factory ? 2 : (isFactory ? 0 : 1);
        if (!Factory) {
            YQL_ENSURE(!isFactory);
        }

        if (expectedArgs != exprs.size()) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory  ? "factory " : "") << Name
                << " requires exactly " << expectedArgs << " argument(s), given: " << exprs.size();
            return false;
        }

        if (!Factory) {
            Factory = exprs[1];
        }

        if (!isFactory) {
            Expr = exprs.front();
            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr AggregationTraitsFactory() const override {
        return Factory;
    }

    TNodePtr GetApply(const TNodePtr& type) const override {
        if (!Multi) {
            return Y("Apply", Factory, (DynamicFactory ? Y("ListItemType", type) : type),
              BuildLambda(Pos, Y("row"), Y("EnsurePersistable", Expr)));
        }

        return Y("MultiAggregate",
            Y("ListItemType", type),
            BuildLambda(Pos, Y("row"), Y("EnsurePersistable", Expr)),
            Factory);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Expr) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!Expr->Init(ctx, src)) {
            return false;
        }
        if (Expr->IsAggregated() && !Expr->IsAggregationKey() && !IsOverWindow()) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden for non window functions";
            return false;
        }
        if (AggMode == EAggregateMode::Distinct) {
            const auto column = Expr->GetColumnName();
            if (!column) {
                ctx.Error(Expr->GetPos()) << "DISTINCT qualifier may only be used with column references";
                return false;
            }
            DistinctKey = *column;
            YQL_ENSURE(src);
            if (src->GetJoin()) {
                const auto sourcePtr = Expr->GetSourceName();
                if (!sourcePtr || !*sourcePtr) {
                    if (!src->IsGroupByColumn(DistinctKey)) {
                        ctx.Error(Expr->GetPos()) << ErrorDistinctWithoutCorrelation(DistinctKey);
                        return false;
                    }
                } else {
                    DistinctKey = DotJoin(*sourcePtr, DistinctKey);
                }
            }
            if (src->IsGroupByColumn(DistinctKey)) {
                ctx.Error(Expr->GetPos()) << ErrorDistinctByGroupKey(DistinctKey);
                return false;
            }
            Expr = AstNode("row");
            ctx.PopBlockShortcuts();
        } else {
            Expr = ctx.GroundBlockShortcutsForExpr(Expr);
        }

        if (FakeSource) {
            ctx.PushBlockShortcuts();
            if (!Factory->Init(ctx, FakeSource.Get())) {
                return false;
            }

            Factory = ctx.GroundBlockShortcutsForExpr(Factory);
            if (AggMode == EAggregateMode::OverWindow) {
                Factory = BuildLambda(Pos, Y("type", "extractor"), Y("block", Q(Y(
                    Y("let", "x", Y("Apply", Factory, "type", "extractor")),
                    Y("return", Y("WindowTraits",
                        Y("NthArg", Q("0"), "x"),
                        Y("NthArg", Q("1"), "x"),
                        Y("NthArg", Q("2"), "x"),
                        BuildLambda(Pos, Y("value", "state"), Y("Void")),
                        Y("NthArg", Q("6"), "x"),
                        Y("NthArg", Q("7"), "x")
                    ))
                ))));
            }
        }

        return true;
    }

    TNodePtr Factory;
    TSourcePtr FakeSource;
    TNodePtr Expr;
    bool DynamicFactory;
    bool Multi;
};

class TAggregationFactoryImpl final : public TAggregationFactory {
public:
    TAggregationFactoryImpl(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi)
        : TAggregationFactory(pos, name, func, aggMode, multi)
    {}

private:
    TNodePtr DoClone() const final {
        return new TAggregationFactoryImpl(Pos, Name, Func, AggMode, Multi);
    }
};

class TAggregationFactoryWinAutoargImpl final : public TAggregationFactory {
public:
    TAggregationFactoryWinAutoargImpl(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, func, aggMode)
    {}

    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        Y_UNUSED(isFactory);
        if (!IsOverWindow()) {
            ctx.Error(Pos) << "Expected aggregation function: " << GetName() << " only as window function. You may have forgotten OVER instruction.";
            return false;
        }
        TVector<TNodePtr> exprsAuto;
        if (!exprs) {
            auto winNamePtr = src->GetWindowName();
            YQL_ENSURE(winNamePtr);
            auto winSpecPtr = src->FindWindowSpecification(ctx, *winNamePtr);
            if (!winSpecPtr) {
                return false;
            }
            const auto& orderSpec = winSpecPtr->OrderBy;
            if (!orderSpec) {
                ctx.Warning(Pos, TIssuesIds::YQL_AGGREGATE_BY_WIN_FUNC_WITHOUT_ORDER_BY) <<
                    "Expected ORDER BY specification for window: '" << *winNamePtr <<
                    "' used in aggregation function: '" << GetName() <<
                    " You may have forgotten to ORDER BY in WINDOW specification or choose the wrong WINDOW.";
            }
            for (const auto& spec: orderSpec) {
                exprsAuto.push_back(spec->OrderExpr);
            }
            if (exprsAuto.size() > 1) {
                exprsAuto = {BuildTuple(GetPos(), exprsAuto)};
            }
        }
        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, exprsAuto ? exprsAuto : exprs);
    }
private:
    TNodePtr DoClone() const final {
        return new TAggregationFactoryWinAutoargImpl(Pos, Name, Func, AggMode);
    }
};

TAggregationPtr BuildFactoryAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi) {
    return new TAggregationFactoryImpl(pos, name, func, aggMode, multi);
}

TAggregationPtr BuildFactoryAggregationWinAutoarg(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode) {
    return new TAggregationFactoryWinAutoargImpl(pos, name, func, aggMode);
}

class TKeyPayloadAggregationFactory final : public TAggregationFactory {
public:
    TKeyPayloadAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 2;
        if (exprs.size() < adjustArgsCount || exprs.size() > 1 + adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name << " requires "
                << adjustArgsCount << " or " << (1 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Payload = exprs.front();
            Key = exprs[1];
        }

        Limit = (1 + adjustArgsCount == exprs.size() ? exprs.back() : Y("Void"));
        if (!isFactory) {
            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TKeyPayloadAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Key), BuildLambda(Pos, Y("row"), Payload));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Limit);
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {1u, 0u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Key) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!Key->Init(ctx, src)) {
            return false;
        }
        Key = ctx.GroundBlockShortcutsForExpr(Key);
        ctx.PushBlockShortcuts();
        if (!Payload->Init(ctx, src)) {
            return false;
        }
        Payload = ctx.GroundBlockShortcutsForExpr(Payload);
        if (!Limit->Init(ctx, src)) {
            return false;
        }

        if (Key->IsAggregated()) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TNodePtr Key, Payload, Limit;
};

TAggregationPtr BuildKeyPayloadFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TKeyPayloadAggregationFactory(pos, name, factory, aggMode);
}

class TPayloadPredicateAggregationFactory final : public TAggregationFactory {
public:
    TPayloadPredicateAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 2;
        if (exprs.size() != adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name << " requires " <<
                adjustArgsCount << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Payload = exprs.front();
            Predicate = exprs.back();

            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TPayloadPredicateAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        return Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Payload), BuildLambda(Pos, Y("row"), Predicate));
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {0u, 1u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Predicate) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!Predicate->Init(ctx, src)) {
            return false;
        }
        Predicate = ctx.GroundBlockShortcutsForExpr(Predicate);
        ctx.PushBlockShortcuts();
        if (!Payload->Init(ctx, src)) {
            return false;
        }
        Payload = ctx.GroundBlockShortcutsForExpr(Payload);

        if (Payload->IsAggregated()) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
            return false;
        }

        return true;
    }

    TNodePtr Payload, Predicate;
};

TAggregationPtr BuildPayloadPredicateFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TPayloadPredicateAggregationFactory(pos, name, factory, aggMode);
}

class TTwoArgsAggregationFactory final : public TAggregationFactory {
public:
    TTwoArgsAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 2;
        if (exprs.size() != adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name << " requires " <<
                adjustArgsCount << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            One = exprs.front();
            Two = exprs.back();

            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTwoArgsAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto tuple = Q(Y(One, Two));
        return Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), tuple));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!One) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!One->Init(ctx, src)) {
            return false;
        }
        One = ctx.GroundBlockShortcutsForExpr(One);
        ctx.PushBlockShortcuts();
        if (!Two->Init(ctx, src)) {
            return false;
        }
        Two = ctx.GroundBlockShortcutsForExpr(Two);

        if ((One->IsAggregated() || Two->IsAggregated()) && !IsOverWindow()) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TNodePtr One, Two;
};

TAggregationPtr BuildTwoArgsFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TTwoArgsAggregationFactory(pos, name, factory, aggMode);
}

class THistogramAggregationFactory final : public TAggregationFactory {
public:
    THistogramAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource(BuildFakeSource(pos))
        , Weight(Y("Double", Q("1.0")))
        , Intervals(Y("Uint32", Q("100")))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        if (isFactory) {
            if (exprs.size() > 1) {
                ctx.Error(Pos) << "Aggregation function factory " << Name << " requires 0 or 1 argument(s), given: " << exprs.size();
                return false;
            }
        } else {
            if (exprs.empty() || exprs.size() > 3) {
                ctx.Error(Pos) << "Aggregation function " << Name << " requires one, two or three arguments, given: " << exprs.size();
                return false;
            }
        }

        if (!isFactory) {
            /// \todo: solve it with named arguments
            const auto integer = exprs.back()->IsIntegerLiteral();
            switch (exprs.size()) {
            case 2U:
                if (!integer) {
                    Weight = exprs.back();
                }
                break;
            case 3U:
                if (!integer) {
                    ctx.Error(Pos) << "Aggregation function " << Name << " for case with 3 argument should have second interger argument";
                    return false;
                }
                Weight = exprs[1];
                break;
            }
            if (exprs.size() >= 2 && integer) {
                Intervals = Y("Cast", exprs.back(), Q("Uint32"));
            }
        } else {
            if (exprs.size() >= 1) {
                const auto integer = exprs.back()->IsIntegerLiteral();
                if (!integer) {
                    ctx.Error(Pos) << "Aggregation function factory " << Name << " should have second interger argument";
                    return false;
                }

                Intervals = Y("Cast", exprs.back(), Q("Uint32"));
            }
        }

        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front()));
    }

    TNodePtr DoClone() const final {
        return new THistogramAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Expr), BuildLambda(Pos, Y("row"), Weight));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Intervals);
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {0u, 1u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        ctx.PushBlockShortcuts();
        if (!Weight->Init(ctx, src)) {
            return false;
        }
        Weight = ctx.GroundBlockShortcutsForExpr(Weight);
        ctx.PushBlockShortcuts();
        if (!Intervals->Init(ctx, FakeSource.Get())) {
            return false;
        }
        Intervals = ctx.GroundBlockShortcutsForExpr(Intervals);

        return TAggregationFactory::DoInit(ctx, src);
    }

    TSourcePtr FakeSource;
    TNodePtr Weight, Intervals;
};

TAggregationPtr BuildHistogramFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new THistogramAggregationFactory(pos, name, factory, aggMode);
}

class TLinearHistogramAggregationFactory final : public TAggregationFactory {
public:
    TLinearHistogramAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource(BuildFakeSource(pos))
        , BinSize(Y("Double", Q("10.0")))
        , Minimum(Y("Double", Q(ToString(-1.0 * Max<double>()))))
        , Maximum(Y("Double", Q(ToString(Max<double>()))))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        Y_UNUSED(isFactory);
        if (exprs.empty() || exprs.size() > 4) {
            ctx.Error(Pos) << "Aggregation function " << Name << " requires one to four arguments, given: " << exprs.size();
            return false;
        }

        if (exprs.size() > 1) {
            BinSize = exprs[1];
        }

        if (exprs.size() > 2) {
            Minimum = exprs[2];
        }

        if (exprs.size() > 3) {
            Maximum = exprs[3];
        }

        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, { exprs.front() });
    }

    TNodePtr DoClone() const final {
        return new TLinearHistogramAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        return Y("Apply", Factory, type,
            BuildLambda(Pos, Y("row"), Expr),
            BinSize, Minimum, Maximum);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        ctx.PushBlockShortcuts();
        if (!BinSize->Init(ctx, FakeSource.Get())) {
            return false;
        }
        BinSize = ctx.GroundBlockShortcutsForExpr(BinSize);
        ctx.PushBlockShortcuts();
        if (!Minimum->Init(ctx, FakeSource.Get())) {
            return false;
        }
        Minimum = ctx.GroundBlockShortcutsForExpr(Minimum);
        ctx.PushBlockShortcuts();
        if (!Maximum->Init(ctx, FakeSource.Get())) {
            return false;
        }
        Maximum = ctx.GroundBlockShortcutsForExpr(Maximum);

        return TAggregationFactory::DoInit(ctx, src);
    }

    TSourcePtr FakeSource;
    TNodePtr BinSize, Minimum, Maximum;
};

TAggregationPtr BuildLinearHistogramFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TLinearHistogramAggregationFactory(pos, name, factory, aggMode);
}

class TPercentileFactory final : public TAggregationFactory {
public:
    TPercentileFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource(BuildFakeSource(pos))
    {}

private:
    const TString* GetGenericKey() const final {
        return Column;
    }

    void Join(IAggregation* aggr) final {
        const auto percentile = dynamic_cast<TPercentileFactory*>(aggr);
        Y_ABORT_UNLESS(percentile);
        Y_ABORT_UNLESS(*Column == *percentile->Column);
        Y_ABORT_UNLESS(AggMode == percentile->AggMode);
        Percentiles.insert(percentile->Percentiles.cbegin(), percentile->Percentiles.cend());
        percentile->Percentiles.clear();
    }

    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        if (exprs.size() < 0 + adjustArgsCount  || exprs.size() > 1 + adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name << " requires "
                << (0 + adjustArgsCount) << " or " << (1 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Column = exprs.front()->GetColumnName();
            if (!Column) {
                ctx.Error(Pos) << Name << " may only be used with column reference as first argument.";
                return false;
            }
        }

        if (!TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front())))
            return false;

        TNodePtr x;
        if (1 + adjustArgsCount == exprs.size()) {
            x = exprs.back();
            ctx.PushBlockShortcuts();
            if (!x->Init(ctx, FakeSource.Get())) {
                return false;
            }
            x = ctx.GroundBlockShortcutsForExpr(x);
        } else {
            x = Y("Double", Q("0.5"));
        }

        if (isFactory) {
            FactoryPercentile = x;
        } else {
            Percentiles.emplace(Name, x);
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TPercentileFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        TNodePtr percentiles(Percentiles.cbegin()->second);

        if (Percentiles.size() > 1U) {
            percentiles = Y();
            for (const auto& percentile : Percentiles) {
                percentiles = L(percentiles, percentile.second);
            }
            percentiles = Q(percentiles);
        }

        return Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Expr), percentiles);
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, FactoryPercentile);
    }

    TNodePtr AggregationTraits(const TNodePtr& type) const final {
        if (Percentiles.empty())
            return TNodePtr();

        TNodePtr names(Q(Percentiles.cbegin()->first));

        if (Percentiles.size() > 1U) {
            names = Y();
            for (const auto& percentile : Percentiles)
                names = L(names, Q(percentile.first));
            names = Q(names);
        }

        const bool distinct = AggMode == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos, DistinctKey))) : type;
        return distinct ? Q(Y(names, GetApply(listType), BuildQuotedAtom(Pos, DistinctKey))) : Q(Y(names, GetApply(listType)));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (const auto& p : Percentiles) {
            if (!p.second->Init(ctx, src)) {
                return false;
            }
        }

        return TAggregationFactory::DoInit(ctx, src);
    }

    TSourcePtr FakeSource;
    std::multimap<TString, TNodePtr> Percentiles;
    TNodePtr FactoryPercentile;
    const TString* Column = nullptr;
};

TAggregationPtr BuildPercentileFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TPercentileFactory(pos, name, factory, aggMode);
}

class TTopFreqFactory final : public TAggregationFactory {
public:
    TTopFreqFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:

    //first - n, second - buffer
    using TPair = std::pair<TNodePtr, TNodePtr>;

    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        const double DefaultBufferC = 1.5;
        const ui32 MinBuffer = 100;

        if (exprs.size() < adjustArgsCount || exprs.size() > 2 + adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory? "factory " : "") << Name <<
                " requires " << adjustArgsCount << " to " << (2 + adjustArgsCount)  << " arguments, given: " << exprs.size();
            return false;
        }

        if (!TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front())))
            return false;

        ui32 n;
        ui32 buffer;

        if (1 + adjustArgsCount <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            if (!Parseui32(exprs[adjustArgsCount], n)) {
                ctx.Error(posSecondArg) << "TopFreq: invalid argument #" << (1 + adjustArgsCount) << ", numeric literal is expected";
                return false;
            }
        } else {
            n = 1;
        }

        if (2 + adjustArgsCount == exprs.size()) {
            auto posThirdArg = exprs[1 + adjustArgsCount]->GetPos();
            if (Parseui32(exprs[1 + adjustArgsCount], buffer)) {
                if (n > buffer) {
                    ctx.Error(posThirdArg) << "TopFreq: #" << (2 + adjustArgsCount) << " argument (buffer size) must be greater or equal than previous argument ";
                    return false;
                }
            } else {
                ctx.Error(posThirdArg) << "TopFreq: invalid #" << (2 + adjustArgsCount) << " argument, numeric literal is expected";
                return false;
            }
        } else {
            buffer = std::max(ui32(n * DefaultBufferC), MinBuffer);
        }

        auto x = TPair{ Y("Uint32", Q(ToString(n))), Y("Uint32", Q(ToString(buffer))) };
        if (isFactory) {
            TopFreqFactoryParams = x;
        } else {
            TopFreqs.emplace(Name, x);
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTopFreqFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        TPair topFreqs(TopFreqs.cbegin()->second);

        if (TopFreqs.size() > 1U) {
            topFreqs = { Y(), Y() };
            for (const auto& topFreq : TopFreqs) {
                topFreqs = { L(topFreqs.first, topFreq.second.first), L(topFreqs.second, topFreq.second.second) };
            }
            topFreqs = { Q(topFreqs.first), Q(topFreqs.second) };
        }

        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Expr), topFreqs.first, topFreqs.second);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, TopFreqFactoryParams.first, TopFreqFactoryParams.second);
    }

    TNodePtr AggregationTraits(const TNodePtr& type) const final {
        if (TopFreqs.empty())
            return TNodePtr();

        TNodePtr names(Q(TopFreqs.cbegin()->first));

        if (TopFreqs.size() > 1U) {
            names = Y();
            for (const auto& topFreq : TopFreqs)
                names = L(names, Q(topFreq.first));
            names = Q(names);
        }

        const bool distinct = AggMode == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos, DistinctKey))) : type;
        return distinct ? Q(Y(names, GetApply(listType), BuildQuotedAtom(Pos, DistinctKey))) : Q(Y(names, GetApply(listType)));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (const auto& topFreq : TopFreqs) {
            if (!topFreq.second.first->Init(ctx, src)) {
                return false;
            }

            if (!topFreq.second.second->Init(ctx, src)) {
                return false;
            }
        }

        return TAggregationFactory::DoInit(ctx, src);
    }

    std::multimap<TString, TPair> TopFreqs;
    TPair TopFreqFactoryParams;
};

TAggregationPtr BuildTopFreqFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TTopFreqFactory(pos, name, factory, aggMode);
}

template <bool HasKey>
class TTopAggregationFactory final : public TAggregationFactory {
public:
    TTopAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource(BuildFakeSource(pos))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 1 : (HasKey ? 3 : 2);
        if (exprs.size() != adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name << " requires "
                << adjustArgsCount << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Payload = exprs[0];
            if (HasKey) {
                Key = exprs[1];
            }
        }

        Count = exprs.back();

        if (!isFactory) {
            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTopAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        TNodePtr apply;
        if (HasKey) {
            apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Key), BuildLambda(Pos, Y("row"), Payload));
        } else {
            apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Payload));
        }
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Count);
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        if (HasKey) {
            return {1u, 0u};
        } else {
            return {0u};
        }
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        ctx.PushBlockShortcuts();
        if (!Count->Init(ctx, FakeSource.Get())) {
            return false;
        }
        Count = ctx.GroundBlockShortcutsForExpr(Count);

        if (!Payload) {
            return true;
        }

        if (HasKey) {
            ctx.PushBlockShortcuts();
            if (!Key->Init(ctx, src)) {
                return false;
            }
            Key = ctx.GroundBlockShortcutsForExpr(Key);
        }

        ctx.PushBlockShortcuts();
        if (!Payload->Init(ctx, src)) {
            return false;
        }
        Payload = ctx.GroundBlockShortcutsForExpr(Payload);

        if ((HasKey && Key->IsAggregated()) || (!HasKey && Payload->IsAggregated())) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TSourcePtr FakeSource;
    TNodePtr Key, Payload, Count;
};

template <bool HasKey>
TAggregationPtr BuildTopFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TTopAggregationFactory<HasKey>(pos, name, factory, aggMode);
}

template TAggregationPtr BuildTopFactoryAggregation<false>(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
template TAggregationPtr BuildTopFactoryAggregation<true >(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);

class TCountDistinctEstimateAggregationFactory final : public TAggregationFactory {
public:
    TCountDistinctEstimateAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        if (exprs.size() < adjustArgsCount || exprs.size() > 1 + adjustArgsCount) {
            ctx.Error(Pos) << Name << " aggregation function " << (isFactory ? "factory " : "") << " requires " <<
                adjustArgsCount << " or " << (1 + adjustArgsCount) << " argument(s), given: " << exprs.size();
            return false;
        }

        Precision = 14;
        if (1 + adjustArgsCount <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            if (!Parseui32(exprs[adjustArgsCount], Precision)) {
                ctx.Error(posSecondArg) << Name << ": invalid argument, numeric literal is expected";
                return false;
            }
        }
        if (Precision > 18 || Precision < 4) {
            ctx.Error(Pos) << Name << ": precision is expected to be between 4 and 18 (inclusive), got " << Precision;
            return false;
        }

        if (!isFactory) {
            Expr = exprs[0];
            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TCountDistinctEstimateAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Expr));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Y("Uint32", Q(ToString(Precision))));
    }

private:
    ui32 Precision = 0;
};

TAggregationPtr BuildCountDistinctEstimateFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TCountDistinctEstimateAggregationFactory(pos, name, factory, aggMode);
}

class TListAggregationFactory final : public TAggregationFactory {
public:
    TListAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        ui32 minArgs = (0 + adjustArgsCount);
        ui32 maxArgs = (1 + adjustArgsCount);
        if (exprs.size() < minArgs || exprs.size() > maxArgs) {
            ctx.Error(Pos) << "List aggregation " << (isFactory ? "factory " : "") << "function require " << minArgs
                << " or " << maxArgs << " arguments, given: " << exprs.size();
            return false;
        }

        Limit = 0;
        if (adjustArgsCount + 1U <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            if (!Parseui32(exprs[adjustArgsCount], Limit)) {
                ctx.Error(posSecondArg) << "List: invalid last argument, numeric literal is expected";
                return false;
            }
        }

        if (!isFactory) {
            Expr = exprs[0];
            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TListAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Expr));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Y("Uint64", Q(ToString(Limit))));
    }

private:
    ui32 Limit = 0;
};

TAggregationPtr BuildListFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TListAggregationFactory(pos, name, factory, aggMode);
}

class TUserDefinedAggregationFactory final : public TAggregationFactory {
public:
    TUserDefinedAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        if (exprs.size() < (3 + adjustArgsCount) || exprs.size() > (7 + adjustArgsCount)) {
            ctx.Error(Pos) << "User defined aggregation function " << (isFactory ? "factory " : "") << " requires " <<
                (3 + adjustArgsCount) << " to " << (7 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }

        Lambdas[0] = BuildLambda(Pos, Y("value", "parent"), Y("NamedApply", exprs[adjustArgsCount], Q(Y("value")), Y("AsStruct"), Y("DependsOn", "parent")));
        Lambdas[1] = BuildLambda(Pos, Y("value", "state", "parent"), Y("NamedApply", exprs[adjustArgsCount + 1], Q(Y("state", "value")), Y("AsStruct"), Y("DependsOn", "parent")));
        Lambdas[2] = BuildLambda(Pos, Y("one", "two"), Y("Apply", exprs[adjustArgsCount + 2], "one", "two"));

        for (size_t i = 3U; i < Lambdas.size(); ++i) {
            const auto j = adjustArgsCount + i;
            Lambdas[i] = BuildLambda(Pos, Y("state"), j >= exprs.size() ? AstNode("state") : Y("Apply", exprs[j], "state"));
        }

        DefVal = (exprs.size() == (7 + adjustArgsCount)) ? exprs[adjustArgsCount + 6] : Y("Null");
        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front()));
    }

    TNodePtr DoClone() const final {
        return new TUserDefinedAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), Expr));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Lambdas[0], Lambdas[1], Lambdas[2], Lambdas[3], Lambdas[4], Lambdas[5], DefVal);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (const auto& lambda : Lambdas) {
            if (!lambda->Init(ctx, src)) {
                return false;
            }
        }

        if (!DefVal->Init(ctx, src)) {
            return false;
        }

        return TAggregationFactory::DoInit(ctx, src);
    }

    std::array<TNodePtr, 6> Lambdas;
    TNodePtr DefVal;
};

TAggregationPtr BuildUserDefinedFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TUserDefinedAggregationFactory(pos, name, factory, aggMode);
}

class TCountAggregation final : public TAggregationFactory {
public:
    TCountAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, func, aggMode)
    {}

private:
    TNodePtr DoClone() const final {
        return new TCountAggregation(Pos, Name, Func, AggMode);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Expr) {
            return true;
        }

        if (Expr->IsAsterisk()) {
            Expr = Y("Void");
        }
        ctx.PushBlockShortcuts();
        if (!Expr->Init(ctx, src)) {
            return false;
        }
        Expr->SetCountHint(Expr->IsConstant());
        Expr = ctx.GroundBlockShortcutsForExpr(Expr);
        return TAggregationFactory::DoInit(ctx, src);
    }
};

TAggregationPtr BuildCountAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode) {
    return new TCountAggregation(pos, name, func, aggMode);
}

} // namespace NSQLTranslationV0
