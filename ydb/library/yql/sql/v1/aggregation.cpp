#include "node.h"
#include "source.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_type_string.h>

#include <library/cpp/charset/ci_string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <array>

using namespace NYql;

namespace NSQLTranslationV1 {

namespace {
    bool BlockWindowAggregationWithoutFrameSpec(TPosition pos, TStringBuf name, ISource* src, TContext& ctx) {
        if (src) {
            auto winNamePtr = src->GetWindowName();
            if (winNamePtr) {
                auto winSpecPtr = src->FindWindowSpecification(ctx, *winNamePtr);
                if (!winSpecPtr) {
                    ctx.Error(pos) << "Failed to use aggregation function " << name << " without window specification or in wrong place";
                    return true;
                }
            }
        }
        return false;
    }

    bool ShouldEmitAggApply(const TContext& ctx) {
        const bool blockEngineEnabled = ctx.BlockEngineEnable || ctx.BlockEngineForce;
        return ctx.EmitAggApply.GetOrElse(blockEngineEnabled);
    }
}

static const THashSet<TString> AggApplyFuncs = {
    "count_traits_factory",
    "sum_traits_factory",
    "avg_traits_factory",
    "min_traits_factory",
    "max_traits_factory",
    "some_traits_factory",
};

class TAggregationFactory : public IAggregation {
public:
    TAggregationFactory(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode,
        bool multi = false, bool validateArgs = true)
        : IAggregation(pos, name, func, aggMode), Factory(!func.empty() ?
            BuildBind(Pos, aggMode == EAggregateMode::OverWindow ? "window_module" : "aggregate_module", func) : nullptr),
        Multi(multi), ValidateArgs(validateArgs), DynamicFactory(!Factory)
    {
        if (aggMode != EAggregateMode::OverWindow && !func.empty() && AggApplyFuncs.contains(func)) {
            AggApplyName = func.substr(0, func.size() - 15);
        }

        if (!Factory) {
            FakeSource = BuildFakeSource(pos);
        }
    }

protected:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        if (!ShouldEmitAggApply(ctx)) {
            AggApplyName = "";
        }

        if (ValidateArgs || isFactory) {
            ui32 expectedArgs = ValidateArgs && !Factory ? 2 : (isFactory ? 0 : 1);
            if (!Factory && ValidateArgs) {
                YQL_ENSURE(!isFactory);
            }

            if (expectedArgs != exprs.size()) {
                ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name
                    << " requires exactly " << expectedArgs << " argument(s), given: " << exprs.size();
                return false;
            }
        }

        if (!ValidateArgs) {
            Exprs = exprs;
        }

        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
            return false;
        }

        if (ValidateArgs) {
            if (!Factory) {
                Factory = exprs[1];
            }
        }

        if (!isFactory) {
            if (ValidateArgs) {
                Expr = exprs.front();
            }

            Name = src->MakeLocalName(Name);
        }

        if (Expr && Expr->IsAsterisk() && AggApplyName == "count") {
            AggApplyName = "count_all";
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr AggregationTraitsFactory() const override {
        return Factory;
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const override {
        Y_UNUSED(ctx);
        return BuildLambda(Pos, Y("row"), Y("PersistableRepr", many ? Y("Unwrap", Expr) : Expr));
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const override {
        auto extractor = GetExtractor(many, ctx);
        if (!extractor) {
            return nullptr;
        }

        if (!Multi) {
            if (!DynamicFactory && allowAggApply && !AggApplyName.empty()) {
                return Y("AggApply", Q(AggApplyName), Y("ListItemType", type), extractor);
            }

            return Y("Apply", Factory, (DynamicFactory ? Y("ListItemType", type) : type),
              extractor);
        }

        return Y("MultiAggregate",
            Y("ListItemType", type),
            extractor,
            Factory);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArgs) {
            for (auto x : Exprs) {
                if (!x->Init(ctx, src)) {
                    return false;
                }
                if (x->IsAggregated() && !x->IsAggregationKey() && !IsOverWindow()) {
                    ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
                    return false;
                }
            }

            return true;
        }

        if (!Expr) {
            return true;
        }

        if (!Expr->Init(ctx, src)) {
            return false;
        }
        if (Expr->IsAggregated() && !Expr->IsAggregationKey() && !IsOverWindow()) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        if (AggMode == EAggregateMode::Distinct) {
            const auto column = Expr->GetColumnName();
            if (!column) {
                // TODO: improve TBasicAggrFunc::CollectPreaggregateExprs()
                ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
                return false;
            }
            DistinctKey = *column;
            YQL_ENSURE(src);
            if (!IsGeneratedKeyColumn && src->GetJoin()) {
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
        }

        if (FakeSource) {
            if (!Factory->Init(ctx, FakeSource.Get())) {
                return false;
            }

            if (AggMode == EAggregateMode::OverWindow) {
                Factory = BuildLambda(Pos, Y("type", "extractor"), Y("block", Q(Y(
                    Y("let", "x", Y("Apply", Factory, "type", "extractor")),
                    Y("return", Y("ToWindowTraits", "x"))
                ))));
            }
        }

        return true;
    }

    TNodePtr Factory;
    TNodePtr Expr;
    bool Multi;
    bool ValidateArgs;
    TString AggApplyName;
    TVector<TNodePtr> Exprs;

private:
    TSourcePtr FakeSource;
    bool DynamicFactory;
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

TAggregationPtr BuildFactoryAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi) {
    return new TAggregationFactoryImpl(pos, name, func, aggMode, multi);
}

class TKeyPayloadAggregationFactory final : public TAggregationFactory {
public:
    TKeyPayloadAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource(BuildFakeSource(pos))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 2;
        if (exprs.size() < adjustArgsCount || exprs.size() > 1 + adjustArgsCount) {
            ctx.Error(Pos) << "Aggregation function " << (isFactory ? "factory " : "") << Name << " requires "
                << adjustArgsCount << " or " << (1 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }
        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
            return false;
        }

        if (!isFactory) {
            Payload = exprs.front();
            Key = exprs[1];
        }

        if (1 + adjustArgsCount == exprs.size()) {
            Limit = exprs.back();
            Func += "2";
        } else {
            Func += "1";
        }

        if (Factory) {
            Factory = BuildBind(Pos, AggMode == EAggregateMode::OverWindow ? "window_module" : "aggregate_module", Func);
        }

        if (!isFactory) {
            Name = src->MakeLocalName(Name);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name));
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TKeyPayloadAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const final {
        Y_UNUSED(ctx);
        return BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Payload) : Payload);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory, type,
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Key) : Key),
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Payload) : Payload));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        if (Limit) {
            apply = L(apply, Limit);
        }
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {1u, 0u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (Limit) {
            if (!Limit->Init(ctx, FakeSource.Get())) {
                return false;
            }
        }

        if (!Key) {
            return true;
        }

        if (!Key->Init(ctx, src)) {
            return false;
        }
        if (!Payload->Init(ctx, src)) {
            return false;
        }

        if (Key->IsAggregated()) {
            ctx.Error(Pos) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TSourcePtr FakeSource;
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

        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TPayloadPredicateAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const final {
        Y_UNUSED(ctx);
        return BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Payload) : Payload);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        return Y("Apply", Factory, type,
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Payload) : Payload),
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Predicate) : Predicate));
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {0u, 1u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Predicate) {
            return true;
        }

        if (!Predicate->Init(ctx, src)) {
            return false;
        }
        if (!Payload->Init(ctx, src)) {
            return false;
        }

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

        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTwoArgsAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const final {
        Y_UNUSED(ctx);
        return BuildLambda(Pos, Y("row"), many ? Y("Unwrap", One) : One);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto tuple = Q(Y(One, Two));
        return Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", tuple) : tuple));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!One) {
            return true;
        }

        if (!One->Init(ctx, src)) {
            return false;
        }
        if (!Two->Init(ctx, src)) {
            return false;
        }

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
                    ctx.Error(Pos) << "Aggregation function " << Name << " for case with 3 arguments should have third argument of integer type";
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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory, type,
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr),
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Weight) : Weight));
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
        if (!Weight->Init(ctx, src)) {
            return false;
        }
        if (!Intervals->Init(ctx, FakeSource.Get())) {
            return false;
        }

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
        if (isFactory) {
            if (exprs.size() > 3) {
                ctx.Error(Pos) << "Aggregation function " << Name << " requires zero to three arguments, given: " << exprs.size();
                return false;
            }
        } else {
            if (exprs.empty() || exprs.size() > 4) {
                ctx.Error(Pos) << "Aggregation function " << Name << " requires one to four arguments, given: " << exprs.size();
                return false;
            }
        }

        if (exprs.size() > 1 - isFactory) {
            BinSize = exprs[1 - isFactory];
        }

        if (exprs.size() > 2 - isFactory) {
            Minimum = exprs[2 - isFactory];
        }

        if (exprs.size() > 3 - isFactory) {
            Maximum = exprs[3 - isFactory];
        }

        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front()));
    }

    TNodePtr DoClone() const final {
        return new TLinearHistogramAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        return Y("Apply", Factory, type,
            BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr),
            BinSize, Minimum, Maximum);
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, BinSize, Minimum, Maximum);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!BinSize->Init(ctx, FakeSource.Get())) {
            return false;
        }
        if (!Minimum->Init(ctx, FakeSource.Get())) {
            return false;
        }
        if (!Maximum->Init(ctx, FakeSource.Get())) {
            return false;
        }

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
        YQL_ENSURE(percentile);
        YQL_ENSURE(Column && percentile->Column && *Column == *percentile->Column);
        YQL_ENSURE(AggMode == percentile->AggMode);
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
        }

        if (!TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front())))
            return false;

        TNodePtr x;
        if (1 + adjustArgsCount == exprs.size()) {
            x = exprs.back();
            if (!x->Init(ctx, FakeSource.Get())) {
                return false;
            }
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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        TNodePtr percentiles(Percentiles.cbegin()->second);

        if (Percentiles.size() > 1U) {
            percentiles = Y();
            for (const auto& percentile : Percentiles) {
                percentiles = L(percentiles, percentile.second);
            }
            percentiles = Q(percentiles);
        }

        return Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr), percentiles);
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, FactoryPercentile);
    }

    std::pair<TNodePtr, bool> AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const final {
        if (Percentiles.empty())
            return { TNodePtr(), true };

        TNodePtr names(Q(Percentiles.cbegin()->first));

        if (Percentiles.size() > 1U) {
            names = Y();
            for (const auto& percentile : Percentiles)
                names = L(names, Q(percentile.first));
            names = Q(names);
        }

        const bool distinct = AggMode == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos, DistinctKey))) : type;
        auto apply = GetApply(listType, many, allowAggApply, ctx);
        if (!apply) {
            return { TNodePtr(), false };
        }

        auto wrapped = WrapIfOverState(apply, overState, many, ctx);
        if (!wrapped) {
            return { TNodePtr(), false };
        }

        return { distinct ?
            Q(Y(names, wrapped, BuildQuotedAtom(Pos, DistinctKey))) :
            Q(Y(names, wrapped)), true };
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
        , FakeSource(BuildFakeSource(pos))
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

        TNodePtr n = Y("Null");
        TNodePtr buffer = Y("Null");

        if (1 + adjustArgsCount <= exprs.size()) {
            n = exprs[adjustArgsCount];
            if (!n->Init(ctx, FakeSource.Get())) {
                return false;
            }
            n = Y("SafeCast", n, Q("Uint32"));
        }

        n = Y("Coalesce", n, Y("Uint32", Q("1")));
        if (2 + adjustArgsCount == exprs.size()) {
            buffer = exprs[1 + adjustArgsCount];
            if (!buffer->Init(ctx, FakeSource.Get())) {
                return false;
            }

            buffer = Y("SafeCast", buffer, Q("Uint32"));
        }

        buffer = Y("Coalesce", buffer, Y("SafeCast", Y("*", n, Y("Double", Q(ToString(DefaultBufferC)))), Q("Uint32")));
        buffer = Y("Coalesce", buffer, Y("Uint32", Q(ToString(MinBuffer))));
        buffer = Y("Max", buffer, Y("Uint32", Q(ToString(MinBuffer))));

        auto x = TPair{ n, buffer };
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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        TPair topFreqs(TopFreqs.cbegin()->second);

        if (TopFreqs.size() > 1U) {
            topFreqs = { Y(), Y() };
            for (const auto& topFreq : TopFreqs) {
                topFreqs = { L(topFreqs.first, topFreq.second.first), L(topFreqs.second, topFreq.second.second) };
            }
            topFreqs = { Q(topFreqs.first), Q(topFreqs.second) };
        }

        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr), topFreqs.first, topFreqs.second);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, TopFreqFactoryParams.first, TopFreqFactoryParams.second);
    }

    std::pair<TNodePtr, bool> AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const final {
        if (TopFreqs.empty())
            return { TNodePtr(), true };

        TNodePtr names(Q(TopFreqs.cbegin()->first));

        if (TopFreqs.size() > 1U) {
            names = Y();
            for (const auto& topFreq : TopFreqs)
                names = L(names, Q(topFreq.first));
            names = Q(names);
        }

        const bool distinct = AggMode == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos, DistinctKey))) : type;
        auto apply = GetApply(listType, many, allowAggApply, ctx);
        if (!apply) {
            return { nullptr, false };
        }

        auto wrapped = WrapIfOverState(apply, overState, many, ctx);
        if (!wrapped) {
            return { nullptr, false };
        }

        return { distinct ?
            Q(Y(names, wrapped, BuildQuotedAtom(Pos, DistinctKey))) :
            Q(Y(names, wrapped)), true };
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
    TSourcePtr FakeSource;
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

        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTopAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        TNodePtr apply;
        if (HasKey) {
            apply = Y("Apply", Factory, type,
                BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Key) : Key),
                BuildLambda(Pos, Y("row"), many ? Y("Payload", Payload) : Payload));
        } else {
            apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Payload) : Payload));
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
        if (!Count->Init(ctx, FakeSource.Get())) {
            return false;
        }

        if (!Payload) {
            return true;
        }

        if (HasKey) {
            if (!Key->Init(ctx, src)) {
                return false;
            }
        }

        if (!Payload->Init(ctx, src)) {
            return false;
        }

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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TCountDistinctEstimateAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr));
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
        , FakeSource(BuildFakeSource(pos))
    {
    }

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

        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
            return false;
        }

        Limit = nullptr;
        if (adjustArgsCount + 1U <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            Limit = exprs[adjustArgsCount];
            if (!Limit->Init(ctx, FakeSource.Get())) {
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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TListAggregationFactory(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        if (!Limit) {
            apply = L(apply, Y("Uint64", Q("0")));
        } else {
            apply = L(apply, Limit);
        }
    }

private:
    TSourcePtr FakeSource;
    TNodePtr Limit;
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
        Lambdas[2] = BuildLambda(Pos, Y("one", "two"), Y("IfType", exprs[adjustArgsCount + 2], Y("NullType"),
            BuildLambda(Pos, Y(), Y("Void")),
            BuildLambda(Pos, Y(), Y("Apply", exprs[adjustArgsCount + 2], "one", "two"))));

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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr));
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
        if (!Expr->Init(ctx, src)) {
            return false;
        }
        Expr->SetCountHint(Expr->IsConstant());
        return TAggregationFactory::DoInit(ctx, src);
    }
};

TAggregationPtr BuildCountAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode) {
    return new TCountAggregation(pos, name, func, aggMode);
}

class TPGFactoryAggregation final : public TAggregationFactory {
public:
    TPGFactoryAggregation(TPosition pos, const TString& name, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, "", aggMode, false, false)
        , PgFunc(Name)
    {}

    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        auto ret = TAggregationFactory::InitAggr(ctx, isFactory, src, node, exprs);
        if (ret) {
            if (isFactory) {
                Factory = BuildLambda(Pos, Y("type", "extractor"), Y(AggMode == EAggregateMode::OverWindow ? "PgWindowTraitsTuple" : "PgAggregationTraitsTuple",
                    Q(PgFunc), Y("ListItemType", "type"), "extractor"));
            } else {
                Lambda = BuildLambda(Pos, Y("row"), exprs);
            }
        }

        return ret;
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const override {
        Y_UNUSED(many);
        ctx.Error() << "Partial aggregation by PostgreSQL function isn't supported";
        return nullptr;
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(many);
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        if (ShouldEmitAggApply(ctx) && allowAggApply && AggMode != EAggregateMode::OverWindow) {
            return Y("AggApply",
                Q("pg_" + to_lower(PgFunc)), Y("ListItemType", type), Lambda);
        }

        return Y(AggMode == EAggregateMode::OverWindow ? "PgWindowTraits" : "PgAggregationTraits",
            Q(PgFunc), Y("ListItemType", type), Lambda);
    }

private:
    TNodePtr DoClone() const final {
        return new TPGFactoryAggregation(Pos, Name, AggMode);
    }

    TString PgFunc;
    TNodePtr Lambda;
};

TAggregationPtr BuildPGFactoryAggregation(TPosition pos, const TString& name, EAggregateMode aggMode) {
    return new TPGFactoryAggregation(pos, name, aggMode);
}

class TNthValueFactoryAggregation final : public TAggregationFactory {
public:
public:
    TNthValueFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource(BuildFakeSource(pos))
    {
    }

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        ui32 expectedArgs = (1 + adjustArgsCount);
        if (exprs.size() != expectedArgs) {
            ctx.Error(Pos) << "NthValue aggregation " << (isFactory ? "factory " : "") << "function require "
                << expectedArgs << " arguments, given: " << exprs.size();
            return false;
        }

        if (BlockWindowAggregationWithoutFrameSpec(Pos, GetName(), src, ctx)) {
            return false;
        }

        Index = exprs[adjustArgsCount];
        if (!Index->Init(ctx, FakeSource.Get())) {
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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TNthValueFactoryAggregation(Pos, Name, Func, AggMode);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory, type, BuildLambda(Pos, Y("row"), many ? Y("Unwrap", Expr) : Expr));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Index);
    }

private:
    TSourcePtr FakeSource;
    TNodePtr Index;
};

TAggregationPtr BuildNthFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TNthValueFactoryAggregation(pos, name, factory, aggMode);
}

} // namespace NSQLTranslationV1
