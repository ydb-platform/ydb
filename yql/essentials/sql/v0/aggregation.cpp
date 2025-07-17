#include "node.h"
#include "context.h"

#include <yql/essentials/ast/yql_type_string.h>

#include <library/cpp/charset/ci_string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <array>

using namespace NYql;

namespace NSQLTranslationV0 {

class TAggregationFactory : public IAggregation {
public:
    TAggregationFactory(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi = false)
        : IAggregation(pos, name, func, aggMode), Factory_(!func.empty() ?
            BuildBind(Pos_, aggMode == EAggregateMode::OverWindow ? "window_module" : "aggregate_module", func) : nullptr),
        DynamicFactory_(!Factory_), Multi_(multi)
    {
        if (!Factory_) {
            FakeSource_ = BuildFakeSource(pos);
        }
    }

protected:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        ui32 expectedArgs = !Factory_ ? 2 : (isFactory ? 0 : 1);
        if (!Factory_) {
            YQL_ENSURE(!isFactory);
        }

        if (expectedArgs != exprs.size()) {
            ctx.Error(Pos_) << "Aggregation function " << (isFactory  ? "factory " : "") << Name_
                << " requires exactly " << expectedArgs << " argument(s), given: " << exprs.size();
            return false;
        }

        if (!Factory_) {
            Factory_ = exprs[1];
        }

        if (!isFactory) {
            Expr_ = exprs.front();
            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr AggregationTraitsFactory() const override {
        return Factory_;
    }

    TNodePtr GetApply(const TNodePtr& type) const override {
        if (!Multi_) {
            return Y("Apply", Factory_, (DynamicFactory_ ? Y("ListItemType", type) : type),
              BuildLambda(Pos_, Y("row"), Y("EnsurePersistable", Expr_)));
        }

        return Y("MultiAggregate",
            Y("ListItemType", type),
            BuildLambda(Pos_, Y("row"), Y("EnsurePersistable", Expr_)),
            Factory_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Expr_) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!Expr_->Init(ctx, src)) {
            return false;
        }
        if (Expr_->IsAggregated() && !Expr_->IsAggregationKey() && !IsOverWindow()) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden for non window functions";
            return false;
        }
        if (AggMode_ == EAggregateMode::Distinct) {
            const auto column = Expr_->GetColumnName();
            if (!column) {
                ctx.Error(Expr_->GetPos()) << "DISTINCT qualifier may only be used with column references";
                return false;
            }
            DistinctKey_ = *column;
            YQL_ENSURE(src);
            if (src->GetJoin()) {
                const auto sourcePtr = Expr_->GetSourceName();
                if (!sourcePtr || !*sourcePtr) {
                    if (!src->IsGroupByColumn(DistinctKey_)) {
                        ctx.Error(Expr_->GetPos()) << ErrorDistinctWithoutCorrelation(DistinctKey_);
                        return false;
                    }
                } else {
                    DistinctKey_ = DotJoin(*sourcePtr, DistinctKey_);
                }
            }
            if (src->IsGroupByColumn(DistinctKey_)) {
                ctx.Error(Expr_->GetPos()) << ErrorDistinctByGroupKey(DistinctKey_);
                return false;
            }
            Expr_ = AstNode("row");
            ctx.PopBlockShortcuts();
        } else {
            Expr_ = ctx.GroundBlockShortcutsForExpr(Expr_);
        }

        if (FakeSource_) {
            ctx.PushBlockShortcuts();
            if (!Factory_->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            Factory_ = ctx.GroundBlockShortcutsForExpr(Factory_);
            if (AggMode_ == EAggregateMode::OverWindow) {
                Factory_ = BuildLambda(Pos_, Y("type", "extractor"), Y("block", Q(Y(
                    Y("let", "x", Y("Apply", Factory_, "type", "extractor")),
                    Y("return", Y("WindowTraits",
                        Y("NthArg", Q("0"), "x"),
                        Y("NthArg", Q("1"), "x"),
                        Y("NthArg", Q("2"), "x"),
                        BuildLambda(Pos_, Y("value", "state"), Y("Void")),
                        Y("NthArg", Q("6"), "x"),
                        Y("NthArg", Q("7"), "x")
                    ))
                ))));
            }
        }

        return true;
    }

    TNodePtr Factory_;
    TSourcePtr FakeSource_;
    TNodePtr Expr_;
    bool DynamicFactory_;
    bool Multi_;
};

class TAggregationFactoryImpl final : public TAggregationFactory {
public:
    TAggregationFactoryImpl(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi)
        : TAggregationFactory(pos, name, func, aggMode, multi)
    {}

private:
    TNodePtr DoClone() const final {
        return new TAggregationFactoryImpl(Pos_, Name_, Func_, AggMode_, Multi_);
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
            ctx.Error(Pos_) << "Expected aggregation function: " << GetName() << " only as window function. You may have forgotten OVER instruction.";
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
                ctx.Warning(Pos_, TIssuesIds::YQL_AGGREGATE_BY_WIN_FUNC_WITHOUT_ORDER_BY) <<
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
        return new TAggregationFactoryWinAutoargImpl(Pos_, Name_, Func_, AggMode_);
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
            ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_ << " requires "
                << adjustArgsCount << " or " << (1 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Payload_ = exprs.front();
            Key_ = exprs[1];
        }

        Limit_ = (1 + adjustArgsCount == exprs.size() ? exprs.back() : Y("Void"));
        if (!isFactory) {
            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TKeyPayloadAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Key_), BuildLambda(Pos_, Y("row"), Payload_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Limit_);
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {1u, 0u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Key_) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!Key_->Init(ctx, src)) {
            return false;
        }
        Key_ = ctx.GroundBlockShortcutsForExpr(Key_);
        ctx.PushBlockShortcuts();
        if (!Payload_->Init(ctx, src)) {
            return false;
        }
        Payload_ = ctx.GroundBlockShortcutsForExpr(Payload_);
        if (!Limit_->Init(ctx, src)) {
            return false;
        }

        if (Key_->IsAggregated()) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TNodePtr Key_, Payload_, Limit_;
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
            ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_ << " requires " <<
                adjustArgsCount << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Payload_ = exprs.front();
            Predicate_ = exprs.back();

            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TPayloadPredicateAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        return Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Payload_), BuildLambda(Pos_, Y("row"), Predicate_));
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {0u, 1u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Predicate_) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!Predicate_->Init(ctx, src)) {
            return false;
        }
        Predicate_ = ctx.GroundBlockShortcutsForExpr(Predicate_);
        ctx.PushBlockShortcuts();
        if (!Payload_->Init(ctx, src)) {
            return false;
        }
        Payload_ = ctx.GroundBlockShortcutsForExpr(Payload_);

        if (Payload_->IsAggregated()) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
            return false;
        }

        return true;
    }

    TNodePtr Payload_, Predicate_;
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
            ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_ << " requires " <<
                adjustArgsCount << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            One_ = exprs.front();
            Two_ = exprs.back();

            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTwoArgsAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto tuple = Q(Y(One_, Two_));
        return Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), tuple));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!One_) {
            return true;
        }

        ctx.PushBlockShortcuts();
        if (!One_->Init(ctx, src)) {
            return false;
        }
        One_ = ctx.GroundBlockShortcutsForExpr(One_);
        ctx.PushBlockShortcuts();
        if (!Two_->Init(ctx, src)) {
            return false;
        }
        Two_ = ctx.GroundBlockShortcutsForExpr(Two_);

        if ((One_->IsAggregated() || Two_->IsAggregated()) && !IsOverWindow()) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TNodePtr One_, Two_;
};

TAggregationPtr BuildTwoArgsFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TTwoArgsAggregationFactory(pos, name, factory, aggMode);
}

class THistogramAggregationFactory final : public TAggregationFactory {
public:
    THistogramAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource_(BuildFakeSource(pos))
        , Weight_(Y("Double", Q("1.0")))
        , Intervals_(Y("Uint32", Q("100")))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        if (isFactory) {
            if (exprs.size() > 1) {
                ctx.Error(Pos_) << "Aggregation function factory " << Name_ << " requires 0 or 1 argument(s), given: " << exprs.size();
                return false;
            }
        } else {
            if (exprs.empty() || exprs.size() > 3) {
                ctx.Error(Pos_) << "Aggregation function " << Name_ << " requires one, two or three arguments, given: " << exprs.size();
                return false;
            }
        }

        if (!isFactory) {
            /// \todo: solve it with named arguments
            const auto integer = exprs.back()->IsIntegerLiteral();
            switch (exprs.size()) {
            case 2U:
                if (!integer) {
                    Weight_ = exprs.back();
                }
                break;
            case 3U:
                if (!integer) {
                    ctx.Error(Pos_) << "Aggregation function " << Name_ << " for case with 3 argument should have second interger argument";
                    return false;
                }
                Weight_ = exprs[1];
                break;
            }
            if (exprs.size() >= 2 && integer) {
                Intervals_ = Y("Cast", exprs.back(), Q("Uint32"));
            }
        } else {
            if (exprs.size() >= 1) {
                const auto integer = exprs.back()->IsIntegerLiteral();
                if (!integer) {
                    ctx.Error(Pos_) << "Aggregation function factory " << Name_ << " should have second interger argument";
                    return false;
                }

                Intervals_ = Y("Cast", exprs.back(), Q("Uint32"));
            }
        }

        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front()));
    }

    TNodePtr DoClone() const final {
        return new THistogramAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Expr_), BuildLambda(Pos_, Y("row"), Weight_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Intervals_);
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {0u, 1u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        ctx.PushBlockShortcuts();
        if (!Weight_->Init(ctx, src)) {
            return false;
        }
        Weight_ = ctx.GroundBlockShortcutsForExpr(Weight_);
        ctx.PushBlockShortcuts();
        if (!Intervals_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        Intervals_ = ctx.GroundBlockShortcutsForExpr(Intervals_);

        return TAggregationFactory::DoInit(ctx, src);
    }

    TSourcePtr FakeSource_;
    TNodePtr Weight_, Intervals_;
};

TAggregationPtr BuildHistogramFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new THistogramAggregationFactory(pos, name, factory, aggMode);
}

class TLinearHistogramAggregationFactory final : public TAggregationFactory {
public:
    TLinearHistogramAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource_(BuildFakeSource(pos))
        , BinSize_(Y("Double", Q("10.0")))
        , Minimum_(Y("Double", Q(ToString(-1.0 * Max<double>()))))
        , Maximum_(Y("Double", Q(ToString(Max<double>()))))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        Y_UNUSED(isFactory);
        if (exprs.empty() || exprs.size() > 4) {
            ctx.Error(Pos_) << "Aggregation function " << Name_ << " requires one to four arguments, given: " << exprs.size();
            return false;
        }

        if (exprs.size() > 1) {
            BinSize_ = exprs[1];
        }

        if (exprs.size() > 2) {
            Minimum_ = exprs[2];
        }

        if (exprs.size() > 3) {
            Maximum_ = exprs[3];
        }

        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, { exprs.front() });
    }

    TNodePtr DoClone() const final {
        return new TLinearHistogramAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        return Y("Apply", Factory_, type,
            BuildLambda(Pos_, Y("row"), Expr_),
            BinSize_, Minimum_, Maximum_);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        ctx.PushBlockShortcuts();
        if (!BinSize_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        BinSize_ = ctx.GroundBlockShortcutsForExpr(BinSize_);
        ctx.PushBlockShortcuts();
        if (!Minimum_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        Minimum_ = ctx.GroundBlockShortcutsForExpr(Minimum_);
        ctx.PushBlockShortcuts();
        if (!Maximum_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        Maximum_ = ctx.GroundBlockShortcutsForExpr(Maximum_);

        return TAggregationFactory::DoInit(ctx, src);
    }

    TSourcePtr FakeSource_;
    TNodePtr BinSize_, Minimum_, Maximum_;
};

TAggregationPtr BuildLinearHistogramFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TLinearHistogramAggregationFactory(pos, name, factory, aggMode);
}

class TPercentileFactory final : public TAggregationFactory {
public:
    TPercentileFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource_(BuildFakeSource(pos))
    {}

private:
    const TString* GetGenericKey() const final {
        return Column_;
    }

    void Join(IAggregation* aggr) final {
        const auto percentile = dynamic_cast<TPercentileFactory*>(aggr);
        Y_ABORT_UNLESS(percentile);
        Y_ABORT_UNLESS(*Column_ == *percentile->Column_);
        Y_ABORT_UNLESS(AggMode_ == percentile->AggMode_);
        Percentiles_.insert(percentile->Percentiles_.cbegin(), percentile->Percentiles_.cend());
        percentile->Percentiles_.clear();
    }

    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        if (exprs.size() < 0 + adjustArgsCount  || exprs.size() > 1 + adjustArgsCount) {
            ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_ << " requires "
                << (0 + adjustArgsCount) << " or " << (1 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Column_ = exprs.front()->GetColumnName();
            if (!Column_) {
                ctx.Error(Pos_) << Name_ << " may only be used with column reference as first argument.";
                return false;
            }
        }

        if (!TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front())))
            return false;

        TNodePtr x;
        if (1 + adjustArgsCount == exprs.size()) {
            x = exprs.back();
            ctx.PushBlockShortcuts();
            if (!x->Init(ctx, FakeSource_.Get())) {
                return false;
            }
            x = ctx.GroundBlockShortcutsForExpr(x);
        } else {
            x = Y("Double", Q("0.5"));
        }

        if (isFactory) {
            FactoryPercentile_ = x;
        } else {
            Percentiles_.emplace(Name_, x);
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TPercentileFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        TNodePtr percentiles(Percentiles_.cbegin()->second);

        if (Percentiles_.size() > 1U) {
            percentiles = Y();
            for (const auto& percentile : Percentiles_) {
                percentiles = L(percentiles, percentile.second);
            }
            percentiles = Q(percentiles);
        }

        return Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Expr_), percentiles);
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, FactoryPercentile_);
    }

    TNodePtr AggregationTraits(const TNodePtr& type) const final {
        if (Percentiles_.empty())
            return TNodePtr();

        TNodePtr names(Q(Percentiles_.cbegin()->first));

        if (Percentiles_.size() > 1U) {
            names = Y();
            for (const auto& percentile : Percentiles_)
                names = L(names, Q(percentile.first));
            names = Q(names);
        }

        const bool distinct = AggMode_ == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos_, DistinctKey_))) : type;
        return distinct ? Q(Y(names, GetApply(listType), BuildQuotedAtom(Pos_, DistinctKey_))) : Q(Y(names, GetApply(listType)));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (const auto& p : Percentiles_) {
            if (!p.second->Init(ctx, src)) {
                return false;
            }
        }

        return TAggregationFactory::DoInit(ctx, src);
    }

    TSourcePtr FakeSource_;
    std::multimap<TString, TNodePtr> Percentiles_;
    TNodePtr FactoryPercentile_;
    const TString* Column_ = nullptr;
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
            ctx.Error(Pos_) << "Aggregation function " << (isFactory? "factory " : "") << Name_ <<
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
            TopFreqFactoryParams_ = x;
        } else {
            TopFreqs_.emplace(Name_, x);
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTopFreqFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        TPair topFreqs(TopFreqs_.cbegin()->second);

        if (TopFreqs_.size() > 1U) {
            topFreqs = { Y(), Y() };
            for (const auto& topFreq : TopFreqs_) {
                topFreqs = { L(topFreqs.first, topFreq.second.first), L(topFreqs.second, topFreq.second.second) };
            }
            topFreqs = { Q(topFreqs.first), Q(topFreqs.second) };
        }

        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Expr_), topFreqs.first, topFreqs.second);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, TopFreqFactoryParams_.first, TopFreqFactoryParams_.second);
    }

    TNodePtr AggregationTraits(const TNodePtr& type) const final {
        if (TopFreqs_.empty())
            return TNodePtr();

        TNodePtr names(Q(TopFreqs_.cbegin()->first));

        if (TopFreqs_.size() > 1U) {
            names = Y();
            for (const auto& topFreq : TopFreqs_)
                names = L(names, Q(topFreq.first));
            names = Q(names);
        }

        const bool distinct = AggMode_ == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos_, DistinctKey_))) : type;
        return distinct ? Q(Y(names, GetApply(listType), BuildQuotedAtom(Pos_, DistinctKey_))) : Q(Y(names, GetApply(listType)));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (const auto& topFreq : TopFreqs_) {
            if (!topFreq.second.first->Init(ctx, src)) {
                return false;
            }

            if (!topFreq.second.second->Init(ctx, src)) {
                return false;
            }
        }

        return TAggregationFactory::DoInit(ctx, src);
    }

    std::multimap<TString, TPair> TopFreqs_;
    TPair TopFreqFactoryParams_;
};

TAggregationPtr BuildTopFreqFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TTopFreqFactory(pos, name, factory, aggMode);
}

template <bool HasKey>
class TTopAggregationFactory final : public TAggregationFactory {
public:
    TTopAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource_(BuildFakeSource(pos))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 1 : (HasKey ? 3 : 2);
        if (exprs.size() != adjustArgsCount) {
            ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_ << " requires "
                << adjustArgsCount << " arguments, given: " << exprs.size();
            return false;
        }

        if (!isFactory) {
            Payload_ = exprs[0];
            if (HasKey) {
                Key_ = exprs[1];
            }
        }

        Count_ = exprs.back();

        if (!isFactory) {
            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTopAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        TNodePtr apply;
        if (HasKey) {
            apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Key_), BuildLambda(Pos_, Y("row"), Payload_));
        } else {
            apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Payload_));
        }
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Count_);
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
        if (!Count_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        Count_ = ctx.GroundBlockShortcutsForExpr(Count_);

        if (!Payload_) {
            return true;
        }

        if (HasKey) {
            ctx.PushBlockShortcuts();
            if (!Key_->Init(ctx, src)) {
                return false;
            }
            Key_ = ctx.GroundBlockShortcutsForExpr(Key_);
        }

        ctx.PushBlockShortcuts();
        if (!Payload_->Init(ctx, src)) {
            return false;
        }
        Payload_ = ctx.GroundBlockShortcutsForExpr(Payload_);

        if ((HasKey && Key_->IsAggregated()) || (!HasKey && Payload_->IsAggregated())) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TSourcePtr FakeSource_;
    TNodePtr Key_, Payload_, Count_;
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
            ctx.Error(Pos_) << Name_ << " aggregation function " << (isFactory ? "factory " : "") << " requires " <<
                adjustArgsCount << " or " << (1 + adjustArgsCount) << " argument(s), given: " << exprs.size();
            return false;
        }

        Precision_ = 14;
        if (1 + adjustArgsCount <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            if (!Parseui32(exprs[adjustArgsCount], Precision_)) {
                ctx.Error(posSecondArg) << Name_ << ": invalid argument, numeric literal is expected";
                return false;
            }
        }
        if (Precision_ > 18 || Precision_ < 4) {
            ctx.Error(Pos_) << Name_ << ": precision is expected to be between 4 and 18 (inclusive), got " << Precision_;
            return false;
        }

        if (!isFactory) {
            Expr_ = exprs[0];
            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TCountDistinctEstimateAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Expr_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Y("Uint32", Q(ToString(Precision_))));
    }

private:
    ui32 Precision_ = 0;
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
            ctx.Error(Pos_) << "List aggregation " << (isFactory ? "factory " : "") << "function require " << minArgs
                << " or " << maxArgs << " arguments, given: " << exprs.size();
            return false;
        }

        Limit_ = 0;
        if (adjustArgsCount + 1U <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            if (!Parseui32(exprs[adjustArgsCount], Limit_)) {
                ctx.Error(posSecondArg) << "List: invalid last argument, numeric literal is expected";
                return false;
            }
        }

        if (!isFactory) {
            Expr_ = exprs[0];
            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TListAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Expr_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Y("Uint64", Q(ToString(Limit_))));
    }

private:
    ui32 Limit_ = 0;
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
            ctx.Error(Pos_) << "User defined aggregation function " << (isFactory ? "factory " : "") << " requires " <<
                (3 + adjustArgsCount) << " to " << (7 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }

        Lambdas_[0] = BuildLambda(Pos_, Y("value", "parent"), Y("NamedApply", exprs[adjustArgsCount], Q(Y("value")), Y("AsStruct"), Y("DependsOn", "parent")));
        Lambdas_[1] = BuildLambda(Pos_, Y("value", "state", "parent"), Y("NamedApply", exprs[adjustArgsCount + 1], Q(Y("state", "value")), Y("AsStruct"), Y("DependsOn", "parent")));
        Lambdas_[2] = BuildLambda(Pos_, Y("one", "two"), Y("Apply", exprs[adjustArgsCount + 2], "one", "two"));

        for (size_t i = 3U; i < Lambdas_.size(); ++i) {
            const auto j = adjustArgsCount + i;
            Lambdas_[i] = BuildLambda(Pos_, Y("state"), j >= exprs.size() ? AstNode("state") : Y("Apply", exprs[j], "state"));
        }

        DefVal_ = (exprs.size() == (7 + adjustArgsCount)) ? exprs[adjustArgsCount + 6] : Y("Null");
        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front()));
    }

    TNodePtr DoClone() const final {
        return new TUserDefinedAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type) const final {
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), Expr_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Lambdas_[0], Lambdas_[1], Lambdas_[2], Lambdas_[3], Lambdas_[4], Lambdas_[5], DefVal_);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (const auto& lambda : Lambdas_) {
            if (!lambda->Init(ctx, src)) {
                return false;
            }
        }

        if (!DefVal_->Init(ctx, src)) {
            return false;
        }

        return TAggregationFactory::DoInit(ctx, src);
    }

    std::array<TNodePtr, 6> Lambdas_;
    TNodePtr DefVal_;
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
        return new TCountAggregation(Pos_, Name_, Func_, AggMode_);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Expr_) {
            return true;
        }

        if (Expr_->IsAsterisk()) {
            Expr_ = Y("Void");
        }
        ctx.PushBlockShortcuts();
        if (!Expr_->Init(ctx, src)) {
            return false;
        }
        Expr_->SetCountHint(Expr_->IsConstant());
        Expr_ = ctx.GroundBlockShortcutsForExpr(Expr_);
        return TAggregationFactory::DoInit(ctx, src);
    }
};

TAggregationPtr BuildCountAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode) {
    return new TCountAggregation(pos, name, func, aggMode);
}

} // namespace NSQLTranslationV0
