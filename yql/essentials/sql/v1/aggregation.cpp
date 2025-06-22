#include "node.h"
#include "source.h"
#include "context.h"

#include <yql/essentials/ast/yql_type_string.h>

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
        : IAggregation(pos, name, func, aggMode), Factory_(!func.empty() ?
            BuildBind(Pos_, aggMode == EAggregateMode::OverWindow || aggMode == EAggregateMode::OverWindowDistinct ? "window_module" : "aggregate_module", func) : nullptr),
        Multi_(multi), ValidateArgs_(validateArgs), DynamicFactory_(!Factory_)
    {
        if (aggMode != EAggregateMode::OverWindow && aggMode != EAggregateMode::OverWindowDistinct && !func.empty() && AggApplyFuncs.contains(func)) {
            AggApplyName_ = func.substr(0, func.size() - 15);
        }

        if (!Factory_) {
            FakeSource_ = BuildFakeSource(pos);
        }
    }

protected:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        if (!ShouldEmitAggApply(ctx)) {
            AggApplyName_ = "";
        }

        if (ValidateArgs_ || isFactory) {
            ui32 expectedArgs = ValidateArgs_ && !Factory_ ? 2 : (isFactory ? 0 : 1);
            if (!Factory_ && ValidateArgs_) {
                YQL_ENSURE(!isFactory);
            }

            if (expectedArgs != exprs.size()) {
                ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_
                    << " requires exactly " << expectedArgs << " argument(s), given: " << exprs.size();
                return false;
            }
        }

        if (!ValidateArgs_) {
            Exprs_ = exprs;
        }

        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
            return false;
        }

        if (ValidateArgs_) {
            if (!Factory_) {
                Factory_ = exprs[1];
            }
        }

        if (!isFactory) {
            if (ValidateArgs_) {
                Expr_ = exprs.front();
            }

            Name_ = src->MakeLocalName(Name_);
        }

        if (Expr_ && Expr_->IsAsterisk() && AggApplyName_ == "count") {
            AggApplyName_ = "count_all";
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr AggregationTraitsFactory() const override {
        return Factory_;
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const override {
        Y_UNUSED(ctx);
        return BuildLambda(Pos_, Y("row"), Y("PersistableRepr", many ? Y("Unwrap", Expr_) : Expr_));
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const override {
        auto extractor = GetExtractor(many, ctx);
        if (!extractor) {
            return nullptr;
        }

        if (!Multi_) {
            if (!DynamicFactory_ && allowAggApply && !AggApplyName_.empty()) {
                return Y("AggApply", Q(AggApplyName_), Y("ListItemType", type), extractor);
            }

            return Y("Apply", Factory_, (DynamicFactory_ ? Y("ListItemType", type) : type),
              extractor);
        }

        return Y("MultiAggregate",
            Y("ListItemType", type),
            extractor,
            Factory_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArgs_) {
            for (auto x : Exprs_) {
                if (!x->Init(ctx, src)) {
                    return false;
                }
                if (x->IsAggregated() && !x->IsAggregationKey() && !IsOverWindow() && !IsOverWindowDistinct()) {
                    ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
                    return false;
                }
            }

            return true;
        }

        if (!Expr_) {
            return true;
        }

        if (!Expr_->Init(ctx, src)) {
            return false;
        }
        if (Expr_->IsAggregated() && !Expr_->IsAggregationKey() && !IsOverWindow() && !IsOverWindowDistinct()) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        if (AggMode_ == EAggregateMode::Distinct || AggMode_ == EAggregateMode::OverWindowDistinct) {
            const auto column = Expr_->GetColumnName();
            if (!column) {
                // TODO: improve TBasicAggrFunc::CollectPreaggregateExprs()
                ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
                return false;
            }
            DistinctKey_ = *column;
            YQL_ENSURE(src);
            if (!IsGeneratedKeyColumn_ && src->GetJoin()) {
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
            if (!ctx.DistinctOverKeys && src->IsGroupByColumn(DistinctKey_)) {
                ctx.Error(Expr_->GetPos()) << ErrorDistinctByGroupKey(DistinctKey_);
                return false;
            }
            Expr_ = AstNode("row");
        }

        if (FakeSource_) {
            if (!Factory_->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            if (AggMode_ == EAggregateMode::OverWindow) {
                Factory_ = BuildLambda(Pos_, Y("type", "extractor"), Y("block", Q(Y(
                    Y("let", "x", Y("Apply", Factory_, "type", "extractor")),
                    Y("return", Y("ToWindowTraits", "x"))
                ))));
            }
        }

        return true;
    }

    TNodePtr Factory_;
    TNodePtr Expr_;
    bool Multi_;
    bool ValidateArgs_;
    TString AggApplyName_;
    TVector<TNodePtr> Exprs_;

private:
    TSourcePtr FakeSource_;
    bool DynamicFactory_;
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

TAggregationPtr BuildFactoryAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi) {
    return new TAggregationFactoryImpl(pos, name, func, aggMode, multi);
}

class TKeyPayloadAggregationFactory final : public TAggregationFactory {
public:
    TKeyPayloadAggregationFactory(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource_(BuildFakeSource(pos))
    {}

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 2;
        if (exprs.size() < adjustArgsCount || exprs.size() > 1 + adjustArgsCount) {
            ctx.Error(Pos_) << "Aggregation function " << (isFactory ? "factory " : "") << Name_ << " requires "
                << adjustArgsCount << " or " << (1 + adjustArgsCount) << " arguments, given: " << exprs.size();
            return false;
        }
        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
            return false;
        }

        if (!isFactory) {
            Payload_ = exprs.front();
            Key_ = exprs[1];
        }

        if (1 + adjustArgsCount == exprs.size()) {
            Limit_ = exprs.back();
            Func_ += "2";
        } else {
            Func_ += "1";
        }

        if (Factory_) {
            Factory_ = BuildBind(Pos_, AggMode_ == EAggregateMode::OverWindow ? "window_module" : "aggregate_module", Func_);
        }

        if (!isFactory) {
            Name_ = src->MakeLocalName(Name_);
        }

        if (!Init(ctx, src)) {
            return false;
        }

        if (!isFactory) {
            node.Add("Member", "row", Q(Name_));
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TKeyPayloadAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const final {
        Y_UNUSED(ctx);
        return BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Payload_) : Payload_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory_, type,
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Key_) : Key_),
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Payload_) : Payload_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        if (Limit_) {
            apply = L(apply, Limit_);
        }
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {1u, 0u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (Limit_) {
            if (!Limit_->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }

        if (!Key_) {
            return true;
        }

        if (!Key_->Init(ctx, src)) {
            return false;
        }
        if (!Payload_->Init(ctx, src)) {
            return false;
        }

        if (Key_->IsAggregated()) {
            ctx.Error(Pos_) << "Aggregation of aggregated values is forbidden";
            return false;
        }
        return true;
    }

    TSourcePtr FakeSource_;
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

        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
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
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TPayloadPredicateAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const final {
        Y_UNUSED(ctx);
        return BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Payload_) : Payload_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        return Y("Apply", Factory_, type,
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Payload_) : Payload_),
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Predicate_) : Predicate_));
    }

    std::vector<ui32> GetFactoryColumnIndices() const final {
        return {0u, 1u};
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Predicate_) {
            return true;
        }

        if (!Predicate_->Init(ctx, src)) {
            return false;
        }
        if (!Payload_->Init(ctx, src)) {
            return false;
        }

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

        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
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
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTwoArgsAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetExtractor(bool many, TContext& ctx) const final {
        Y_UNUSED(ctx);
        return BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", One_) : One_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto tuple = Q(Y(One_, Two_));
        return Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", tuple) : tuple));
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!One_) {
            return true;
        }

        if (!One_->Init(ctx, src)) {
            return false;
        }
        if (!Two_->Init(ctx, src)) {
            return false;
        }

        if ((One_->IsAggregated() || Two_->IsAggregated()) && !IsOverWindow() && !IsOverWindowDistinct()) {
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
                    ctx.Error(Pos_) << "Aggregation function " << Name_ << " for case with 3 arguments should have third argument of integer type";
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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory_, type,
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_),
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Weight_) : Weight_));
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
        if (!Weight_->Init(ctx, src)) {
            return false;
        }
        if (!Intervals_->Init(ctx, FakeSource_.Get())) {
            return false;
        }

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
        if (isFactory) {
            if (exprs.size() > 3) {
                ctx.Error(Pos_) << "Aggregation function " << Name_ << " requires zero to three arguments, given: " << exprs.size();
                return false;
            }
        } else {
            if (exprs.empty() || exprs.size() > 4) {
                ctx.Error(Pos_) << "Aggregation function " << Name_ << " requires one to four arguments, given: " << exprs.size();
                return false;
            }
        }

        if (exprs.size() > 1 - isFactory) {
            BinSize_ = exprs[1 - isFactory];
        }

        if (exprs.size() > 2 - isFactory) {
            Minimum_ = exprs[2 - isFactory];
        }

        if (exprs.size() > 3 - isFactory) {
            Maximum_ = exprs[3 - isFactory];
        }

        return TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front()));
    }

    TNodePtr DoClone() const final {
        return new TLinearHistogramAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        return Y("Apply", Factory_, type,
            BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_),
            BinSize_, Minimum_, Maximum_);
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, BinSize_, Minimum_, Maximum_);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!BinSize_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        if (!Minimum_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        if (!Maximum_->Init(ctx, FakeSource_.Get())) {
            return false;
        }

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
        YQL_ENSURE(percentile);
        YQL_ENSURE(Column_ && percentile->Column_ && *Column_ == *percentile->Column_);
        YQL_ENSURE(AggMode_ == percentile->AggMode_);
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
        }

        if (!TAggregationFactory::InitAggr(ctx, isFactory, src, node, isFactory ? TVector<TNodePtr>() : TVector<TNodePtr>(1, exprs.front())))
            return false;

        TNodePtr x;
        if (1 + adjustArgsCount == exprs.size()) {
            x = exprs.back();
            if (!x->Init(ctx, FakeSource_.Get())) {
                return false;
            }
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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        TNodePtr percentiles(Percentiles_.cbegin()->second);

        if (Percentiles_.size() > 1U) {
            percentiles = Y();
            for (const auto& percentile : Percentiles_) {
                percentiles = L(percentiles, percentile.second);
            }
            percentiles = Q(percentiles);
        }

        return Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_), percentiles);
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, FactoryPercentile_);
    }

    std::pair<TNodePtr, bool> AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const final {
        if (Percentiles_.empty())
            return { TNodePtr(), true };

        TNodePtr names(Q(Percentiles_.cbegin()->first));

        if (Percentiles_.size() > 1U) {
            names = Y();
            for (const auto& percentile : Percentiles_)
                names = L(names, Q(percentile.first));
            names = Q(names);
        }

        const bool distinct = AggMode_ == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos_, DistinctKey_))) : type;
        auto apply = GetApply(listType, many, allowAggApply, ctx);
        if (!apply) {
            return { TNodePtr(), false };
        }

        auto wrapped = WrapIfOverState(apply, overState, many, ctx);
        if (!wrapped) {
            return { TNodePtr(), false };
        }

        return { distinct ?
            Q(Y(names, wrapped, BuildQuotedAtom(Pos_, DistinctKey_))) :
            Q(Y(names, wrapped)), true };
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
        , FakeSource_(BuildFakeSource(pos))
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

        TNodePtr n = Y("Null");
        TNodePtr buffer = Y("Null");

        if (1 + adjustArgsCount <= exprs.size()) {
            n = exprs[adjustArgsCount];
            if (!n->Init(ctx, FakeSource_.Get())) {
                return false;
            }
            n = Y("SafeCast", n, Q("Uint32"));
        }

        n = Y("Coalesce", n, Y("Uint32", Q("1")));
        if (2 + adjustArgsCount == exprs.size()) {
            buffer = exprs[1 + adjustArgsCount];
            if (!buffer->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            buffer = Y("SafeCast", buffer, Q("Uint32"));
        }

        buffer = Y("Coalesce", buffer, Y("SafeCast", Y("*", n, Y("Double", Q(ToString(DefaultBufferC)))), Q("Uint32")));
        buffer = Y("Coalesce", buffer, Y("Uint32", Q(ToString(MinBuffer))));
        buffer = Y("Max", buffer, Y("Uint32", Q(ToString(MinBuffer))));

        auto x = TPair{ n, buffer };
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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        TPair topFreqs(TopFreqs_.cbegin()->second);

        if (TopFreqs_.size() > 1U) {
            topFreqs = { Y(), Y() };
            for (const auto& topFreq : TopFreqs_) {
                topFreqs = { L(topFreqs.first, topFreq.second.first), L(topFreqs.second, topFreq.second.second) };
            }
            topFreqs = { Q(topFreqs.first), Q(topFreqs.second) };
        }

        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_), topFreqs.first, topFreqs.second);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, TopFreqFactoryParams_.first, TopFreqFactoryParams_.second);
    }

    std::pair<TNodePtr, bool> AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const final {
        if (TopFreqs_.empty())
            return { TNodePtr(), true };

        TNodePtr names(Q(TopFreqs_.cbegin()->first));

        if (TopFreqs_.size() > 1U) {
            names = Y();
            for (const auto& topFreq : TopFreqs_)
                names = L(names, Q(topFreq.first));
            names = Q(names);
        }

        const bool distinct = AggMode_ == EAggregateMode::Distinct;
        const auto listType = distinct ? Y("ListType", Y("StructMemberType", Y("ListItemType", type), BuildQuotedAtom(Pos_, DistinctKey_))) : type;
        auto apply = GetApply(listType, many, allowAggApply, ctx);
        if (!apply) {
            return { nullptr, false };
        }

        auto wrapped = WrapIfOverState(apply, overState, many, ctx);
        if (!wrapped) {
            return { nullptr, false };
        }

        return { distinct ?
            Q(Y(names, wrapped, BuildQuotedAtom(Pos_, DistinctKey_))) :
            Q(Y(names, wrapped)), true };
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
    TSourcePtr FakeSource_;
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

        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
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
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TTopAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        TNodePtr apply;
        if (HasKey) {
            apply = Y("Apply", Factory_, type,
                BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Key_) : Key_),
                BuildLambda(Pos_, Y("row"), many ? Y("Payload", Payload_) : Payload_));
        } else {
            apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Payload_) : Payload_));
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
        if (!Count_->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        if (!Payload_) {
            return true;
        }

        if (HasKey) {
            if (!Key_->Init(ctx, src)) {
                return false;
            }
        }

        if (!Payload_->Init(ctx, src)) {
            return false;
        }

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
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TCountDistinctEstimateAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_));
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
        , FakeSource_(BuildFakeSource(pos))
    {
    }

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

        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
            return false;
        }

        Limit_ = nullptr;
        if (adjustArgsCount + 1U <= exprs.size()) {
            auto posSecondArg = exprs[adjustArgsCount]->GetPos();
            Limit_ = exprs[adjustArgsCount];
            if (!Limit_->Init(ctx, FakeSource_.Get())) {
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
            if (IsOverWindow() || IsOverWindowDistinct()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TListAggregationFactory(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        if (!Limit_) {
            apply = L(apply, Y("Uint64", Q("0")));
        } else {
            apply = L(apply, Limit_);
        }
    }

private:
    TSourcePtr FakeSource_;
    TNodePtr Limit_;
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
        Lambdas_[2] = BuildLambda(Pos_, Y("one", "two"), Y("IfType", exprs[adjustArgsCount + 2], Y("NullType"),
            BuildLambda(Pos_, Y(), Y("Void")),
            BuildLambda(Pos_, Y(), Y("Apply", exprs[adjustArgsCount + 2], "one", "two"))));

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

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_));
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
        if (!Expr_->Init(ctx, src)) {
            return false;
        }
        Expr_->SetCountHint(Expr_->IsConstant());
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
        , PgFunc_(Name_)
    {}

    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) override {
        auto ret = TAggregationFactory::InitAggr(ctx, isFactory, src, node, exprs);
        if (ret) {
            if (isFactory) {
                Factory_ = BuildLambda(Pos_, Y("type", "extractor"), Y(AggMode_ == EAggregateMode::OverWindow ? "PgWindowTraitsTuple" : "PgAggregationTraitsTuple",
                    Q(PgFunc_), Y("ListItemType", "type"), "extractor"));
            } else {
                Lambda_ = BuildLambda(Pos_, Y("row"), exprs);
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
        if (ShouldEmitAggApply(ctx) && allowAggApply && AggMode_ != EAggregateMode::OverWindow) {
            return Y("AggApply",
                Q("pg_" + to_lower(PgFunc_)), Y("ListItemType", type), Lambda_);
        }

        return Y(AggMode_ == EAggregateMode::OverWindow ? "PgWindowTraits" : "PgAggregationTraits",
            Q(PgFunc_), Y("ListItemType", type), Lambda_);
    }

private:
    TNodePtr DoClone() const final {
        return new TPGFactoryAggregation(Pos_, Name_, AggMode_);
    }

    TString PgFunc_;
    TNodePtr Lambda_;
};

TAggregationPtr BuildPGFactoryAggregation(TPosition pos, const TString& name, EAggregateMode aggMode) {
    return new TPGFactoryAggregation(pos, name, aggMode);
}

class TNthValueFactoryAggregation final : public TAggregationFactory {
public:
public:
    TNthValueFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode)
        : TAggregationFactory(pos, name, factory, aggMode)
        , FakeSource_(BuildFakeSource(pos))
    {
    }

private:
    bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) final {
        ui32 adjustArgsCount = isFactory ? 0 : 1;
        ui32 expectedArgs = (1 + adjustArgsCount);
        if (exprs.size() != expectedArgs) {
            ctx.Error(Pos_) << "NthValue aggregation " << (isFactory ? "factory " : "") << "function require "
                << expectedArgs << " arguments, given: " << exprs.size();
            return false;
        }

        if (BlockWindowAggregationWithoutFrameSpec(Pos_, GetName(), src, ctx)) {
            return false;
        }

        Index_ = exprs[adjustArgsCount];
        if (!Index_->Init(ctx, FakeSource_.Get())) {
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
            if (IsOverWindow()) {
                src->AddTmpWindowColumn(Name_);
            }
        }

        return true;
    }

    TNodePtr DoClone() const final {
        return new TNthValueFactoryAggregation(Pos_, Name_, Func_, AggMode_);
    }

    TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const final {
        Y_UNUSED(ctx);
        Y_UNUSED(allowAggApply);
        auto apply = Y("Apply", Factory_, type, BuildLambda(Pos_, Y("row"), many ? Y("Unwrap", Expr_) : Expr_));
        AddFactoryArguments(apply);
        return apply;
    }

    void AddFactoryArguments(TNodePtr& apply) const final {
        apply = L(apply, Index_);
    }

private:
    TSourcePtr FakeSource_;
    TNodePtr Index_;
};

TAggregationPtr BuildNthFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode) {
    return new TNthValueFactoryAggregation(pos, name, factory, aggMode);
}

} // namespace NSQLTranslationV1
