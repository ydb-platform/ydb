#include "select_yql_aggregation.h"

#include "context.h"
#include "select_yql_window.h"

namespace NSQLTranslationV1 {

class TYqlAggregation final: public IYqlWindowLikeNode, private TYqlAggregationArgs {
public:
    TYqlAggregation(TPosition position, TYqlAggregationArgs&& args)
        : IYqlWindowLikeNode(std::move(position))
        , TYqlAggregationArgs(std::move(args))
        , Aggregation_(BuildAggregationByType(
              Type,
              GetPos(),
              /*realFunctionName=*/FunctionName,
              /*factoryName=*/FactoryName,
              Mode))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        TAstListNodeImpl dummy(GetPos()); // For a legacy interopability
        if (!NSQLTranslationV1::Init(ctx, src, Args) ||
            !Aggregation_->InitAggr(ctx, /*isFactory=*/false, src, dummy, Args)) {
            return false;
        }

        Apply_ = Y();

        if (IsWindow()) {
            Apply_ = L(std::move(Apply_), "YqlAggWin");
        } else {
            Apply_ = L(std::move(Apply_), "YqlAgg");
        }

        Apply_ = L(std::move(Apply_), Factory());

        if (IsWindow()) {
            Apply_ = L(std::move(Apply_), Q(GetWindowName()));
        }

        Apply_ = L(std::move(Apply_), Options());
        Apply_ = L(std::move(Apply_), TypeStub());
        Apply_ = L(std::move(Apply_), ExtractorBody(ctx));

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Apply_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlAggregation(*this);
    }

private:
    TNodePtr Factory() const {
        TNodePtr factory = Y();

        if (IsWindow()) {
            factory = L(std::move(factory), "YqlWinFactory");
        } else {
            factory = L(std::move(factory), "YqlAggFactory");
        }

        factory = L(std::move(factory), Name());
        for (TNodePtr arg : Args | std::views::drop(1)) {
            factory = L(std::move(factory), std::move(arg));
        }
        return factory;
    }

    TNodePtr Name() const {
        TStringBuf name = FactoryName;
        YQL_ENSURE(name.ChopSuffix("_traits_factory"), "'" << name << "'");
        return Q(TString(name));
    }

    TNodePtr Options() const {
        TNodePtr options = Y();
        if (IsDistinct()) {
            options = L(std::move(options), Q(Y(Q("distinct"))));
        }
        return Q(std::move(options));
    }

    TNodePtr TypeStub() const {
        return Y("Void");
    }

    TNodePtr ExtractorBody(TContext& ctx) const {
        return Aggregation_->GetExtractorBody(/*many=*/false, ctx);
    }

    bool IsDistinct() const {
        return Mode == EAggregateMode::Distinct ||
               Mode == EAggregateMode::OverWindowDistinct;
    }

    bool IsWindow() const {
        return Mode == EAggregateMode::OverWindow ||
               Mode == EAggregateMode::OverWindowDistinct;
    }

    const TAggregationPtr Aggregation_;
    TNodePtr Apply_;
};

bool IsSupported(EAggregationType type) {
    return type == NORMAL ||
           type == COUNT;
}

bool IsSupported(const TVector<TNodePtr>& args) {
    return args.size() == 1;
}

bool IsSupported(TYqlAggregationArgs& args) {
    return IsSupported(args.Type) &&
           IsSupported(args.Args);
}

TNodeResult BuildYqlAggregation(TPosition position, TYqlAggregationArgs&& args) {
    if (!IsSupported(args)) {
        return std::unexpected(ESQLError::UnsupportedYqlSelect);
    }

    return TNonNull(TNodePtr(new TYqlAggregation(std::move(position), std::move(args))));
}

TNodePtr BuildYqlGrouping(TPosition position, TVector<TNodePtr> args) {
    return new TCallNodeImpl(position, "YqlGrouping", std::move(args));
}

} // namespace NSQLTranslationV1
