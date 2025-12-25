#include "select_yql_aggregation.h"

#include "context.h"

namespace NSQLTranslationV1 {

class TYqlAggregation final: public INode, private TYqlAggregationArgs {
public:
    TYqlAggregation(TPosition position, TYqlAggregationArgs&& args)
        : INode(std::move(position))
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

        Apply_ = Y("YqlAgg", Factory(), Options(), TypeStub(), ExtractorBody(ctx));
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
        TNodePtr factory = Y("YqlAggFactory", Name());
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
        if (Mode == EAggregateMode::Distinct) {
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

    const TAggregationPtr Aggregation_;
    TNodePtr Apply_;
};

bool IsSupported(EAggregationType type) {
    return type == NORMAL ||
           type == COUNT;
}

bool IsSupported(EAggregateMode mode) {
    return mode == EAggregateMode::Normal ||
           mode == EAggregateMode::Distinct;
}

bool IsSupported(const TVector<TNodePtr>& args) {
    return args.size() == 1;
}

bool IsSupported(TYqlAggregationArgs& args) {
    return IsSupported(args.Type) &&
           IsSupported(args.Mode) &&
           IsSupported(args.Args);
}

TNodeResult BuildYqlAggregation(TPosition position, TYqlAggregationArgs&& args) {
    if (!IsSupported(args)) {
        return std::unexpected(ESQLError::UnsupportedYqlSelect);
    }

    return TNonNull(TNodePtr(new TYqlAggregation(std::move(position), std::move(args))));
}

} // namespace NSQLTranslationV1
