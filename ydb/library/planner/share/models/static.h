#pragma once

#include "recursive.h"

namespace NScheduling {

class TStaticContext : public IContext {
private:
    TShareNode* Node;
public:
    TStaticContext(IModel* model, TShareNode* node)
        : IContext(model)
        , Node(node)
    {}

    FWeight GetWeight() const override
    {
        return Node->w0();
    }
};

class TStaticModel : public TRecursiveModel<TStaticContext> {
public:
    explicit TStaticModel(TSharePlanner* planner)
        : TRecursiveModel(planner)
    {}

    void OnAccount(TShareAccount*, TCtx&, TCtx&) override {}
    void OnDescend(TShareGroup*, TCtx&, TCtx&) override {}
    void OnAscend(TShareGroup*, TCtx&, TCtx&) override {}
    void OnAttach(TShareNode*) override {}
    void OnDetach(TShareNode*) override {}
};

}
