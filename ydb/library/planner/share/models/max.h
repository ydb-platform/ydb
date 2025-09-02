#pragma once

#include "recursive.h"

namespace NScheduling {

class TMaxContext : public IContext {
private:
    TShareNode* Node;
public:
    TMaxContext(IModel* model, TShareNode* node)
        : IContext(model)
        , Node(node)
    {}

    FWeight GetWeight() const override
    {
        return Node->wmax();
    }
};

class TMaxModel : public TRecursiveModel<TMaxContext> {
public:
    explicit TMaxModel(TSharePlanner* planner)
        : TRecursiveModel(planner)
    {}

    void OnAccount(TShareAccount*, TCtx&, TCtx&) override {}
    void OnDescend(TShareGroup*, TCtx&, TCtx&) override {}
    void OnAscend(TShareGroup*, TCtx&, TCtx&) override {}
    void OnAttach(TShareNode*) override {}
    void OnDetach(TShareNode*) override {}
};

}
