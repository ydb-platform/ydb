#pragma once

#include "model.h"
#include <ydb/library/planner/share/shareplanner.h>
#include <functional>

namespace NScheduling {

template <class Context>
class TRecursiveModel : public IModel
{
public:
    typedef Context TCtx;
private:
    THolder<TCtx> GlobalCtx; // Special parent context for root
public:
    explicit TRecursiveModel(TSharePlanner* planner)
        : IModel(planner)
    {}

    void Visit(TShareAccount* node) final
    {
        TCtx& ctx = GetContext(node);
        TCtx& pctx = GetParentContext(node);
        OnAccount(node, ctx, pctx);
    }

    void Visit(TShareGroup* node) final
    {
        TCtx& ctx = GetContext(node);
        TCtx& pctx = GetParentContext(node);
        OnDescend(node, ctx, pctx);
        node->AcceptInChildren(this);
        OnAscend(node, ctx, pctx);
    }

    virtual TCtx* CreateContext(TShareNode* node = nullptr)
    {
        return new TCtx(this, node);
    }
protected:
    virtual void OnAccount(TShareAccount* account, TCtx& ctx, TCtx& pctx) { Y_UNUSED(account); Y_UNUSED(ctx); Y_UNUSED(pctx); }
    virtual void OnDescend(TShareGroup* group, TCtx& ctx, TCtx& pctx) { Y_UNUSED(group); Y_UNUSED(ctx); Y_UNUSED(pctx); }
    virtual void OnAscend(TShareGroup* group, TCtx& ctx, TCtx& pctx) { Y_UNUSED(group); Y_UNUSED(ctx); Y_UNUSED(pctx); }
protected:
    TCtx& GetGlobalCtx()
    {
        Y_ASSERT(GlobalCtx);
        return *GlobalCtx;
    }

    TCtx& GetContext(TShareNode* node)
    {
        if (!node->Ctx() || node->Ctx()->GetModel() != this) {
            node->ResetCtx(CreateContext(node));
        }
        return node->Ctx<TCtx>();
    }

    TCtx& GetParentContext(TShareNode* node)
    {
        TShareNode* parent = node->GetParent();
        if (parent) {
            Y_ASSERT(parent->Ctx()->GetModel() == this);
            return parent->Ctx<TCtx>();
        } else {
            if (!GlobalCtx) {
                GlobalCtx.Reset(CreateContext());
            }
            return *GlobalCtx;
        }
    }

    void DestroyGlobalContext()
    {
        GlobalCtx.Destroy();
    }
};

}
