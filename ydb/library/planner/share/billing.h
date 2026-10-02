#pragma once

#include <util/generic/algorithm.h>
#include "apply.h"
#include "node_visitor.h"
#include "shareplanner.h"
#include <ydb/library/planner/share/models/density.h>

namespace NScheduling {

// Just apply static tariff
class TStaticBilling : public INodeVisitor
{
private:
    TDimless Tariff;
public:
    TStaticBilling(TDimless tariff = 1.0)
        : Tariff(tariff)
    {}

    void Visit(TShareAccount* node) override
    {
        Handle(node);
    }

    void Visit(TShareGroup* node) override
    {
        node->AcceptInChildren(this);
        Handle(node);
    }
protected:
    void Handle(TShareNode* node)
    {
        node->SetTariff(Tariff);
    }
};

// It sets current tariff, which in turn is just sigma value
// for parent group. Sigma represents how many nodes of a group is present at the moment
// each multiplied by it's static share
// For example:
// 1) if every user is working, then sigma equals one
// 2) if there is the only working user with static weight s0, then sigma equals s0
// Note that 0 < sigma <= 1
class TPresentShareBilling : public INodeVisitor
{
private:
    bool Memoryless = false;
public:
    TPresentShareBilling(bool memoryless)
        : Memoryless(memoryless)
    {}

    void Visit(TShareAccount* node) override
    {
        Handle(node);
    }

    void Visit(TShareGroup* node) override
    {
        node->AcceptInChildren(this);
        Handle(node);
    }
protected:
    void Handle(TShareNode* node)
    {
        if (TShareGroup* group = node->GetParent()) {
            if (TDeContext* pctx = group->CtxAs<TDeContext>()) {
                TDimless tariff = Memoryless? pctx->isigma: pctx->sigma;
                if (tariff == 0) // There is nobody working on cluster
                    tariff = 1.0; // Just to avoid stalling (otherwise, every dDi will be zero always)
                node->SetTariff(tariff);
            }
        }
    }
};

}
