#include "node.h"
#include "group.h"
#include "apply.h"

namespace NScheduling {

void TShareNode::SetConfig(TAutoPtr<TShareNode::TConfig> cfg)
{
    Y_ABORT_UNLESS(!Planner, "configure of attached share planner nodes is not allowed");
    Config.Reset(cfg.Release());
}

TEnergy TShareNode::ComputeEg() const
{
    if (Parent) {
        return Parent->ComputeEc();
    } else {
        return E();
    }
}

TForce TShareNode::CalcGlobalShare() const
{
    return Share * (Parent? Parent->CalcGlobalShare(): 1.0);
}

void TShareNode::Attach(TSharePlanner* planner, TShareGroup* parent)
{
    Planner = planner;
    Parent = parent;
    if (Parent) {
        Parent->Add(this);
    } else {
        Share = gs;
    }
}

void TShareNode::Detach()
{
    if (Parent) {
        Parent->Remove(this);
    }
    DetachNoRemove();
}

void TShareNode::DetachNoRemove()
{
    Planner = nullptr;
    Parent = nullptr;
}

void TShareNode::Done(TEnergy cost)
{
    Y_ABORT_UNLESS(cost >= 0, "negative work in node '%s' dD=%lf", GetName().data(), cost);

    // Apply current tariff
    TEnergy bill = cost * Tariff;
    D_ += bill;
    dD_ += bill;
    Stats.Done(cost);
    Stats.Pay(bill);

    if (Parent) {
        Parent->Done(cost); // Work is transmitted to parent
    }
}

void TShareNode::Spoil(TEnergy cost)
{
    Y_ABORT_UNLESS(cost >= 0, "negative spoil in node '%s' dS=%lf", GetName().data(), cost);
    S_ += cost;
    dS_ += cost;
    Stats.Spoil(cost);
    // Spoil is NOT transmitted to parent
}

void TShareNode::SetTariff(TDimless tariff)
{
    Tariff = tariff;
}

void TShareNode::Step()
{
    bool wasActive = IsActive();
    bool wasRetarded = IsRetard();
    pdS_ = dS_;
    pdD_ = dD_;
    dS_ = 0;
    dD_ = 0;
    if (!wasActive && IsActive())
        Stats.Activations++;
    if (wasActive && !IsActive())
        Stats.Deactivations++;
    if (!wasRetarded && IsRetard())
        Stats.Retardations++;
    if (wasRetarded && !IsRetard())
        Stats.Overtakes++;
}

TString TShareNode::GetStatus() const
{
    TString status;
    if (pdD() > 0) {
        status += "ACTIVE";
    } else {
        status += "IDLE";
    }
    if (pdS() > 0) {
        status += " RETARD";
    }
    return status;
}

}
