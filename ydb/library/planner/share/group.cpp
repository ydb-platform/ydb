#include "group.h"
#include "apply.h"

namespace NScheduling {

TShareNode* TShareGroup::FindByName(const TString& name)
{
    auto i = Children.find(name);
    if (i == Children.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

void TShareGroup::ResetShare()
{
    TForce s0 = gs;
    FWeight wsum = TotalWeight;
    for (auto ch : Children) {
        TShareNode* node = ch.second;
        node->SetShare(WCut(node->w0(), wsum, s0));
    }
}

void TShareGroup::Add(TShareNode* node)
{
    Y_ABORT_UNLESS(!Children.contains(node->GetName()), "duplicate child name '%s' in group '%s'", node->GetName().data(), GetName().data());
    Children[node->GetName()] = node;
    TotalWeight += node->w0();
    TotalVolume += node->V();
    ResetShare();
}

void TShareGroup::Remove(TShareNode* node)
{
    Y_ABORT_UNLESS(Children.contains(node->GetName()), "trying to delete unknown child name '%s' in group '%s'", node->GetName().data(), GetName().data());
    Children.erase(node->GetName());
    TotalWeight -= node->w0();
    TotalVolume -= node->V();
    ResetShare();
}

void TShareGroup::Clear()
{
    ApplyTo<TShareAccount, TShareGroup>(this, [] (TShareAccount* acc) {
        acc->DetachNoRemove();
    }, [] (TShareGroup* grp) {
        grp->Clear();
        grp->DetachNoRemove();
    });
    Children.clear();
    TotalWeight = 0;
    TotalVolume = 0;
}

TEnergy TShareGroup::ComputeEc() const
{
    TEnergy Ec = 0;
    ApplyTo<const TShareNode>(this, [&Ec] (const TShareNode* node) {
        Ec += node->E();
    });
    return Ec;
}

TEnergy TShareGroup::GetTotalCredit() const
{
    TEnergy Eg = 0;
    for (auto ch : Children) {
        TShareNode* node = ch.second;
        Eg += node->E();
    }
    TEnergy credit = 0;
    for (auto ch : Children) {
        TShareNode* node = ch.second;
        TEnergy P = node->P(Eg);
        if (P > 0) {
            credit += P;
        }
    }
    return credit;
}

void TShareGroup::AcceptInChildren(IVisitorBase* v)
{
    for (auto ch : Children) {
        TShareNode* node = ch.second;
        node->Accept(v);
    }
}

void TShareGroup::AcceptInChildren(IVisitorBase* v) const
{
    for (auto ch : Children) {
        const TShareNode* node = ch.second;
        node->Accept(v);
    }
}

}
