#pragma once

#include <util/generic/map.h>
#include <util/system/type_name.h>
#include "account.h"

namespace NScheduling {

class TShareGroup: public TShareNode {
public:
    SCHEDULING_DEFINE_VISITABLE(TShareNode);
protected:
    TMap<TString, TShareNode*> Children;
    FWeight TotalWeight = 0; // Total weight of children
    TEnergy TotalVolume = 0; // Total volume of children
public: // Configuration
    TShareGroup(TAutoPtr<TConfig> cfg)
        : TShareNode(cfg.Release())
    {}
    TShareGroup(const TString& name, FWeight w, FWeight wmax, TEnergy v)
        : TShareGroup(new TConfig(v, wmax, w, name))
    {}
    void Configure(const TString& name, FWeight w, FWeight wmax, TEnergy v)
    {
        SetConfig(new TConfig(v, wmax, w, name));
    }
    const TConfig& Cfg() const { return *static_cast<const TConfig*>(Config.Get()); }
public:
    bool Empty() const { return Children.empty(); }
    void AcceptInChildren(IVisitorBase* v);
    void AcceptInChildren(IVisitorBase* v) const;
public:
    TShareNode* FindByName(const TString& name);
    inline FWeight GetTotalWeight() const { return TotalWeight; }
    inline TEnergy GetTotalVolume() const { return TotalVolume; }
    void Add(TShareNode* node);
    void Remove(TShareNode* node);
    void Clear();
    TEnergy ComputeEc() const;
public: // Monitoring
    TEnergy GetTotalCredit() const;
protected:
    void Insert(TShareAccount* node);
    void Insert(TShareGroup* node);
    void Erase(TShareAccount* node);
    void Erase(TShareGroup* node);
    void ResetShare();
};

#define CUSTOMSHAREPLANNER_FOR(acctype, grptype, node, expr) \
    do { \
        for (auto _ch_ : Children) { \
            auto* _node_ = _ch_.second; \
            if (auto* node = dynamic_cast<acctype*>(_node_)) { expr; } \
            else if (auto* node = dynamic_cast<grptype*>(_node_)) { expr; } \
            else { Y_ABORT("node is niether " #acctype " nor " #grptype ", it is %s", TypeName(*node).c_str()); } \
        } \
    } while (false) \
    /**/

#define SHAREPLANNER_FOR(node, expr) CUSTOMSHAREPLANNER_FOR(TShareAccount, TShareGroup, node, expr)

}
