#pragma once

#include "node.h"

namespace NScheduling {

class TShareAccount: public TShareNode {
public:
    SCHEDULING_DEFINE_VISITABLE(TShareNode);
public: // Configuration
    TShareAccount(TAutoPtr<TConfig> cfg)
        : TShareNode(cfg.Release())
    {}
    TShareAccount(const TString& name, FWeight w, FWeight wmax, TEnergy v)
        : TShareAccount(new TConfig(v, wmax, w, name))
    {}
    void Configure(const TString& name, FWeight w, FWeight wmax, TEnergy v)
    {
        SetConfig(new TConfig(v, wmax, w, name));
    }
    const TConfig& Cfg() const { return *static_cast<const TConfig*>(Config.Get()); }
};

}
