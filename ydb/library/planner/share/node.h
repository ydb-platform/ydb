#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/generic/vector.h>
#include <util/generic/list.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>
#include <ydb/library/planner/base/defs.h>
#include <ydb/library/planner/share/models/model.h>
#include "stats.h"

namespace NScheduling {

class IContext;
class IModel;
class TShareAccount;
class TShareGroup;
class TSharePlanner;

class TShareNode : public IVisitable {
public:
    SCHEDULING_DEFINE_VISITABLE(IVisitable);
public:
    struct TConfig {
        virtual ~TConfig() {}
        TString Name;
        FWeight Weight; // Weight in parent group
        FWeight MaxWeight;
        TEnergy Volume;

        TConfig(TEnergy v, FWeight wmax, FWeight w, const TString& name)
            : Name(name)
            , Weight(w)
            , MaxWeight(wmax)
            , Volume(v)
        {
            Y_ABORT_UNLESS(Weight > 0, "non-positive (%lf) weight in planner node '%s'",
                   Weight, Name.data());
            Y_ABORT_UNLESS(Weight <= MaxWeight, "max weight (%lf) must be greater or equal to normal weight (%lf) in planner node '%s'",
                   MaxWeight, Weight, Name.data());
            Y_ABORT_UNLESS(Volume >= 0, "negative (%lf) volume in planner node '%s'",
                   Volume, Name.data());
        }
    };
protected:
    THolder<IContext> Context;
    THolder<TConfig> Config;
    TSharePlanner* Planner = nullptr;
    TShareGroup* Parent = nullptr;
    TForce Share = 0; // s0[i] = w0[i] / sum(w0[i] for i in parent group) -- default share in parent group
    TEnergy D_ = 0; // Work done
    TEnergy S_ = 0; // Spoiled energy
    TEnergy dD_ = 0;
    TEnergy dS_ = 0;
    TEnergy pdD_ = 0;
    TEnergy pdS_ = 0;
    TDimless Tariff = 1.0;
    TNodeStats Stats;
public:
    explicit TShareNode(TAutoPtr<TConfig> cfg)
        : Config(cfg.Release())
    {}
    virtual ~TShareNode() {}
    const TConfig& Cfg() const { return *Config.Get(); }
    void SetConfig(TAutoPtr<TConfig> cfg);
public: // Accessors
    const TString& GetName() const { return Cfg().Name; }
    TShareGroup* GetParent() { return Parent; }
    const TShareGroup* GetParent() const { return Parent; }
    TSharePlanner* GetPlanner() { return Planner; }
    const TSharePlanner* GetPlanner() const { return Planner; }
    TForce GetShare() const { return Share; }
    void SetShare(TForce value) { Share = value; }
public: // Computations
    TLength Normalize(TEnergy cost) const { return cost / s0(); }
    TEnergy ComputeEg() const;
    TForce CalcGlobalShare() const;
public: // Short accessors for formulas
    TForce s0() const { return Share; }
    FWeight w0() const { return Cfg().Weight; }
    FWeight wmax() const { return Cfg().MaxWeight; }
    FWeight w() const { return Context? Ctx()->GetWeight(): w0(); }
    TDimless h() const { return (w() - w0()) / (wmax() - w0()); }
    TEnergy D() const { return D_; }
    TEnergy S() const { return S_; }
    TEnergy dD() const { return dD_; }
    TEnergy dS() const { return dS_; }
    TEnergy pdD() const { return pdD_; }
    TEnergy pdS() const { return pdS_; }
    TEnergy E() const { return D_ + S_; }
    TEnergy V() const { return Cfg().Volume; }
    TEnergy L(TEnergy Eg) const { return Eg*Share/gs - V(); }
    TEnergy O(TEnergy Eg) const { return L(Eg); }
    TEnergy A(TEnergy Eg) const { return E() - L(Eg); }
    TEnergy P(TEnergy Eg) const { return E() - Eg*Share/gs; }
    TLength dd() const { return dD() / Share; }
    TLength ds() const { return dS() / Share; }
    TLength pdd() const { return pdD() / Share; }
    TLength pds() const { return pdS() / Share; }
    TLength e() const { return E() / Share; }
    TLength v() const { return V() / Share; }
    TLength l(TEnergy Eg) const { return L(Eg) / Share; }
    TLength o(TEnergy Eg) const { return O(Eg) / Share; }
    TLength a(TEnergy Eg) const { return A(Eg) / Share; }
    TLength p(TEnergy Eg) const { return e() - Eg/gs; }
public:
    void SetS(TEnergy value) { S_ = value; }
    void SetD(TEnergy value) { D_ = value; }
public: // Context for models
    IContext* Ctx() { return Context.Get(); }
    const IContext* Ctx() const { return Context.Get(); }
    void ResetCtx(IContext* ctx) { Context.Reset(ctx); }
    template <class TCtx>
    TCtx& Ctx() { return *static_cast<TCtx*>(Context.Get()); }
    template <class TCtx>
    TCtx* CtxAs() { return dynamic_cast<TCtx*>(Context.Get()); }
public: // Tree modifications
    void Attach(TSharePlanner* planner, TShareGroup* parent);
    void Detach();
    void DetachNoRemove();
public: // Evolution
    void Done(TEnergy cost);
    void Spoil(TEnergy cost);
    void SetTariff(TDimless tariff);
    void Step();
public: // Monitoring
    bool IsActive() const { return pdD() > 0; }
    bool IsRetard() const { return pdS() > 0; }
    const TNodeStats& GetStats() const { return Stats; }
    virtual TString GetStatus() const;
};

}
