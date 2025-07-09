#pragma once

#include <cmath>
#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/generic/vector.h>
#include <util/generic/list.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>
#include <ydb/library/planner/share/protos/shareplanner.pb.h>
#include <ydb/library/planner/share/models/model.h>
#include "probes.h"
#include "node.h"
#include "account.h"
#include "group.h"
//#include <ydb/library/planner/share/utilization.h>

namespace NScheduling {

struct TArtParams {
    bool SigmaStretch;
    double Scale;

    TArtParams(bool sigmaStretch = true, double scale = 1.0)
        : SigmaStretch(sigmaStretch)
        , Scale(scale)
    {}
};

struct TSharePlannerStats {
    double ResUsed = 0;
    double ResWasted = 0;
};

class TSharePlanner {
protected:
    // Configuration
    TSharePlannerConfig Config;

    TShareGroup* Root = nullptr; // Root must be set by derived class (e.g. TCustomSharePlanner)
    TEnergy D_ = 0; // Total work done in system
    TEnergy W_ = 0; // Total work wasted in system
    TEnergy dD_ = 0;
    TEnergy dW_ = 0;
    TEnergy pdD_ = 0;
    TEnergy pdW_ = 0;
    //THolder<TUtilizationMeter> UtilMeter;
    THolder<INodeVisitor> Billing;
    THolder<IModel> Model;
    THolder<INodeVisitor> Puller;
    THolder<INodeVisitor> Stepper;

    // Statistics and monitoring
    TSharePlannerStats Stats;
public: // Configuration
    explicit TSharePlanner(const TSharePlannerConfig& cfg = TSharePlannerConfig());
    virtual ~TSharePlanner() {}
    const TSharePlannerConfig& Cfg() const { return Config; }
    void Configure(const TSharePlannerConfig& cfg);
public: // Accessors
    TString GetName() const { return Config.GetName(); }
    TShareGroup* GetRoot() { return Root; }
    const TShareGroup* GetRoot() const { return Root; }
    TEnergy D() const { return D_; }
    TEnergy W() const { return W_; }
    TEnergy dD() const { return dD_; }
    TEnergy dW() const { return dW_; }
    TEnergy pdD() const { return dD_; }
    TEnergy pdW() const { return dW_; }
    //double Utilization() const { return UtilMeter->Utilization(); }
public: // Evolution
    void Done(TShareAccount* acc, TEnergy cost);
    void Waste(TEnergy cost);
    void Commit(bool model = true);
public: // Monitoring
    virtual TString PrintStatus() const;
    virtual TString PrintTree(bool band, bool model, const TArtParams& art) const;
    virtual TString PrintStats() const;
    TString PrintInfo(bool status = true, bool band = true, bool model = true, bool stats = true, const TArtParams& art = TArtParams()) const;
    void GetStats(TSharePlannerStats& stats) const;
protected: // Implementation
    void Advance(TEnergy cost, bool wasted);
    virtual void OnDetach(TShareNode* node);
    virtual void OnAttach(TShareNode* node, TShareGroup* parent);
};

/////////////////////////////////
///                           ///
///      Custom planner       ///
///                           ///
/////////////////////////////////

// Helpers
template <class TAcc, class TGrp, class TAccPtr, class TGrpPtr, class Node> struct TNodeTraits {}; // See specialization below
template <class TAcc, class TGrp, class TAccPtr, class TGrpPtr>
struct TNodeTraits<TAcc, TGrp, TAccPtr, TGrpPtr, TAcc> { typedef TAccPtr TPtr; };
template <class TAcc, class TGrp, class TAccPtr, class TGrpPtr>
struct TNodeTraits<TAcc, TGrp, TAccPtr, TGrpPtr, TGrp> { typedef TGrpPtr TPtr; };

template <class TAcc, class TGrp, class TAccPtr = std::shared_ptr<TAcc>, class TGrpPtr = std::shared_ptr<TGrp>>
class TCustomSharePlanner: public TSharePlanner {
public:
    typedef THashMap<TString, TAccPtr> TAccs;
    typedef THashMap<TString, TGrpPtr> TGrps;
    static_assert(std::is_base_of<TShareGroup, TGrp>::value, "TGrp must inherit TShareGroup");
    static_assert(std::is_base_of<TShareAccount, TAcc>::value, "TAcc must inherit TShareAccount");
    template <class TNode> using TTraits = TNodeTraits<TAcc, TGrp, TAccPtr, TGrpPtr, TNode>;
protected:
    TAccs Accs; // All accounts storage
    TGrps Grps; // All groups storage
protected: // To support shared pointers on base class of TAcc/TGrp (not directly of TAcc/TGrp)
    static TAcc* Cast(const TAccPtr& node) { return static_cast<TAcc*>(&*node); }
    static TGrp* Cast(const TGrpPtr& node) { return static_cast<TGrp*>(&*node); }
    static const TAcc* ConstCast(const TAccPtr& node) { return static_cast<const TAcc*>(&*node); }
    static const TGrp* ConstCast(const TGrpPtr& node) { return static_cast<const TGrp*>(&*node); }
    static TShareAccount* BaseCast(const TAccPtr& node) { return (TShareAccount*)Cast(node); }
    static TShareGroup* BaseCast(const TGrpPtr& node) { return (TShareGroup*)Cast(node); }
    static TShareAccount* Base(TAcc* node) { return (TShareAccount*)node; }
    static TShareGroup* Base(TGrp* node) { return (TShareGroup*)node; }
public: // Configuration
    explicit TCustomSharePlanner(const TSharePlannerConfig& cfg = TSharePlannerConfig())
        : TSharePlanner(cfg)
    {
        Root = Cast(Add<TGrp>(nullptr, "/", 1, 1, 1));
    }

    TGrp* GetRoot()
    {
        return static_cast<TGrp*>(Root);
    }

    const TGrp* GetRoot() const
    {
        return static_cast<const TGrp*>(Root);
    }

    template <typename TNode, typename... Args>
    typename TTraits<TNode>::TPtr Add(TGrp* parent, const Args&... args)
    {
        typename TTraits<TNode>::TPtr node(new TNode(args...));
        Add<TNode>(node, parent);
        return node;
    }

    template <class TNode>
    void Add(typename TTraits<TNode>::TPtr node, TGrp* parent)
    {
        PLANNER_PROBE(Add, GetName(), parent? parent->GetName(): "NULL", Cast(node)->w0(), Cast(node)->GetName());
        AddImpl(node);
        OnAttach(Cast(node), parent);
    }

    template <class TNode>
    void Delete(TNode* node)
    {
        PLANNER_PROBE(Delete, GetName(), node->GetName());
        OnDetach(node);
        DeleteImpl(node);
    }

    void Clear()
    {
        Root->Clear();
        Accs.clear();
        // Clear Grps, but save Root
        for (auto i = Grps.begin(), e = Grps.end(); i != e;) {
            if (Cast(i->second) != Root) {
                Grps.erase(i++);
            } else {
                ++i;
            }
        }
    }

    TAccPtr FindAccountByName(const TString& name)
    {
        auto i = Accs.find(name);
        if (i == Accs.end()) {
            return TAccPtr();
        }
        return i->second;
    }

    TGrpPtr FindGroupByName(const TString& name)
    {
        auto i = Grps.find(name);
        if (i == Grps.end()) {
            return TGrpPtr();
        }
        return i->second;
    }

    size_t AccountsCount() const
    {
        return Accs.size();
    }

    size_t GroupsCount() const
    {
        return Grps.size();
    }

    const TAccs& Accounts() const
    {
        return Accs;
    }

    const TGrps& Groups() const
    {
        return Grps;
    }

    void GetSensors(TSharePlannerSensors& sensors, bool total, bool accounts, bool groups) const
    {
        if (accounts) {
            for (typename TAccs::const_iterator i = Accs.begin(), e = Accs.end(); i != e; ++i) {
                const TShareAccount& node = *Cast(i->second);
                TShareNodeSensors* pb = sensors.AddAccounts();
                pb->SetName(node.GetName());
                pb->SetParentName(node.GetParent()? node.GetParent()->GetName(): "");
                node.GetStats().FillSensorsPb(pb);
                FillNodeSensors(pb, node);
            }
        }
        if (groups) {
            for (typename TGrps::const_iterator i = Grps.begin(), e = Grps.end(); i != e; ++i) {
                const TShareGroup& node = *Cast(i->second);
                TShareNodeSensors* pb = sensors.AddGroups();
                pb->SetName(node.GetName());
                pb->SetParentName(node.GetParent()? node.GetParent()->GetName(): "");
                FillNodeSensors(pb, node);
            }
        }
        if (accounts && groups) {
            sensors.MutableNodes()->MergeFrom(sensors.GetAccounts());
            sensors.MutableNodes()->MergeFrom(sensors.GetGroups());
        }
        if (total) {
            TShareNodeSensors* pb = sensors.MutableTotal();
            Root->GetStats().FillSensorsPb(pb);

            size_t idleCount = 0;
            size_t activeCount = 0;
            size_t idleRetardCount = 0;
            size_t activeRetardCount = 0;
            size_t ac = 0, de = 0, re = 0, ot = 0;
            for (typename TAccs::const_iterator i = Accs.begin(), e = Accs.end(); i != e; ++i) {
                const TShareAccount& node = *Cast(i->second);
                bool active = node.IsActive();
                bool retard = node.IsRetard();
                idleCount += !active && !retard;
                activeCount += active && !retard;
                idleRetardCount += !active && retard;
                activeRetardCount += active && retard;
                ac += node.GetStats().Activations;
                de += node.GetStats().Deactivations;
                re += node.GetStats().Retardations;
                ot += node.GetStats().Overtakes;
            }
            pb->SetIdle(idleCount);
            pb->SetActive(activeCount);
            pb->SetIdleRetard(idleRetardCount);
            pb->SetActiveRetard(activeRetardCount);
            pb->SetActivations(ac);
            pb->SetDeactivations(de);
            pb->SetRetardations(re);
            pb->SetOvertakes(ot);
            sensors.SetResUsed(Stats.ResUsed);
            sensors.SetResWasted(Stats.ResWasted);
            //UtilMeter->GetSensors(sensors);
        }
    }
private:
    void AddImpl(TAccPtr node)
    {
        bool inserted = Accs.insert(typename TAccs::value_type(Cast(node)->GetName(), node)).second;
        Y_ABORT_UNLESS(inserted, "duplicate account name '%s'", Cast(node)->GetName().data());
    }

    void AddImpl(TGrpPtr node)
    {
        bool inserted = Grps.insert(typename TGrps::value_type(Cast(node)->GetName(), node)).second;
        Y_ABORT_UNLESS(inserted, "duplicate group name '%s'", Cast(node)->GetName().data());
    }

    void DeleteImpl(TAcc* node)
    {
        typename TAccs::iterator i = Accs.find(node->GetName());
        Y_ASSERT(i != Accs.end() && "trying to delete unknown/detached account");
        Accs.erase(i);
    }

    void DeleteImpl(TGrp* node)
    {
        typename TGrps::iterator i = Grps.find(node->GetName());
        Y_ASSERT(i != Grps.end() && "trying to delete unknown/detached group");
        Grps.erase(i);
    }

    template <class T>
    void FillNodeSensors(TShareNodeSensors* pb, const T& node) const
    {
        bool active = node.IsActive();
        bool retard = node.IsRetard();
        pb->SetIdle(!active && !retard);
        pb->SetActive(active && !retard);
        pb->SetIdleRetard(!active && retard);
        pb->SetActiveRetard(active && retard);
        pb->SetActivations(node.GetStats().Activations);
        pb->SetDeactivations(node.GetStats().Deactivations);
        pb->SetRetardations(node.GetStats().Retardations);
        pb->SetOvertakes(node.GetStats().Overtakes);
        TEnergy Eg = node.ComputeEg();
        pb->SetProficit(node.P(Eg));
        pb->SetLag(node.p(Eg));
        pb->SetDefShare(node.CalcGlobalShare());
        pb->SetGrpDefShare(node.s0());
        pb->SetDefWeight(node.w0());
        pb->SetMaxWeight(node.wmax());
        pb->SetDynamicWeight(node.w());
        if (const IContext* ctx = node.Ctx()) {
            ctx->FillSensors(*pb);
        }
        if (const TShareGroup* grp = dynamic_cast<const TShareGroup*>(&node)) {
            pb->SetTotalCredit(grp->GetTotalCredit());
        }
    }
};

}
