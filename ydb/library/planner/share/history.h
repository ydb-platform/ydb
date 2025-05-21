#pragma once

#include "shareplanner.h"
#include <ydb/library/planner/share/protos/shareplanner_history.pb.h>
#include "node_visitor.h"
#include "apply.h"
#include "probes.h"

namespace NScheduling {

class THistorySaver : public IConstNodeVisitor
{
protected:
    TSharePlannerHistory History;
    TEnergy Eg = 0;
public:
    static void Save(const TSharePlanner* planner, IOutputStream& os)
    {
        PLANNER_PROBE(SerializeHistory, planner->GetName());
        THistorySaver v;
        planner->GetRoot()->Accept(&v);
        v.History.SerializeToArcadiaStream(&os);
    }
protected:
    void Visit(const TShareAccount* account) override
    {
        SaveNode(account);
    }

    void Visit(const TShareGroup* group) override
    {
        SaveNode(group);
        // Recurse into children
        TEnergy Eg_stack = Eg;
        Eg = 0;
        ApplyTo<const TShareNode>(group, [=, this] (const TShareNode* node) {
            Eg += node->E();
        });
        group->AcceptInChildren(this);
        Eg = Eg_stack;
    }

    void SaveNode(const TShareNode* node)
    {
        TShareNodeHistory* nh = History.AddNodes();
        nh->SetName(node->GetName());
        nh->SetProficit(node->P(Eg));
    }
};

class THistoryLoader: public INodeVisitor
{
protected:
    THashMap<TString, const TShareNodeHistory*> NHMap;
    TSharePlannerHistory History;
    TSharePlanner* Planner;
public:
    static bool Load(TSharePlanner* planner, IInputStream& is)
    {
        THistoryLoader v(planner);
        return v.LoadFrom(is);
    }
protected:
    explicit THistoryLoader(TSharePlanner* planner)
        : Planner(planner)
    {}

    bool LoadFrom(IInputStream& is)
    {
        PLANNER_PROBE(DeserializeHistoryBegin, Planner->GetName());
        bool success = false;
        if (History.ParseFromArcadiaStream(&is)) {
            for (size_t i = 0; i < History.NodesSize(); i++) {
                const TShareNodeHistory& nh = History.GetNodes(i);
                NHMap[nh.GetName()] = &nh;
            }

            if (OnHistoryDeserialized()) {
                //UtilMeter->Load(History.GetUtilization()); // Load utilization history
                Planner->GetRoot()->Accept(this);
                success = true;
            }
        }
        PLANNER_PROBE(DeserializeHistoryEnd, Planner->GetName(), success);
        return success;
    }

    void Visit(TShareAccount* account) override
    {
        LoadNode(account);
    }

    void Visit(TShareGroup* group) override
    {
        LoadNode(group);
        group->AcceptInChildren(this);
    }

    void LoadNode(TShareNode* node)
    {
        auto i = NHMap.find(node->GetName());
        TEnergy Dnew = 0;
        if (i != NHMap.end()) {
            const TShareNodeHistory& nh = *i->second;
            Dnew = nh.GetProficit();
        }
        node->SetS(0);
        node->SetD(Dnew);
    }

    virtual bool OnHistoryDeserialized() { return true; }
};

}
