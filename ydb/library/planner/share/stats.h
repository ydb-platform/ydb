#pragma once

#include <ydb/library/planner/base/defs.h>
#include <ydb/library/planner/share/protos/shareplanner_sensors.pb.h>

namespace NScheduling {

struct TNodeStats {
    ui64 NDone = 0;
    ui64 NSpoiled = 0;
    TEnergy ResDone = 0;
    TEnergy ResSpoiled = 0;
    TEnergy MoneySpent = 0;
    ui64 Activations = 0;
    ui64 Deactivations = 0;
    ui64 Retardations = 0;
    ui64 Overtakes = 0;

    void Done(TEnergy cost, size_t queries = 1)
    {
        NDone += queries;
        ResDone += cost;
    }

    void Spoil(TEnergy cost, size_t queries = 1)
    {
        NSpoiled += queries;
        ResSpoiled += cost;
    }

    void Pay(TEnergy bill)
    {
        MoneySpent += bill;
    }

    void FillSensorsPb(TShareNodeSensors* sensors) const
    {
        sensors->SetNDone(NDone);
        sensors->SetNSpoiled(NSpoiled);
        sensors->SetResDone(ResDone);
        sensors->SetResSpoiled(ResSpoiled);
        sensors->SetMoneySpent(MoneySpent);
        sensors->SetActivations(Activations);
        sensors->SetDeactivations(Deactivations);
    }
};

}
