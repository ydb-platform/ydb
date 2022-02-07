#pragma once
#include "defs.h"
#include <ydb/library/schlab/schoot/schoot_gen.h>
#include <ydb/library/schlab/schine/scheduler.h>
#include <util/generic/string.h>
#include <unordered_map>

namespace NKikimr {
namespace NSchLab {

class TSchEmu {
public:
    TSchEmu(TString jsonConfig);
    void Reset();
    bool GenJob(double &outTimeToNextReqMs);
    void Emulate(double durtaionMs);
    void OutputLogJson(IOutputStream &out);
    bool IsOk();
    TString GetErrorReason();

    ui64 EstimateCost(ui64 size);
protected:
    THolder<NSchLab::TSchOotGenSet> GenSet;
    THolder<TScheduler> Scheduler;
    double TimeMs;
    TString ErrorReason;
    std::unordered_map<TString, ui32> OwnerForUser;
    std::unordered_map<TString, ui32> GateForDesc;
    ui32 NextOwnerIdx;
    ui32 NextGateIdx;

    ui32 GetNextOwnerIdx();
    ui32 GetNextGateIdx();

    static ui64 MsToNs(double ms);
    static double NsToMs(ui64 ns);
    static double NsToMs(double ns);

    ui64 MibiByteCost;
    ui64 SeekCost;
};

}; // NSchLab
}; // NKikimr
