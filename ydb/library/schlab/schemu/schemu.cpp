#include "schemu.h"
#include <ydb/library/schlab/schine/job_kind.h>
#include <ydb/library/schlab/schoot/schoot_gen_cfg.h>
#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NSchLab {

TSchEmu::TSchEmu(TString jsonConfig) {
    Reset();
    TString json = Sprintf("{\"cfg\":%s}", jsonConfig.c_str());
    NSchLab::TSchOotGenCfgSet cfgSet(json);
    if (!cfgSet.IsValid) {
        Cerr << "Invalid cfgSet! " << cfgSet.ErrorDetails << Endl;
        Cerr << "BEGIN" << Endl;
        Cerr << jsonConfig << Endl;
        Cerr << "END" << Endl;
        GenSet.Reset(nullptr);
        return;
    }
    GenSet.Reset(new NSchLab::TSchOotGenSet(cfgSet));
}

void TSchEmu::Reset() {
    GenSet.Reset(nullptr);
    Scheduler.Reset(new NSchLab::TScheduler);
    TimeMs = 0.0;
    NextOwnerIdx = 0;
    NextGateIdx = 0;

    MibiByteCost = 1000000000ull / 100ull;
    SeekCost = 0ull;//8000000ull;
}

bool TSchEmu::GenJob(double &outTimeToNextReqMs) {
    ui64 reqSize = 0;
    TString user;
    TString desc;
    if (!GenSet) {
        return false;
    }
    bool isOk = GenSet->Step(TimeMs, &reqSize, &outTimeToNextReqMs, &user, &desc);
    if (!isOk) {
        return false;
    }
    if (!reqSize) {
        return true;
    }

    ui32 ownerIdx = 0;
    ui32 gateIdx = 0;
    auto ownerIt = OwnerForUser.find(user);
    if (ownerIt == OwnerForUser.end()) {
        ownerIdx = GetNextOwnerIdx();
        OwnerForUser[user] = ownerIdx;
    } else {
        ownerIdx = ownerIt->second;
    }
    auto gateIt = GateForDesc.find(desc);
    if (gateIt == GateForDesc.end()) {
        gateIdx = GetNextGateIdx();
        GateForDesc[desc] = gateIdx;
    } else {
        gateIdx = gateIt->second;
    }

    ui64 timeNs = MsToNs(TimeMs);

    if (Scheduler->GetCbs(ownerIdx, gateIdx) == nullptr) {
        TCbs cbs;
        TStringStream str;
        str << user << "_" << desc;
        cbs.CbsName = str.Str();
        cbs.Weight = 1;
        cbs.MaxBudget = 1000ull;
        cbs.CurBudget = cbs.MaxBudget;
        cbs.Period = 2000ull;

        Scheduler->AddCbs(ownerIdx, gateIdx, cbs, timeNs);
        TimeMs += 0.000001;
        timeNs = MsToNs(TimeMs);
    }

    TIntrusivePtr<TJob> job = new TJob();
    job->JobKind = JobKindRequest;
    job->Cost = EstimateCost(reqSize);

    isOk = Scheduler->AddJob(job, ownerIdx, gateIdx, timeNs);
    TimeMs += 0.000001;

    Y_ABORT_UNLESS(isOk);
    return true;
}

void TSchEmu::Emulate(double durationMs) {
    double endTimeMs = TimeMs + durationMs;
    while (TimeMs < endTimeMs) {
        double timeToNextReqMs = 0.0;
        bool isOk = GenJob(timeToNextReqMs);
        if (!isOk) {
            Cerr << "GenJob not ok" << Endl;
            return;
        }
        if (timeToNextReqMs > 0.0) {
            double genTimeStepMs = Max(timeToNextReqMs, 1.0);

            ui64 timeNs = MsToNs(TimeMs);
            TIntrusivePtr<TJob> job = Scheduler->SelectJob(timeNs);
            if (job) {
                double jobCost = (double)job->Cost;
                double jobDurationMs = NsToMs(jobCost);

                double schTimeStepMs = Max(1.0, jobDurationMs);
                TimeMs += schTimeStepMs;

                timeNs = MsToNs(TimeMs);
                Scheduler->CompleteJob(timeNs, job);
            } else {
                TimeMs += Min(0.1, genTimeStepMs);
            }
        }
    }
    TStringStream str;
    Scheduler->OutputLog(str);
}

void TSchEmu::OutputLogJson(IOutputStream &out) {
    if (Scheduler) {
        Scheduler->OutputLog(out);
    }
    return;
}

bool TSchEmu::IsOk() {
    if (GenSet.Get() == nullptr) {
        return false;
    }
    return true;
}

ui64 TSchEmu::EstimateCost(ui64 size) {
    return ((MibiByteCost * size) >> 20) + SeekCost;
}

TString TSchEmu::GetErrorReason() {
    return ErrorReason;
}

ui32 TSchEmu::GetNextOwnerIdx() {
    ui32 idx = NextOwnerIdx;
    NextOwnerIdx++;
    return idx;
}

ui32 TSchEmu::GetNextGateIdx() {
    ui32 idx = NextGateIdx;
    NextGateIdx++;
    return idx;
}

ui64 TSchEmu::MsToNs(double ms) {
    return (ui64)(ms * 1000000.0);
}

double TSchEmu::NsToMs(ui64 ns) {
    return ns / 1000000.0;
}

double TSchEmu::NsToMs(double ns) {
    return ns / 1000000.0;
}


}; // NSchLab
}; // NKikimr
