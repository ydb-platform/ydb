#pragma once
#include "defs.h"
#include "cbs.h"
#include "name_table.h"
#include "schlog.h"
#include "schlog_frame.h"
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/random/fast.h>

namespace NKikimr {
namespace NSchLab {

//                               /-relative weight of the gate, to split the resource between all gates
//           ownerIdx  gateIdx weight cbsIdx -- inner index of the cbs
//                         \-per-owner gete identifier
//
// scheduler - owner1 - gate1 (w11) - cbs0
//      .    .        - gate2 (w12) - cbs1
//     EDF+  .        - gate3 (w13) - cbs2
//           .
//           - owner2 - gate1 (w21) - cbs3
//                    - gate2 (w22) - cbs4
//                    - gate3 (w23) - cbs5
//
//
// cbs - job1
//     - job2
//     - job3
//
//
// ("user", "desc") -> (ownerIdx, gateIdx) -> cbsIdx
//
// OwnerForUser : "user" -> ownerIdx
// GateForDesc: "desc" -> gateIdx

struct TGateRecord {
    ui16 CbsIdx = TCbs::InvalidIdx;
};

struct TOwnerRecord {
    TVector<TGateRecord> Gates;
};

// Single-threaded. Registered CBS queues and state are managed by the scheduler.
class TScheduler {
    friend struct TSchedulerTest;
    TIntrusivePtr<TJobPool> JobPool = new TJobPool;
    TVector<TCbs> Cbs;
    // Sorted indices preserve EDF ties and weighted fallback iteration order.
    TVector<ui32> NonEmptyCbs;
    ui32 ActiveRunningCount = 0;

    static bool IsActiveRunning(ECbsState state) {
        return state == CbsStateActive || state == CbsStateRunning;
    }

    void SetCbsState(TCbs& cbs, ECbsState state) {
        ActiveRunningCount += ui32(IsActiveRunning(state));
        ActiveRunningCount -= ui32(IsActiveRunning(cbs.State));
        cbs.State = state;
    }
    TNameTable NameTable;
    ui64 TotalWeight = 0;
    ui32 FirstEmptyCbs = 0;
    TVector<TOwnerRecord> Owners;
    ui64 NextJobId = 0;
    TVector<TIntrusivePtr<TJob>> EdfJobs;
    TReallyFastRng32 Rng;

    TSchLog SchLog;
    bool IsBinLogEnabled = true;
    bool IsProbeRegistrationDone = false;
    const ui64 MaxDelay = 23000000ull; // TODO(cthulhu): Evaluate it on the fly

    double Uact = 0;

    bool HasAtLeastOneJob = true;

public:
    TScheduler();
    void SetIsBinLogEnabled(bool is_enabled);
    // Returned pointers are invalidated by AddCbs. Only configuration (e.g. Weight)
    // may be changed directly; use scheduler methods to change jobs and state.
    TCbs* GetCbs(ui8 ownerIdx, ui8 gateIdx);
    // Registers an empty, idle CBS. Jobs must be submitted through AddJob.
    void AddCbs(ui8 ownerIdx, ui8 gateIdx, TCbs newCbs, ui64 timeNs);
    TIntrusivePtr<TJob> CreateJob();
    void AddJob(TCbs *cbs, TIntrusivePtr<TJob> &job, ui8 ownerIdx, ui8 gateIdx, ui64 timeNs);
    bool AddJob(TIntrusivePtr<TJob> &job, ui8 ownerIdx, ui8 gateIdx, ui64 timeNs);
    ui64 MakeNextJobId();
    void OnJobCostChange(TCbs *cbs, TIntrusivePtr<TJob> &job, ui64 timeNs, ui64 prevCost);
    // Borrowed until CompleteJob finishes or scheduler/queue is destroyed.
    TJob* SelectJob(ui64 timeNs);
    void UpdateTotalWeight();
    void EvaluateNextJob(ui64 cbsIdx, ui64 timeNs, bool doLogCbsUpdate);
    void CompleteJob(ui64 timeNs, TJob* job);
    void AddSchLogFrame(ui64 timeNs);
    void SchLogKeyframe(ui64 timeNs);
    void SchLogCbsKeyframeRecord(const TCbs &cbs);
    void OutputLog(IOutputStream &out);
    bool IsEmpty();
protected:
    ui32 NewCbs();
};

} // NSchLab
} // NKikimr
