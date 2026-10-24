#pragma once
#include "defs.h"
#include "cbs_state.h"
#include "job.h"
#include <util/generic/intrlist.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NSchLab {

class TCbs {
public:
    ui64 CbsIdx;
    ui64 CbsNameIdx;
    TString CbsName;

    i64 MaxBudget; // Qs
    ui64 CbsDeadline; // dsk, related to Ps
    ECbsState State;
    i64 CurBudget; // qs
    ui64 Period;

    ui64 LastSeqNo;
    // Us = Qs/Ps - bandwidth

    ///////////////////////////////////////////////////////
    ui64 Weight;

    static const ui64 InvalidIdx = 65535;

    using TJobs = TIntrusiveList<TJob>;
    TJobs Jobs;
    ui64 JobsSize;
    ui64 JobsCost;
    ui8 OwnerIdx;
    ui8 GateIdx;
    bool IsNew;

public:
    TCbs();
    ~TCbs();
    TCbs(TCbs&& other) noexcept;
    TCbs& operator=(TCbs&& other) noexcept;
    TCbs(const TCbs&) = delete;
    TCbs& operator=(const TCbs&) = delete;

    void Reset(ui64 cbsIdx, ui8 ownerIdx, ui8 gateIdx);
    void PushJob(TIntrusivePtr<TJob> &job);
    TIntrusivePtr<TJob> PeekTailJob();
    TIntrusivePtr<TJob> PeekJob() {
        Y_ABORT_UNLESS(!Jobs.Empty());
        if (Jobs.Empty()) {
            return nullptr;
        }
        return Jobs.Front();
    }
    // Valid only while the queue retains the job; caller must not remove it.
    TJob* PeekJobBorrowed() {
        Y_ABORT_UNLESS(!Jobs.Empty());
        if (Jobs.Empty()) {
            return nullptr;
        }
        return Jobs.Front();
    }
    TIntrusivePtr<TJob> PopJob();
    bool IsEmpty() {
        return Jobs.Empty();
    }
    bool IsPresent() {
        return CbsIdx != InvalidIdx;
    }
protected:
    void Swap(TCbs& other) noexcept;
    void DeleteJobs();
};

} // NSchLab
} // NKikimr
