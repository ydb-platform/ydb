#pragma once
#include "defs.h"
#include "cbs_state.h"
#include "job.h"
#include <util/generic/list.h>
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

    typedef TList<TIntrusivePtr<TJob>> TJobs;
    TJobs Jobs;
    ui64 JobsSize;
    ui64 JobsCost;
    ui8 OwnerIdx;
    ui8 GateIdx;
    bool IsNew;

public:
    TCbs();
    ~TCbs();

    void Reset(ui64 cbsIdx, ui8 ownerIdx, ui8 gateIdx);
    void PushJob(TIntrusivePtr<TJob> &job);
    TIntrusivePtr<TJob> PeekTailJob();
    TIntrusivePtr<TJob> PeekJob();
    TIntrusivePtr<TJob> PopJob();
    bool IsEmpty();
    bool IsPresent();
protected:
    void DeleteJobs();
};

} // NSchLab
} // NKikimr
