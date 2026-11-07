#pragma once
#include "defs.h"
#include <util/generic/intrlist.h>
#include "job_state.h"
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchLab {

class TJobPool;
struct TJob;
struct TRecycleJob {
    static void Destroy(TJob* job) noexcept;
};

struct TJob : TSimpleRefCount<TJob, TRecycleJob>, TIntrusiveListItem<TJob> {
    void *Payload = nullptr;
    ui64 CbsIdx = 0;

    ui64 Id = 0;
    ui64 JobKind = 0;

    ui64 AccountCbsIdx = 65535;

    i64 Cost = 0;
    EJobState State = JobStatePending;
    ui64 SeqNo = 0;
    ui64 Deadline = 0;
    double WeightedInverseCost = 0.0;

    bool IsNew = true;

private:
    TJobPool* Pool = nullptr;
    friend class TJobPool;
    friend struct TRecycleJob;
};

// Same single-thread ownership as the scheduler and TJob refcount.
// Outstanding jobs keep their pool alive even after scheduler destruction.
class TJobPool : public TSimpleRefCount<TJobPool> {
    TIntrusiveList<TJob> Free;
    ui64 Created = 0;
public:
    ~TJobPool();
    TIntrusivePtr<TJob> Acquire();
    void Release(TJob* job) noexcept;
    ui64 GetCreated() const { return Created; }
};

} // NSchLab
} // NKikimr
