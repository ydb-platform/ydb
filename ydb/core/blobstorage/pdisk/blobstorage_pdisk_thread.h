#pragma once
#include "defs.h"
#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_abstract.h"
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/thread.h>
#include <util/system/thread.h>

#include <optional>
#include <utility>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Thread
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDiskThread : public TThread {
public:
    TPDiskThread(IPDisk &pDisk, std::optional<TCpuMask> affinity = std::nullopt)
        : TThread(&ThreadProc, this)
        , Affinity(std::move(affinity))
        , Quit(0)
        , IsEnded(0)
        , PDisk(pDisk)
    {}

    static void* ThreadProc(void* _this) {
        SetCurrentThreadName("PDisk");

        auto *thread = static_cast<TPDiskThread*>(_this);
        TAffinityGuard affinityGuard(thread->Affinity ? &*thread->Affinity : nullptr);
        thread->Exec();
        return nullptr;
    }

    void Exec() {
        while (!AtomicGet(Quit)) {
            PDisk.Update();
        }
        AtomicSet(IsEnded, 1);
    }

    void Stop() {
        AtomicSet(Quit, 1);
        PDisk.Wakeup();
    }

    void StopSync() {
        Stop();
        while (!AtomicGet(IsEnded)) {
            SpinLockPause();
        }
    }

private:
    std::optional<TCpuMask> Affinity;
    TAtomic Quit;
    TAtomic IsEnded;
    IPDisk &PDisk;
};

} // NPDisk
} // NKikimr
