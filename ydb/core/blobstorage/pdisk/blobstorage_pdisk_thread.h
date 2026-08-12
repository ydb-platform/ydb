#pragma once
#include "defs.h"
#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_internal_interface.h"
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/thread.h>
#include <util/system/thread.h>

#include <optional>
#include <utility>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk native thread
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDiskSimpleThread : private ISimpleThread {
public:
    explicit TPDiskSimpleThread(std::optional<TCpuMask> affinity = std::nullopt)
        : Affinity(std::move(affinity))
    {}

    void Start() {
        ISimpleThread::Start();
    }

    void* Join() {
        return ISimpleThread::Join();
    }

    bool Running() const noexcept {
        return ISimpleThread::Running();
    }

    ::TThread::TId Id() const noexcept {
        return ISimpleThread::Id();
    }

protected:
    virtual void* DoThreadProc() = 0;

private:
    // The thread applies its own affinity: mutating the creator's affinity for inheritance
    // would race with other affinity writers (e.g. TExecutorPoolJail) when the creator is
    // an executor pool thread.
    void* ThreadProc() final {
        if (Affinity && !Affinity->IsEmpty()) {
            TAffinity(*Affinity).Set();
        }
        return DoThreadProc();
    }

    const std::optional<TCpuMask> Affinity;
};

class TPDiskThreadStatsRegistration {
public:
    TPDiskThreadStatsRegistration(
            const std::shared_ptr<TPDiskCtx>& pCtx,
            TString threadName)
        : PCtx(pCtx)
        , ThreadName(std::move(threadName))
    {
        if (PCtx->ActorSystem && PCtx->PDiskActor) {
            ThreadId = ::TThread::CurrentThreadNumericId();
            Send(TEvPDiskThreadLifecycle::EAction::Register);
        }
    }

    ~TPDiskThreadStatsRegistration() {
        if (ThreadId) {
            Send(TEvPDiskThreadLifecycle::EAction::Unregister);
        }
    }

    TPDiskThreadStatsRegistration(const TPDiskThreadStatsRegistration&) = delete;
    TPDiskThreadStatsRegistration& operator=(const TPDiskThreadStatsRegistration&) = delete;

private:
    void Send(TEvPDiskThreadLifecycle::EAction action) const {
        PCtx->ActorSystem->Send(PCtx->PDiskActor, new TEvPDiskThreadLifecycle(
            ThreadName, ThreadId, action));
    }

    const std::shared_ptr<TPDiskCtx> PCtx;
    const TString ThreadName;
    ui64 ThreadId = 0;
};

class TPDiskFunctionThread final : public TPDiskSimpleThread {
public:
    TPDiskFunctionThread(::TThread::TThreadProc threadProc, void* cookie,
            std::optional<TCpuMask> affinity = std::nullopt)
        : TPDiskSimpleThread(std::move(affinity))
        , ThreadProcFunction(threadProc)
        , Cookie(cookie)
    {}

    ~TPDiskFunctionThread() {
        Join();
    }

private:
    void* DoThreadProc() override {
        return ThreadProcFunction(Cookie);
    }

    const ::TThread::TThreadProc ThreadProcFunction;
    void* const Cookie;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk Thread
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDiskThread : public TPDiskSimpleThread {
public:
    TPDiskThread(IPDisk& pDisk,
            const std::shared_ptr<TPDiskCtx>& pCtx,
            std::optional<TCpuMask> affinity = std::nullopt)
        : TPDiskSimpleThread(std::move(affinity))
        , Quit(0)
        , IsEnded(0)
        , PDisk(pDisk)
        , PCtx(pCtx)
    {}

private:
    void* DoThreadProc() override {
        ::SetCurrentThreadName("PDisk");
        TPDiskThreadStatsRegistration registration(PCtx, "main");
        Exec();
        return nullptr;
    }

public:
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
    TAtomic Quit;
    TAtomic IsEnded;
    IPDisk& PDisk;
    const std::shared_ptr<TPDiskCtx> PCtx;
};

} // NPDisk
} // NKikimr
