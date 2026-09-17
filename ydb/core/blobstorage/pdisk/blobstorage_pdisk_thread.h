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
    TPDiskThread(IPDisk &pDisk, std::optional<TCpuMask> affinity = std::nullopt)
        : TPDiskSimpleThread(std::move(affinity))
        , Quit(0)
        , IsEnded(0)
        , PDisk(pDisk)
    {}

private:
    void* DoThreadProc() override {
        ::TThread::SetCurrentThreadName("PDisk");
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
    IPDisk &PDisk;
};

} // NPDisk
} // NKikimr
