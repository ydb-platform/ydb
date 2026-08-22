#include "poller_tcp_unit.h"

#if !defined(_win_) && !defined(_darwin_)
#include "poller_tcp_unit_epoll.h"
#endif

#include "poller_tcp_unit_select.h"
#include "poller.h"

#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/util/intrinsics.h>

#if defined _linux_
#include <pthread.h>
#endif

namespace NInterconnect {
    TPollerUnit::TPtr
    TPollerUnit::Make(bool useSelect) {
#if defined(_win_) || defined(_darwin_)
        Y_UNUSED(useSelect);
        return TPtr(new TPollerUnitSelect);
#else
        return useSelect ? TPtr(new TPollerUnitSelect) : TPtr(new TPollerUnitEpoll);
#endif
    }

    TPollerUnit::TPollerUnit()
        : StopFlag(true)
        , ReadLoop(TThread::TParams(IdleThread<false>, this).SetName("network read"))
        , WriteLoop(TThread::TParams(IdleThread<true>, this).SetName("network write"))
    {
    }

    TPollerUnit::~TPollerUnit() {
        if (!AtomicLoad(&StopFlag))
            Stop();
    }

    void
    TPollerUnit::Start() {
        AtomicStore(&StopFlag, false);
        ReadLoop.Start();
        WriteLoop.Start();
    }

    void
    TPollerUnit::Stop() {
        AtomicStore(&StopFlag, true);
        ReadLoop.Join();
        WriteLoop.Join();
    }

    template <>
    TPollerUnit::TSide&
    TPollerUnit::GetSide<false>() {
        return Read;
    }

    template <>
    TPollerUnit::TSide&
    TPollerUnit::GetSide<true>() {
        return Write;
    }

    void
    TPollerUnit::StartReadOperation(
        const TIntrusivePtr<TSharedDescriptor>& stream,
        TFDDelegate&& operation) {
        Y_DEBUG_ABORT_UNLESS(stream);
        if (AtomicLoad(&StopFlag))
            return;
        GetSide<false>().InputQueue.Push(TSide::TItem(stream, std::move(operation)));
    }

    void
    TPollerUnit::StartWriteOperation(
        const TIntrusivePtr<TSharedDescriptor>& stream,
        TFDDelegate&& operation) {
        Y_DEBUG_ABORT_UNLESS(stream);
        if (AtomicLoad(&StopFlag))
            return;
        GetSide<true>().InputQueue.Push(TSide::TItem(stream, std::move(operation)));
    }

    template <bool IsWrite>
    void*
    TPollerUnit::IdleThread(void* param) {
    // TODO: musl-libc version of `sched_param` struct is for some reason different from pthread
    // version in Ubuntu 12.04
#if defined(_linux_) && !defined(_musl_)
        pthread_t threadSelf = pthread_self();
        sched_param sparam = {20};
        pthread_setschedparam(threadSelf, SCHED_FIFO, &sparam);
#endif

        static_cast<TPollerUnit*>(param)->RunLoop<IsWrite>();
        return nullptr;
    }

    template <>
    void
    TPollerUnit::RunLoop<false>() {
        NProfiling::TMemoryTagScope tag("INTERCONNECT_RECEIVED_DATA");
        while (!AtomicLoad(&StopFlag))
            ProcessRead();
    }

    template <>
    void
    TPollerUnit::RunLoop<true>() {
        NProfiling::TMemoryTagScope tag("INTERCONNECT_SEND_DATA");
        while (!AtomicLoad(&StopFlag))
            ProcessWrite();
    }

    void
    TPollerUnit::TSide::ProcessInput() {
        if (!InputQueue.IsEmpty())
            do {
                auto sock = InputQueue.Top().first->GetDescriptor();
                if (!Operations.emplace(sock, std::move(InputQueue.Top())).second)
                    Y_ABORT("Descriptor is already in pooler.");
            } while (InputQueue.Pop());
    }
}
