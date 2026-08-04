#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/backpressure/queue.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/system/event.h>
#include <util/system/yassert.h>

namespace {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsQueue;

constexpr size_t MaxOperations = 128;
constexpr size_t MaxPayload = 512;

class TNullActor final : public TActorBootstrapped<TNullActor> {
public:
    void Bootstrap(const TActorContext&) {
        Become(&TNullActor::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TSystem::Poison, PassAway)
        default:
            break;
        }
    }
};

struct TSentItem {
    ui64 SequenceId = 0;
    ui64 MsgId = 0;
};

class TQueueHarnessActor final : public TActorBootstrapped<TQueueHarnessActor> {
public:
    TQueueHarnessActor(TString bytes, TAutoEvent* done, TActorId sender, TActorId remote)
        : Bytes(std::move(bytes))
        , Done(done)
        , Sender(sender)
        , Remote(remote)
    {}

    void Bootstrap(const TActorContext& ctx) {
        FuzzedDataProvider fdp(reinterpret_cast<const ui8*>(Bytes.data()), Bytes.size());
        Run(ctx, fdp);
        Done->Signal();
        Die(ctx);
    }

private:
    TEvBlobStorage::TEvVPut::TPtr MakePut(FuzzedDataProvider& fdp, ui64 cookie) {
        const ui32 payloadSize = fdp.ConsumeIntegralInRange<ui32>(0, MaxPayload);
        TString payload(payloadSize, '\0');
        for (char& ch : payload) {
            ch = static_cast<char>(fdp.ConsumeIntegral<ui8>());
        }

        const TLogoBlobID blobId(
            0x0123456789abcdefULL,
            fdp.ConsumeIntegralInRange<ui32>(1, 16),
            static_cast<ui32>(cookie),
            0,
            1,
            payload.size());
        auto* event = new TEvBlobStorage::TEvVPut(
            blobId,
            TRope(payload),
            VDiskId,
            fdp.ConsumeBool(),
            nullptr,
            TInstant::Max(),
            NKikimrBlobStorage::EPutHandleClass::TabletLog,
            fdp.ConsumeBool());

        TAutoPtr<IEventHandle> handle = new IEventHandle(Remote, Sender, event, 0, cookie);
        return IEventHandle::Downcast<TEvBlobStorage::TEvVPut>(std::move(handle));
    }

    void CheckCounters(TBlobStorageQueue& queue, ui64 waiting, ui64 inFlight) {
        Y_ABORT_UNLESS(queue.GetItemsWaiting() == waiting);
        Y_ABORT_UNLESS(queue.InFlightCount() == inFlight);
        Y_ABORT_UNLESS(static_cast<ui64>(queue.QueueWaitingItems->Val()) == waiting);
        Y_ABORT_UNLESS(static_cast<ui64>(queue.QueueInFlightItems->Val()) == inFlight);
    }

    void Run(const TActorContext& ctx, FuzzedDataProvider& fdp) {
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        TBSProxyContextPtr bsProxyCtx = new TBSProxyContext(counters);
        TString logPrefix = "blobstorage_queue_stateful";
        TBlobStorageQueue queue(
            counters,
            logPrefix,
            bsProxyCtx,
            NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::DSProxy, 1),
            0,
            TBlobStorageGroupType::ErasureNone,
            NMonitoring::TCountableBase::EVisibility::Public,
            true);

        TDeque<TSentItem> inFlight;
        ui64 waiting = 0;
        ui64 nextCookie = 1;
        ui64 nextMsgId = 0;
        ui64 sequenceId = 1;

        for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
            switch (fdp.ConsumeIntegralInRange<unsigned>(0, 7)) {
                case 0:
                case 1: {
                    auto ev = MakePut(fdp, nextCookie++);
                    queue.Enqueue(ctx, ev, TInstant::Max(), true);
                    ++waiting;
                    break;
                }

                case 2: {
                    if (fdp.ConsumeBool()) {
                        queue.OnConnect();
                    } else {
                        queue.SetMaxWindowSize(fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000));
                    }
                    break;
                }

                case 3: {
                    const ui64 beforeInFlight = queue.InFlightCount();
                    const ui64 beforeWaiting = queue.GetItemsWaiting();
                    queue.SendToVDisk(ctx, Remote, 0);
                    const ui64 sent = queue.InFlightCount() - beforeInFlight;
                    Y_ABORT_UNLESS(beforeWaiting >= queue.GetItemsWaiting());
                    Y_ABORT_UNLESS(beforeWaiting - queue.GetItemsWaiting() == sent);
                    for (ui64 i = 0; i < sent; ++i) {
                        inFlight.push_back({sequenceId, nextMsgId++});
                    }
                    waiting -= sent;
                    break;
                }

                case 4: {
                    if (!inFlight.empty()) {
                        const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, inFlight.size() - 1);
                        const TSentItem item = inFlight[index];
                        TActorId outSender;
                        ui64 outCookie = 0;
                        TDuration processingTime;
                        queue.OnResponse(item.MsgId, item.SequenceId, 0, &outSender, &outCookie, &processingTime);
                        Y_ABORT_UNLESS(outSender == Sender);
                        inFlight.erase(inFlight.begin() + index);
                    }
                    break;
                }

                case 5: {
                    if (!inFlight.empty()) {
                        const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, inFlight.size() - 1);
                        const TSentItem failed = inFlight[index];
                        nextMsgId = fdp.ConsumeIntegralInRange<ui64>(0, 64);
                        sequenceId += 1 + fdp.ConsumeIntegralInRange<ui64>(0, 4);
                        queue.Unwind(failed.MsgId, failed.SequenceId, nextMsgId, sequenceId);
                        waiting += inFlight.size() - index;
                        inFlight.erase(inFlight.begin() + index, inFlight.end());
                    }
                    break;
                }

                case 6: {
                    queue.DrainQueue(NKikimrProto::ERROR, "fuzz drain", ctx);
                    waiting = 0;
                    inFlight.clear();
                    break;
                }

                case 7: {
                    queue.GetBytesWaiting();
                    queue.GetInFlightCost();
                    queue.GetWorstRequestProcessingTime();
                    queue.GetCostModel();
                    break;
                }
            }

            CheckCounters(queue, waiting, inFlight.size());
        }

        queue.DrainQueue(NKikimrProto::ERROR, "fuzz final drain", ctx);
    }

private:
    TString Bytes;
    TAutoEvent* Done;
    TActorId Sender;
    TActorId Remote;
    TVDiskID VDiskId{0, 1, 0, 0, 0};
};

THolder<TActorSystemSetup> MakeSetup() {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 20));
    setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(128, 10)));
    return setup;
}

void RunBlobStorageQueueFuzz(const ui8* data, size_t size) {
    if (size > 4096) {
        size = 4096;
    }

    const TActorId sender{1, "sender"};
    const TActorId remote{1, "remote"};
    TAutoEvent done;

    auto setup = MakeSetup();
    setup->LocalServices.emplace_back(sender, TActorSetupCmd(new TNullActor, TMailboxType::Simple, 0));
    setup->LocalServices.emplace_back(remote, TActorSetupCmd(new TNullActor, TMailboxType::Simple, 0));
    setup->LocalServices.emplace_back(TActorId{1, "harness"}, TActorSetupCmd(
        new TQueueHarnessActor(TString(reinterpret_cast<const char*>(data), size), &done, sender, remote),
        TMailboxType::Simple,
        0));

    TAppData appData(0, 1, 0, 1, {}, nullptr, nullptr, nullptr, nullptr);
    TActorSystem actorSystem(setup, &appData);
    actorSystem.Start();
    Y_ABORT_UNLESS(done.WaitT(TDuration::Seconds(10)));
    actorSystem.Stop();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    RunBlobStorageQueueFuzz(data, size);
    return 0;
}
