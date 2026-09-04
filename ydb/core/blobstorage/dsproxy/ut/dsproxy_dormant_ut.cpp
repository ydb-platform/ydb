#include "defs.h"
#include "dsproxy_env_mock_ut.h"

#include <ydb/core/blobstorage/nodewarden/group_stat_aggregator.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>

namespace NKikimr {
namespace {

struct TProxyState {
    TIntrusivePtr<TGroupQueues> GroupQueues;
    bool IsDormant;
};

class TEventSink : public TActor<TEventSink> {
public:
    TEventSink()
        : TActor(&TThis::StateFunc)
    {}

    STFUNC(StateFunc) {
        Y_UNUSED(ev);
    }
};

TProxyState QueryProxyState(TTestActorRuntime& runtime, const TDSProxyEnv& env) {
    runtime.Send(new IEventHandle(env.RealProxyActorId, env.FakeProxyActorId,
        new TEvRequestProxySessionsState));
    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvProxySessionsState>(handle);
    UNIT_ASSERT(response);
    return {
        .GroupQueues = response->GroupQueues,
        .IsDormant = response->IsDormant,
    };
}

void AdvanceTime(TTestActorRuntime& runtime, TDuration duration) {
    runtime.AdvanceCurrentTime(duration);
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
}

void AdvanceSeconds(TTestActorRuntime& runtime, ui32 seconds) {
    for (ui32 i = 0; i < seconds; ++i) {
        AdvanceTime(runtime, TDuration::Seconds(1));
    }
}

void AdvancePastOneMinute(TTestActorRuntime& runtime) {
    AdvanceSeconds(runtime, TDuration::Minutes(1).Seconds() + 1);
}

TVector<TActorId> GetQueueActorIds(const TIntrusivePtr<TGroupQueues>& groupQueues) {
    TVector<TActorId> result;
    for (const TGroupQueues::TVDisk *disk : groupQueues->DisksByOrderNumber) {
        disk->Queues.ForEachQueue([&](const auto& queue) {
            result.push_back(queue.ActorId);
        });
    }
    return result;
}

void DisableQueueSchedules(TTestActorRuntime& runtime, const TIntrusivePtr<TGroupQueues>& groupQueues) {
    for (ui32 attempt = 0; attempt < 1000; ++attempt) {
        bool allConnected = true;
        for (const TGroupQueues::TVDisk *disk : groupQueues->DisksByOrderNumber) {
            disk->Queues.ForEachQueue([&](const auto& queue) {
                allConnected &= queue.IsConnected.load(std::memory_order_relaxed);
            });
        }
        if (allConnected) {
            break;
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_C(attempt != 999, "DSProxy queues failed to connect");
    }

    for (const TActorId& actorId : GetQueueActorIds(groupQueues)) {
        runtime.EnableScheduleForActor(actorId, false);
    }
}

void SendGetBlock(TTestActorRuntime& runtime, const TDSProxyEnv& env, TInstant deadline = TInstant::Max()) {
    runtime.Send(new IEventHandle(env.RealProxyActorId, env.FakeProxyActorId,
        new TEvBlobStorage::TEvGetBlock(12345, deadline)));
}

void SendInvalidPut(TTestActorRuntime& runtime, const TDSProxyEnv& env) {
    runtime.Send(new IEventHandle(env.RealProxyActorId, env.FakeProxyActorId,
        new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 0, 0, 0), TString(), TInstant::Max())));
}

void RegisterGroupStatSinks(TTestActorRuntime& runtime, const TDSProxyEnv& env) {
    const TActorId sink = runtime.Register(new TEventSink, env.NodeIdx);
    runtime.RegisterService(MakeBlobStorageNodeWardenID(runtime.GetNodeId(env.NodeIdx)), sink, env.NodeIdx);
    for (const TActorId& vdiskId : env.VDisks) {
        runtime.RegisterService(MakeGroupStatAggregatorId(vdiskId), sink, env.NodeIdx);
    }
}

Y_UNIT_TEST_SUITE(TDSProxyDormantTest) {

    Y_UNIT_TEST(EntersDormantAndWakesWithoutRecreatingQueues) {
        TTestBasicRuntime runtime(1, false);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        SetupRuntime(runtime);

        TControlWrapper dormantTimeoutMinutes(1, 0, 1'000'000);
        TDSProxyEnv env;
        env.Configure(runtime, TBlobStorageGroupType(TBlobStorageGroupType::ErasureMirror3dc), 0, 0,
            TBlobStorageGroupInfo::EEM_ENC_V1, dormantTimeoutMinutes, true);
        DisableQueueSchedules(runtime, env.GroupQueues);

        std::atomic_bool sawDormantDeadlineWakeup = false;
        std::atomic_bool watchActiveDeadlineCadence = false;
        std::atomic_bool sawActiveDeadlineCadenceAfterWake = false;
        runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event,
                TDuration delay, TInstant& deadline) {
            if (event->GetRecipientRewrite() == env.ProxyActorId) {
                if (delay == TDuration::Minutes(1)) {
                    sawDormantDeadlineWakeup.store(true, std::memory_order_relaxed);
                } else if (watchActiveDeadlineCadence.load(std::memory_order_relaxed)
                        && delay == TDuration::Seconds(1)) {
                    sawActiveDeadlineCadenceAfterWake.store(true, std::memory_order_relaxed);
                }
            }
            return ScheduledFilterFunc(runtime, event, delay, deadline);
        });

        const TProxyState initial = QueryProxyState(runtime, env);
        UNIT_ASSERT(!initial.IsDormant);
        const TVector<TActorId> queueActorIds = GetQueueActorIds(initial.GroupQueues);
        const auto stateCounters = GetServiceCounters(runtime.GetAppData(env.NodeIdx).Counters, "dsproxy")
            ->GetSubgroup("blobstorageproxy", Sprintf("%09" PRIu64, env.GroupId))
            ->GetSubgroup("subsystem", "state");
        const auto dormantCounter = stateCounters->GetCounter("IsDormant");
        const auto activeCounter = stateCounters->GetCounter("IsActive");
        const auto transitionCounter = stateCounters->GetCounter("DormancyTransitions", true);
        UNIT_ASSERT_VALUES_EQUAL(dormantCounter->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(activeCounter->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(transitionCounter->Val(), 0);

        AdvancePastOneMinute(runtime);
        const TProxyState dormant = QueryProxyState(runtime, env);
        UNIT_ASSERT(dormant.IsDormant);
        UNIT_ASSERT_VALUES_EQUAL(dormantCounter->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(activeCounter->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(transitionCounter->Val(), 1);
        UNIT_ASSERT(!sawDormantDeadlineWakeup.load(std::memory_order_relaxed));
        UNIT_ASSERT_VALUES_EQUAL(dormant.GroupQueues.Get(), initial.GroupQueues.Get());
        UNIT_ASSERT_VALUES_EQUAL(GetQueueActorIds(dormant.GroupQueues), queueActorIds);

        watchActiveDeadlineCadence.store(true, std::memory_order_relaxed);
        SendGetBlock(runtime, env);
        const TProxyState awake = QueryProxyState(runtime, env);
        UNIT_ASSERT(!awake.IsDormant);
        UNIT_ASSERT_VALUES_EQUAL(dormantCounter->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(activeCounter->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(transitionCounter->Val(), 2);
        UNIT_ASSERT(sawActiveDeadlineCadenceAfterWake.load(std::memory_order_relaxed));
        UNIT_ASSERT_VALUES_EQUAL(awake.GroupQueues.Get(), initial.GroupQueues.Get());
        UNIT_ASSERT_VALUES_EQUAL(GetQueueActorIds(awake.GroupQueues), queueActorIds);
    }

    Y_UNIT_TEST(ActiveRequestAndDeadlineDelayDormancy) {
        TTestBasicRuntime runtime(1, false);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        SetupRuntime(runtime);

        TControlWrapper dormantTimeoutMinutes(1, 0, 1'000'000);
        TDSProxyEnv env;
        env.Configure(runtime, TBlobStorageGroupType(TBlobStorageGroupType::ErasureMirror3dc), 0, 0,
            TBlobStorageGroupInfo::EEM_ENC_V1, dormantTimeoutMinutes, true);
        DisableQueueSchedules(runtime, env.GroupQueues);

        SendGetBlock(runtime, env, runtime.GetCurrentTime() + TDuration::Minutes(2));
        AdvancePastOneMinute(runtime);
        UNIT_ASSERT(!QueryProxyState(runtime, env).IsDormant);

        AdvanceSeconds(runtime, TDuration::Minutes(1).Seconds() - 1);
        TAutoPtr<IEventHandle> handle;
        auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetBlockResult>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::DEADLINE);
        UNIT_ASSERT(!QueryProxyState(runtime, env).IsDormant);

        AdvancePastOneMinute(runtime);
        UNIT_ASSERT(QueryProxyState(runtime, env).IsDormant);
    }

    Y_UNIT_TEST(DisablingDormancyDoesNotWakeDormantProxy) {
        TTestBasicRuntime runtime(1, false);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        SetupRuntime(runtime);

        TControlWrapper dormantTimeoutMinutes(DefaultDormantTimeout.Minutes(), 0, 1'000'000);
        TDSProxyEnv env;
        env.Configure(runtime, TBlobStorageGroupType(TBlobStorageGroupType::ErasureMirror3dc), 0, 0,
            TBlobStorageGroupInfo::EEM_ENC_V1, dormantTimeoutMinutes, true);
        DisableQueueSchedules(runtime, env.GroupQueues);

        AdvanceSeconds(runtime, 2);
        UNIT_ASSERT(!QueryProxyState(runtime, env).IsDormant);

        dormantTimeoutMinutes = 1;
        AdvancePastOneMinute(runtime);
        UNIT_ASSERT(QueryProxyState(runtime, env).IsDormant);

        dormantTimeoutMinutes = 0;
        AdvanceTime(runtime, TDuration::Minutes(2));
        UNIT_ASSERT(QueryProxyState(runtime, env).IsDormant);

        SendInvalidPut(runtime, env);
        UNIT_ASSERT(!QueryProxyState(runtime, env).IsDormant);

        AdvancePastOneMinute(runtime);
        UNIT_ASSERT(!QueryProxyState(runtime, env).IsDormant);
    }

    Y_UNIT_TEST(SuspendsAndResumesPeriodicProcesses) {
        TTestBasicRuntime runtime(1, false);
        runtime.SetDispatchTimeout(TDuration::Seconds(1));
        SetupRuntime(runtime);

        TControlWrapper dormantTimeoutMinutes(100, 0, 1'000'000);
        TDSProxyEnv env;
        env.Configure(runtime, TBlobStorageGroupType(TBlobStorageGroupType::ErasureMirror3dc), 0, 0,
            TBlobStorageGroupInfo::EEM_ENC_V1, dormantTimeoutMinutes, true);
        DisableQueueSchedules(runtime, env.GroupQueues);
        RegisterGroupStatSinks(runtime, env);

        ui64 groupStatReports = 0;
        ui64 whiteboardUpdates = 0;
        auto groupStatObserver = runtime.AddObserver<TEvGroupStatReport>(
            [&](TEvGroupStatReport::TPtr&) {
                ++groupStatReports;
            });
        auto whiteboardObserver = runtime.AddObserver<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate>(
            [&](NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate::TPtr&) {
                ++whiteboardUpdates;
            });

        AdvanceSeconds(runtime, 16);
        UNIT_ASSERT(groupStatReports > 0);
        UNIT_ASSERT(whiteboardUpdates > 0);

        dormantTimeoutMinutes = 1;
        AdvancePastOneMinute(runtime);
        UNIT_ASSERT(QueryProxyState(runtime, env).IsDormant);

        // Wake before updates from the old generation arrive. The invalid put
        // completes synchronously, so the proxy can become dormant again.
        dormantTimeoutMinutes = 100;
        const ui64 beforeWakeWhiteboardUpdates = whiteboardUpdates;
        SendInvalidPut(runtime, env);
        UNIT_ASSERT(!QueryProxyState(runtime, env).IsDormant);
        const ui64 beforeWakeGroupStatReports = groupStatReports;
        AdvanceSeconds(runtime, 16);
        UNIT_ASSERT(groupStatReports > beforeWakeGroupStatReports);
        // One immediate update and one after 15 seconds. Any update left from
        // the previous generation would make this larger.
        UNIT_ASSERT_VALUES_EQUAL(whiteboardUpdates - beforeWakeWhiteboardUpdates, 2);

        dormantTimeoutMinutes = 1;
        AdvancePastOneMinute(runtime);
        UNIT_ASSERT(QueryProxyState(runtime, env).IsDormant);

        // Let updates scheduled before the second transition arrive and be
        // rejected by their generation checks, then verify no chain remains.
        AdvanceSeconds(runtime, 20);
        const ui64 dormantGroupStatReports = groupStatReports;
        const ui64 dormantWhiteboardUpdates = whiteboardUpdates;
        AdvanceSeconds(runtime, 20);
        UNIT_ASSERT_VALUES_EQUAL(groupStatReports, dormantGroupStatReports);
        UNIT_ASSERT_VALUES_EQUAL(whiteboardUpdates, dormantWhiteboardUpdates);
    }
}

} // anonymous namespace
} // namespace NKikimr
