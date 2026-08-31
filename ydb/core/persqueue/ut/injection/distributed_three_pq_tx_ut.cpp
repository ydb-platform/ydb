#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <unordered_set>

namespace NKikimr::NPQ {

namespace {

// Edge waits under injection: keep short so a dead dispatch fails into retry quickly.
constexpr TDuration kDistThreePqEdgeTimeout = TDuration::Seconds(1);
constexpr ui32 kDistThreePqMsgCount = 2;

class TDistThreePqRetry : public yexception {
};

struct TThreeRealPqEnv {
    static constexpr ui32 TabletCount = 3;

    THolder<TTestBasicRuntime> Runtime;
    TActorId Edge;
    TVector<ui64> TabletIds;
    THashMap<ui64, TActorId> Pipes;
    THashMap<ui64, ui32> MsgSeqNo;

    TThreeRealPqEnv() {
        // Keep id=2 free (balancer id in TTestContext); use 1/3/4 for real PQ tablets.
        TabletIds = {
            MakeTabletID(false, 1),
            MakeTabletID(false, 3),
            MakeTabletID(false, 4),
        };
    }

    ~TThreeRealPqEnv() {
        ResetAllPipes();
        Runtime.Reset(nullptr);
    }

    void ResetAllPipes() {
        if (!Runtime) {
            Pipes.clear();
            return;
        }
        for (auto& [tabletId, pipe] : Pipes) {
            if (pipe) {
                Runtime->ClosePipe(pipe, Edge, 0);
            }
        }
        Pipes.clear();
    }

    void Prepare(
        const TString& /*dispatchName*/,
        std::function<void(TTestActorRuntime&)> setup,
        bool& activeZone)
    {
        activeZone = false;
        ResetAllPipes();
        MsgSeqNo.clear();
        Runtime.Reset(new TTestBasicRuntime());
        Runtime->SetScheduledLimit(5'000);
        TTestContext::SetupLogging(*Runtime, /*enableDetailedPQLog=*/false);
        SetupTabletServices(*Runtime);
        setup(*Runtime);

        FillPQConfig(Runtime->GetAppData(0).PQConfig, "/Root/PQ", /*isFirstClass=*/false);
        Runtime->GetAppData(0).PQConfig.SetEnabled(true);

        for (ui64 tabletId : TabletIds) {
            CreateTestBootstrapper(
                *Runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::PersQueue, TErasureType::ErasureNone),
                &CreatePersQueue);
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
            Runtime->DispatchEvents(options);
        }

        Edge = Runtime->AllocateEdgeActor();
        Runtime->SetScheduledEventFilter(&TTestContext::RequestTimeoutFilter);

        const TVector<TConsumerPreparationParameters> users{
            {.Name = "user", .Important = true},
        };
        for (ui64 tabletId : TabletIds) {
            PQTabletPrepare(
                {.partitions = 1},
                users,
                *Runtime,
                tabletId,
                Edge);
            MsgSeqNo[tabletId] = 0;
        }
    }

    TActorId& PipeFor(ui64 tabletId) {
        auto& pipe = Pipes[tabletId];
        if (!pipe) {
            pipe = Runtime->ConnectToPipe(tabletId, Edge, 0, GetPipeConfigWithRetries());
        }
        Y_ABORT_UNLESS(pipe);
        return pipe;
    }

    void SendToTablet(ui64 tabletId, IEventBase* event) {
        auto& pipe = PipeFor(tabletId);
        Runtime->SendToPipe(pipe, Edge, event, 0, 0);
    }

    void ResetPipe(ui64 tabletId) {
        auto it = Pipes.find(tabletId);
        if (it != Pipes.end() && it->second) {
            Runtime->ClosePipe(it->second, Edge, 0);
            it->second = {};
        }
    }

    // Drain leftover propose/plan replies from a previous interrupted attempt.
    void DrainEdgeProposeResults() {
        for (;;) {
            auto event = Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
                TDuration::MilliSeconds(1));
            if (!event) {
                break;
            }
        }
        for (ui32 i = 0; i < TabletCount * 4; ++i) {
            Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(TDuration::MilliSeconds(1));
            Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>(TDuration::MilliSeconds(1));
        }
    }

    // Shared Edge receives replies from all tablets; match TabletId before accepting.
    THolder<TEvPersQueue::TEvOffsetsResponse> GrabOffsetsResponseFor(
        ui64 tabletId, TDuration timeout)
    {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline) {
            const TDuration left = deadline - TInstant::Now();
            auto result = Runtime->GrabEdgeEvent<TEvPersQueue::TEvOffsetsResponse>(left);
            if (!result) {
                return {};
            }
            if (!result->Record.HasTabletId() || result->Record.GetTabletId() != tabletId) {
                continue;
            }
            return result;
        }
        return {};
    }

    // TEvResponse may lack TabletId; stamp PartitionRequest cookie with tabletId and filter on it.
    THolder<TEvPersQueue::TEvResponse> GrabResponseForCookie(ui64 cookie, TDuration timeout) {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline) {
            const TDuration left = deadline - TInstant::Now();
            auto result = Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(left);
            if (!result) {
                return {};
            }
            if (!result->Record.HasPartitionResponse() ||
                result->Record.GetPartitionResponse().GetCookie() != cookie)
            {
                continue;
            }
            return result;
        }
        return {};
    }

    bool IsTabletReady(ui64 tabletId) {
        try {
            Runtime->ResetScheduledCount();
            ResetPipe(tabletId);
            auto request = MakeHolder<TEvPersQueue::TEvOffsets>();
            SendToTablet(tabletId, request.Release());
            auto result = GrabOffsetsResponseFor(tabletId, TDuration::MilliSeconds(200));
            if (!result || result->Record.PartResultSize() == 0) {
                return false;
            }
            return result->Record.GetPartResult(0).GetErrorCode() != NPersQueue::NErrorCode::INITIALIZING;
        } catch (const NActors::TSchedulingLimitReachedException&) {
            return false;
        } catch (const NActors::TEmptyEventQueueException&) {
            return false;
        }
    }

    void WaitAllTabletsReady() {
        for (ui32 round = 0; round < 15; ++round) {
            bool allReady = true;
            for (ui64 tabletId : TabletIds) {
                if (!IsTabletReady(tabletId)) {
                    allReady = false;
                    break;
                }
            }
            if (allReady) {
                return;
            }
            try {
                Runtime->ResetScheduledCount();
                Runtime->SimulateSleep(TDuration::MilliSeconds(50));
            } catch (const NActors::TSchedulingLimitReachedException&) {
            } catch (const NActors::TEmptyEventQueueException&) {
            }
        }
        ythrow TDistThreePqRetry() << "tablets not ready after reboot";
    }

    ui64 GetEndOffset(ui64 tabletId) {
        for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
            try {
                Runtime->ResetScheduledCount();
                ResetPipe(tabletId);
                auto request = MakeHolder<TEvPersQueue::TEvOffsets>();
                SendToTablet(tabletId, request.Release());
                auto result = GrabOffsetsResponseFor(tabletId, kDistThreePqEdgeTimeout);
                if (!result || result->Record.PartResultSize() == 0) {
                    continue;
                }
                if (result->Record.GetPartResult(0).GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                    Runtime->DispatchEvents();
                    retriesLeft = 5;
                    continue;
                }
                return result->Record.GetPartResult(0).GetEndOffset();
            } catch (const NActors::TSchedulingLimitReachedException&) {
            } catch (const NActors::TEmptyEventQueueException&) {
            }
        }
        ythrow TDistThreePqRetry() << "GetEndOffset failed for tablet " << tabletId;
    }

    i64 GetClientOffset(ui64 tabletId, const TString& user) {
        for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
            try {
                Runtime->ResetScheduledCount();
                ResetPipe(tabletId);
                auto request = MakeHolder<TEvPersQueue::TEvRequest>();
                auto* req = request->Record.MutablePartitionRequest();
                req->SetPartition(0);
                // Cookie identifies the target tablet among shared-Edge replies.
                req->SetCookie(tabletId);
                req->MutableCmdGetClientOffset()->SetClientId(user);
                SendToTablet(tabletId, request.Release());
                auto result = GrabResponseForCookie(tabletId, kDistThreePqEdgeTimeout);
                if (!result) {
                    continue;
                }
                if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                    Runtime->DispatchEvents();
                    retriesLeft = 5;
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    (int)result->Record.GetErrorCode(), (int)NPersQueue::NErrorCode::OK);
                UNIT_ASSERT(result->Record.GetPartitionResponse().HasCmdGetClientOffsetResult());
                return result->Record.GetPartitionResponse().GetCmdGetClientOffsetResult().GetOffset();
            } catch (const NActors::TSchedulingLimitReachedException&) {
            } catch (const NActors::TEmptyEventQueueException&) {
            }
        }
        ythrow TDistThreePqRetry() << "GetClientOffset failed for tablet " << tabletId;
    }
};

void ProposeDistributedOffsetTx(TThreeRealPqEnv& env, ui64 txId, ui64 begin, ui64 end);

void RecordOrigin(THashSet<ui64>& seenOrigins, const NKikimrPQ::TEvProposeTransactionResult& record) {
    UNIT_ASSERT_C(record.HasOrigin(), record.ShortDebugString());
    seenOrigins.insert(record.GetOrigin());
}

void WaitNProposeResults(
    TThreeRealPqEnv& env,
    ui64 txId,
    NKikimrPQ::TEvProposeTransactionResult::EStatus status,
    ui32 expectedCount,
    ui64 begin,
    ui64 end)
{
    THashSet<ui64> seenOrigins;
    for (ui32 round = 0; round < 20 && seenOrigins.size() < expectedCount; ++round) {
        try {
            env.Runtime->ResetScheduledCount();
            auto event = env.Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
                kDistThreePqEdgeTimeout);
            if (!event) {
                // Reboot mid-propose: restore pipes and re-send propose to missing tablets.
                env.ResetAllPipes();
                env.WaitAllTabletsReady();
                if (status == NKikimrPQ::TEvProposeTransactionResult::PREPARED) {
                    ProposeDistributedOffsetTx(env, txId, begin, end);
                }
                continue;
            }
            if (event->Record.GetTxId() != txId) {
                continue;
            }
            const auto gotStatus = event->Record.GetStatus();
            // COMPLETE implies the tablet already passed PREPARED for this tx.
            if (status == NKikimrPQ::TEvProposeTransactionResult::PREPARED &&
                (gotStatus == NKikimrPQ::TEvProposeTransactionResult::PREPARED ||
                 gotStatus == NKikimrPQ::TEvProposeTransactionResult::COMPLETE))
            {
                RecordOrigin(seenOrigins, event->Record);
                continue;
            }
            if (gotStatus != status) {
                ythrow TDistThreePqRetry()
                    << "unexpected ProposeResult status="
                    << static_cast<int>(gotStatus)
                    << " expected=" << static_cast<int>(status);
            }
            RecordOrigin(seenOrigins, event->Record);
        } catch (const NActors::TSchedulingLimitReachedException&) {
            env.Runtime->ResetScheduledCount();
        } catch (const NActors::TEmptyEventQueueException&) {
            env.Runtime->ResetScheduledCount();
        }
    }
    if (seenOrigins.size() < expectedCount) {
        ythrow TDistThreePqRetry()
            << "timeout waiting ProposeResult status=" << static_cast<int>(status)
            << " got=" << seenOrigins.size() << "/" << expectedCount;
    }
}

void ProposeDistributedOffsetTx(TThreeRealPqEnv& env, ui64 txId, ui64 begin, ui64 end) {
    for (ui64 tabletId : env.TabletIds) {
        env.ResetPipe(tabletId);
        auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();
        ActorIdToProto(env.Edge, event->Record.MutableSourceActor());
        event->Record.SetTxId(txId);
        auto* body = event->Record.MutableData();
        auto* operation = body->MutableOperations()->Add();
        operation->SetPartitionId(0);
        operation->SetCommitOffsetsBegin(begin);
        operation->SetCommitOffsetsEnd(end);
        operation->SetConsumer("user");
        operation->SetPath("/topic");
        // Peer shards only — self is the local participant; RS mesh is among the three tablets.
        for (ui64 shard : env.TabletIds) {
            if (shard == tabletId) {
                continue;
            }
            body->AddSendingShards(shard);
            body->AddReceivingShards(shard);
        }
        body->SetImmediate(false);
        env.SendToTablet(tabletId, event.Release());
    }
}

void PlanDistributedTx(TThreeRealPqEnv& env, ui64 txId, ui64 step) {
    for (ui64 tabletId : env.TabletIds) {
        env.ResetPipe(tabletId);
        auto event = MakeHolder<TEvTxProcessing::TEvPlanStep>();
        event->Record.SetStep(step);
        auto* tx = event->Record.AddTransactions();
        tx->SetTxId(txId);
        ActorIdToProto(env.Edge, tx->MutableAckTo());
        env.SendToTablet(tabletId, event.Release());
    }
}

void DrainPlanStepSideEffects(TThreeRealPqEnv& env) {
    // Best-effort: each tablet may emit Ack/Accepted; ignore under injection.
    for (ui32 i = 0; i < TThreeRealPqEnv::TabletCount * 2; ++i) {
        env.Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(TDuration::MilliSeconds(1));
        env.Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>(TDuration::MilliSeconds(1));
    }
}

// After PlanStep, a mid-flight reboot must not start a new TxId: peers stay in WAIT_RS for
// the original tx, and the restarted tablet restores it from KV and continues the RS mesh.
void WaitAllCompleteAfterPlan(TThreeRealPqEnv& env, ui64 txId, ui64 step) {
    THashSet<ui64> completedOrigins;
    for (ui32 round = 0; round < 25 && completedOrigins.size() < TThreeRealPqEnv::TabletCount; ++round) {
        try {
            env.Runtime->ResetScheduledCount();
            auto event = env.Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
                kDistThreePqEdgeTimeout);
            if (!event) {
                // Reboot may drop in-flight replies; restore and re-deliver PlanStep.
                env.ResetAllPipes();
                env.WaitAllTabletsReady();
                PlanDistributedTx(env, txId, step);
                continue;
            }
            if (event->Record.GetTxId() != txId) {
                continue;
            }
            if (event->Record.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::COMPLETE) {
                RecordOrigin(completedOrigins, event->Record);
                continue;
            }
            if (event->Record.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::PREPARED) {
                // Late PREPARED after we already planned — ignore.
                continue;
            }
            ythrow TDistThreePqRetry()
                << "unexpected status after PlanStep="
                << static_cast<int>(event->Record.GetStatus());
        } catch (const NActors::TSchedulingLimitReachedException&) {
            env.Runtime->ResetScheduledCount();
        } catch (const NActors::TEmptyEventQueueException&) {
            env.Runtime->ResetScheduledCount();
        }
    }
    if (completedOrigins.size() < TThreeRealPqEnv::TabletCount) {
        ythrow TDistThreePqRetry()
            << "timeout waiting COMPLETE after PlanStep got=" << completedOrigins.size()
            << "/" << TThreeRealPqEnv::TabletCount;
    }
}

bool AllConsumerOffsetsAt(TThreeRealPqEnv& env, i64 expected) {
    for (ui64 tabletId : env.TabletIds) {
        if (env.GetClientOffset(tabletId, "user") != expected) {
            return false;
        }
    }
    return true;
}

void AssertAllConsumerOffsetsAt(TThreeRealPqEnv& env, i64 expected) {
    for (ui64 tabletId : env.TabletIds) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            env.GetClientOffset(tabletId, "user"), expected,
            "tabletId=" << tabletId);
    }
}

// Seed each tablet partition so the distributed offset-commit has a real [0, msgCount) range.
void EnsureMessagesWritten(TThreeRealPqEnv& env, ui32 attempt) {
    for (ui64 tabletId : env.TabletIds) {
        const ui64 endOffset = env.GetEndOffset(tabletId);
        if (endOffset >= kDistThreePqMsgCount) {
            continue;
        }
        TVector<std::pair<ui64, TString>> data;
        data.reserve(kDistThreePqMsgCount - endOffset);
        for (ui64 i = endOffset; i < kDistThreePqMsgCount; ++i) {
            data.emplace_back(
                i + 1,
                TStringBuilder() << "d3-" << tabletId << "-" << attempt << "-" << i);
        }
        CmdWrite(
            env.Runtime.Get(),
            tabletId,
            env.Edge,
            /*partition=*/0,
            TStringBuilder() << "src-" << tabletId,
            env.MsgSeqNo[tabletId],
            data,
            /*error=*/false,
            /*alreadyWrittenSeqNo=*/{},
            /*isFirst=*/endOffset == 0,
            /*ownerCookie=*/"",
            /*msn=*/-1,
            /*offset=*/-1,
            /*treatWrongCookieAsError=*/false,
            /*treatBadOffsetAsError=*/true,
            /*disableDeduplication=*/true);
    }
}

void DistributedThreePqTxScenario(TThreeRealPqEnv& env, bool& activeZone) {
    for (ui32 attempt = 0; attempt < 15; ++attempt) {
        try {
            env.Runtime->ResetScheduledCount();
            env.ResetAllPipes();
            activeZone = false;
            env.DrainEdgeProposeResults();
            env.WaitAllTabletsReady();

            // Previous attempt may have committed while a later GrabEdgeEvent failed.
            if (AllConsumerOffsetsAt(env, kDistThreePqMsgCount)) {
                return;
            }

            EnsureMessagesWritten(env, attempt);

            const ui64 txId = 9400 + attempt;
            const ui64 step = 100 + txId;
            constexpr ui64 begin = 0;
            constexpr ui64 end = kDistThreePqMsgCount;

            // Propose outside the injection zone so the reboot/pipe matrix stays small.
            ProposeDistributedOffsetTx(env, txId, begin, end);
            WaitNProposeResults(
                env, txId, NKikimrPQ::TEvProposeTransactionResult::PREPARED,
                TThreeRealPqEnv::TabletCount, begin, end);

            // Inject through PlanStep delivery and the following RS mesh / COMPLETE wait.
            // PlanDistributedTx only queues pipe sends; tablet events run while we wait.
            activeZone = true;
            PlanDistributedTx(env, txId, step);
            // Stay on the same TxId through reboot recovery (do not propose a new one).
            WaitAllCompleteAfterPlan(env, txId, step);
            activeZone = false;

            DrainPlanStepSideEffects(env);
            AssertAllConsumerOffsetsAt(env, kDistThreePqMsgCount);
            return;
        } catch (const TDistThreePqRetry&) {
            activeZone = false;
            try {
                env.Runtime->ResetScheduledCount();
                env.Runtime->SimulateSleep(TDuration::MilliSeconds(50));
            } catch (...) {
            }
        } catch (const NActors::TSchedulingLimitReachedException&) {
            activeZone = false;
        } catch (const NActors::TEmptyEventQueueException&) {
            activeZone = false;
        }
    }
    UNIT_FAIL("DistributedThreePqTxScenario: retries exhausted");
}

void RunDistributedThreePqInjectionTest(
    std::function<void(
        const TVector<ui64>&,
        std::function<TTestActorRuntime::TEventFilter()>,
        std::function<void(const TString&, std::function<void(TTestActorRuntime&)>, bool&)>)> runner,
    const std::unordered_set<TString>& extraSkipEventTypes = {})
{
    TInitialEventsFilter filter;
    const TVector<ui64> rebootTablets{
        MakeTabletID(false, 1),
        MakeTabletID(false, 3),
        MakeTabletID(false, 4),
    };
    runner(
        rebootTablets,
        [&]() {
            return filter.Prepare(
                {TabletPipe, NPDisk, KeyValue, PQ},
                extraSkipEventTypes);
        },
        [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
            TThreeRealPqEnv env;
            activeZone = false;
            env.Prepare(dispatchName, setup, activeZone);
            DistributedThreePqTxScenario(env, activeZone);
        });
}

} // namespace

Y_UNIT_TEST_SUITE(TDistributedThreePqTxInjectionTests) {

// Three real PQ tablets run a distributed offset-commit tx (ReadSet mesh) under tablet reboots.
Y_UNIT_TEST(DistributedThreePqTxWithTabletReboots) {
    RunDistributedThreePqInjectionTest([](const auto& tabletIds, auto filterFactory, auto testFunc) {
        RunTestWithReboots(tabletIds, filterFactory, testFunc);
    });
}

// Same distributed 3-tablet tx under pipe client resets.
// Skip inter-tablet ReadSet pipes: reconnect goes through Hive, which handmade PQ tablets lack.
Y_UNIT_TEST(DistributedThreePqTxWithPipeResets) {
    const std::unordered_set<TString> skipInterTabletRs{
        "NKikimr::TEvTxProcessing::TEvReadSet",
        "NKikimr::TEvTxProcessing::TEvReadSetAck",
    };
    RunDistributedThreePqInjectionTest(
        [](const auto& tabletIds, auto filterFactory, auto testFunc) {
            RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
        },
        skipInterTabletRs);
}

} // Y_UNIT_TEST_SUITE(TDistributedThreePqTxInjectionTests)

} // namespace NKikimr::NPQ
