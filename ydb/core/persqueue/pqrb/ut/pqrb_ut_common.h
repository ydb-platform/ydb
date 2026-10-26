#pragma once

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace NKikimr::NPQ {

inline void DispatchFor(TTestContext& tc, TDuration timeout = TDuration::MilliSeconds(50)) {
    tc.Runtime->DispatchEvents({}, timeout);
}

inline THolder<TEvPersQueue::TEvGetPartitionsLocationResponse> SendLocationRequest(
    TTestContext& tc,
    TEvPersQueue::TEvGetPartitionsLocation* request,
    TDuration timeout = TDuration::Seconds(10),
    ui64 cookie = 0,
    ui64* responseCookie = nullptr
) {
    if (responseCookie) {
        *responseCookie = Max<ui64>();
        tc.Runtime->SetObserverFunc([responseCookie, edge = tc.Edge](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPersQueue::TEvGetPartitionsLocationResponse::EventType &&
                ev->Recipient == edge)
            {
                *responseCookie = ev->Cookie;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
    }
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        request,
        0,
        GetPipeConfigWithRetries(),
        TActorId(),
        cookie);
    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(timeout);
    if (responseCookie) {
        tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }
    return response;
}

inline void WaitBalancerReady(TTestContext& tc, ui32 retries = 20) {
    for (ui32 i = 0; i < retries; ++i) {
        auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
        UNIT_ASSERT(response);
        if (response->Record.GetStatus()) {
            return;
        }
        tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(100));
        DispatchFor(tc);
    }
    UNIT_ASSERT_C(false, "Could not get positive response from balancer");
}

struct TBalancerUpdate {
    TString Topic = "topic";
    TVector<std::pair<ui32, std::pair<ui64, ui32>>> Partitions;
    TVector<ui64> ExtraTablets;
    ui64 SsId = 1;
    ui32 Version = 0;
    ui64 TxId = 12345;
    NKikimrPQ::TPQTabletConfig::TPartitionStrategyType Strategy =
        NKikimrPQ::TPQTabletConfig::DISABLED;
    ui32 MaxPartitionCount = 10;
    TVector<std::pair<TString, NKikimrPQ::TPQTabletConfig::EConsumerType>> Consumers;
    absl::flat_hash_map<ui32, TVector<ui32>> ParentPartitionIds;
    absl::flat_hash_map<ui32, TVector<ui32>> ChildPartitionIds;
    ui32 NextPartitionId = 0;
    ui32 ReceiveAttemptIdPeriodMs = 0;
};

inline ui32 NextBalancerVersion() {
    static ui32 version = 1000;
    return ++version;
}

inline void SendBalancerUpdate(TTestContext& tc, TBalancerUpdate params) {
    if (params.Version == 0) {
        params.Version = NextBalancerVersion();
    }

    auto request = MakeHolder<TEvPersQueue::TEvUpdateBalancerConfig>();
    auto& record = request->Record;
    record.SetTxId(params.TxId);
    record.SetPathId(1);
    record.SetVersion(params.Version);
    record.SetTopicName(params.Topic);
    record.SetPath("/Root/" + params.Topic);
    record.SetSchemeShardId(params.SsId);
    if (params.NextPartitionId) {
        record.SetNextPartitionId(params.NextPartitionId);
    }

    absl::flat_hash_set<ui64> tabletIds;
    for (const auto& p : params.Partitions) {
        auto* part = record.AddPartitions();
        part->SetPartition(p.first);
        part->SetGroup(p.second.second);
        part->SetTabletId(p.second.first);
        part->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Active);
        if (auto it = params.ParentPartitionIds.find(p.first); it != params.ParentPartitionIds.end()) {
            for (auto parent : it->second) {
                part->AddParentPartitionIds(parent);
            }
        }
        if (auto it = params.ChildPartitionIds.find(p.first); it != params.ChildPartitionIds.end()) {
            for (auto child : it->second) {
                part->AddChildPartitionIds(child);
            }
        }

        tabletIds.insert(p.second.first);
        auto* pp = record.MutableTabletConfig()->AddPartitions();
        pp->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Active);
    }
    for (auto tabletId : params.ExtraTablets) {
        tabletIds.insert(tabletId);
    }
    for (auto tabletId : tabletIds) {
        auto* tablet = record.AddTablets();
        tablet->SetTabletId(tabletId);
        tablet->SetOwner(1);
        tablet->SetIdx(tabletId);
    }

    if (params.Strategy != NKikimrPQ::TPQTabletConfig::DISABLED) {
        auto* strategy = record.MutableTabletConfig()->MutablePartitionStrategy();
        strategy->SetPartitionStrategyType(params.Strategy);
        strategy->SetMinPartitionCount(1);
        strategy->SetMaxPartitionCount(params.MaxPartitionCount);
    }

    if (params.Consumers.empty()) {
        record.MutableTabletConfig()->AddConsumers()->SetName("client");
    } else {
        for (const auto& [name, type] : params.Consumers) {
            auto* consumer = record.MutableTabletConfig()->AddConsumers();
            consumer->SetName(name);
            consumer->SetType(type);
            if (params.ReceiveAttemptIdPeriodMs) {
                consumer->SetReadRequestAttemptIdPeriodMs(params.ReceiveAttemptIdPeriodMs);
            }
        }
    }

    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
    auto result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvUpdateConfigResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(result);
    UNIT_ASSERT(result->Record.HasStatus() && result->Record.GetStatus() == NKikimrPQ::OK);
}

inline void NotifyDatabasePath(TTestContext& tc, const TString& path = "/Root") {
    NSchemeCache::TDescribeResult::TPtr result = new NSchemeCache::TDescribeResult{};
    result->SetPath(path);
    NSchemeCache::TDescribeResult::TCPtr cres = result;
    auto event = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(0, path, TPathId{}, cres);
    ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, event.Release());
    DispatchFor(tc, TDuration::MilliSeconds(200));
}

struct TScaleEnv {
    TTestContext tc;
    absl::flat_hash_map<TString, TActorId> Pipes;
    absl::flat_hash_map<ui32, TString> LockedBy;
    absl::flat_hash_map<ui32, TVector<ui32>> ParentPartitionIds;
    absl::flat_hash_map<ui32, TVector<ui32>> ChildPartitionIds;
    ui32 NextPartitionId = 0;
    NKikimrPQ::TPQTabletConfig::TPartitionStrategyType PartitionStrategy =
        NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE;

    explicit TScaleEnv(NKikimrPQ::TPQTabletConfig::TPartitionStrategyType strategy =
        NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE)
        : PartitionStrategy(strategy)
    {
        tc.Prepare();
        tc.Runtime->SetScheduledLimit(10000);
        PQTabletPrepare({}, {}, tc);
    }

    void Publish() {
        TBalancerUpdate update;
        update.Strategy = PartitionStrategy;
        update.NextPartitionId = NextPartitionId;
        update.ParentPartitionIds = ParentPartitionIds;
        update.ChildPartitionIds = ChildPartitionIds;
        for (ui32 i = 0; i < NextPartitionId; ++i) {
            update.Partitions.push_back({i, {tc.TabletId, i + 1}});
        }
        SendBalancerUpdate(tc, update);
        Pump();
    }

    void CreateParents(ui32 count = 2) {
        NextPartitionId = count;
        Publish();
    }

    ui32 Merge(ui32 left, ui32 right) {
        const ui32 child = NextPartitionId++;
        ParentPartitionIds[child] = {left, right};
        ChildPartitionIds[left].push_back(child);
        ChildPartitionIds[right].push_back(child);
        Publish();
        return child;
    }

    std::pair<ui32, ui32> Split(ui32 parent) {
        const ui32 left = NextPartitionId++;
        const ui32 right = NextPartitionId++;
        ParentPartitionIds[left] = {parent};
        ParentPartitionIds[right] = {parent};
        ChildPartitionIds[parent].push_back(left);
        ChildPartitionIds[parent].push_back(right);
        Publish();
        return {left, right};
    }

    TActorId RegisterSession(const TString& name, const TVector<ui32>& groups = {}, bool pump = true) {
        auto pipe = RegisterReadSession(name, tc, groups);
        Pipes[name] = pipe;
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
        return pipe;
    }

    void DisconnectSession(const TString& name) {
        auto it = Pipes.find(name);
        UNIT_ASSERT_C(it != Pipes.end(), name);
        tc.Runtime->ClosePipe(it->second, tc.Edge, 0);
        Pipes.erase(it);
        std::vector<ui32> drop;
        for (const auto& [partition, session] : LockedBy) {
            if (session == name) {
                drop.push_back(partition);
            }
        }
        for (auto partition : drop) {
            LockedBy.erase(partition);
        }
        Pump();
    }

    void Started(const TString& session, ui32 partition, bool pump = true) {
        auto it = Pipes.find(session);
        UNIT_ASSERT_C(it != Pipes.end(), session);
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvReadingPartitionStartedRequest(it->second, "user", partition),
            0,
            GetPipeConfigWithRetries(),
            it->second
        );
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
    }

    void Finish(const TString& session, ui32 partition, bool scaleAware = true, bool fromEnd = true, bool pump = true) {
        auto it = Pipes.find(session);
        UNIT_ASSERT_C(it != Pipes.end(), session);
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvReadingPartitionFinishedRequest(it->second, "user", partition, scaleAware, fromEnd),
            0,
            GetPipeConfigWithRetries(),
            it->second
        );
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
    }

    void Commit(ui32 partition, ui32 generation = 1, ui64 cookie = 1, bool pump = true) {
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPQ::TEvReadingPartitionStatusRequest("user", partition, generation, cookie),
            0,
            GetPipeConfigWithRetries()
        );
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
    }

    void AckRelease(const TActorId& pipe, ui32 partition, const TString& session) {
        auto released = MakeHolder<TEvPersQueue::TEvPartitionReleased>();
        released->Record.SetSession(session);
        released->Record.SetPartition(partition);
        released->Record.SetTopic("topic");
        released->Record.SetClientId("user");
        ActorIdToProto(pipe, released->Record.MutablePipeClient());
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            released.Release(),
            0,
            GetPipeConfigWithRetries(),
            pipe
        );
    }

    void Pump(TDuration wait = TDuration::MilliSeconds(50)) {
        DispatchFor(tc, wait);
        for (;;) {
            auto release = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReleasePartition>(TDuration::MilliSeconds(5));
            if (release) {
                const ui32 partition = release->Record.GetGroup() - 1;
                const TString session = release->Record.GetSession();
                auto pipeIt = Pipes.find(session);
                if (pipeIt == Pipes.end()) {
                    LockedBy.erase(partition);
                    continue;
                }
                AckRelease(pipeIt->second, partition, session);
                LockedBy.erase(partition);
                DispatchFor(tc, TDuration::MilliSeconds(10));
                continue;
            }
            auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::MilliSeconds(5));
            if (lock) {
                const TString session = lock->Record.GetSession();
                const ui32 partition = lock->Record.GetPartition();
                if (Pipes.contains(session)) {
                    LockedBy[partition] = session;
                } else {
                    LockedBy.erase(partition);
                }
                continue;
            }
            break;
        }
        DispatchFor(tc, wait);
    }

    THolder<TEvPersQueue::TEvReadSessionsInfoResponse> SessionsInfo() {
        auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
        sessions->Record.SetClientId("user");
        tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
        auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
        UNIT_ASSERT(info);
        return info;
    }

    TString SessionOf(ui32 partition) {
        Pump(TDuration::MilliSeconds(10));
        auto info = SessionsInfo();
        for (const auto& pi : info->Record.GetPartitionInfo()) {
            if (pi.GetPartition() == partition) {
                return pi.GetSession();
            }
        }
        return {};
    }

    void AssertNotLocked(ui32 partition) {
        Pump();
        if (auto it = LockedBy.find(partition); it != LockedBy.end()) {
            UNIT_ASSERT_C(false, "partition " << partition << " must not be locked, session=" << it->second);
        }
        const TString session = SessionOf(partition);
        UNIT_ASSERT_C(session.empty(),
            "partition " << partition << " must not be assigned, session=" << session);
    }

    void AssertLocked(ui32 partition, const TString& expectedSession = {}) {
        Pump();
        TString session;
        if (auto it = LockedBy.find(partition); it != LockedBy.end()) {
            session = it->second;
        }
        if (session.empty()) {
            session = SessionOf(partition);
        }
        UNIT_ASSERT_C(!session.empty(), "partition " << partition << " must be locked");
        if (!expectedSession.empty()) {
            UNIT_ASSERT_VALUES_EQUAL_C(expectedSession, session, "partition " << partition);
        }
    }

    void AssertSameSession(const std::vector<ui32>& partitions) {
        UNIT_ASSERT(!partitions.empty());
        const TString session = SessionOf(partitions.front());
        UNIT_ASSERT_C(!session.empty(), "partition " << partitions.front() << " must be locked");
        for (auto partition : partitions) {
            UNIT_ASSERT_VALUES_EQUAL_C(session, SessionOf(partition), "family must stay on one session");
        }
    }

    void AssertEvenDistribution(ui32 partitionCount, ui32 sessionCount) {
        Pump();
        auto info = SessionsInfo();
        absl::flat_hash_map<TString, ui32> counts;
        absl::flat_hash_set<ui32> seen;
        ui32 assigned = 0;
        for (const auto& pi : info->Record.GetPartitionInfo()) {
            if (pi.GetSession().empty()) {
                continue;
            }
            UNIT_ASSERT_C(seen.insert(pi.GetPartition()).second,
                "partition " << pi.GetPartition() << " listed twice in sessions info");
            counts[pi.GetSession()]++;
            ++assigned;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(assigned, partitionCount, "every readable partition must be assigned");
        UNIT_ASSERT_VALUES_EQUAL_C(counts.size(), sessionCount, "every session must get a share");
        ui32 minCount = partitionCount;
        ui32 maxCount = 0;
        for (const auto& [session, count] : counts) {
            Y_UNUSED(session);
            minCount = Min(minCount, count);
            maxCount = Max(maxCount, count);
        }
        UNIT_ASSERT_LE_C(maxCount - minCount, 1u,
            "families must go to the least loaded sessions, counts=" << counts.size());
    }
};

struct TProposeCapture {
    std::vector<NKikimrTxUserProxy::TEvProposeTransaction> Records;
};

class TCapturingTxProxy : public NActors::TActor<TCapturingTxProxy> {
public:
    explicit TCapturingTxProxy(TProposeCapture* cap)
        : TActor(&TThis::State)
        , Cap(cap)
    {}

    STRICT_STFUNC(State,
        hFunc(TEvTxUserProxy::TEvProposeTransaction, Handle);
    )

private:
    void Handle(TEvTxUserProxy::TEvProposeTransaction::TPtr& ev) {
        Cap->Records.push_back(ev->Get()->Record);
    }

    TProposeCapture* Cap;
};

inline void InstallProposeCapture(TTestContext& tc, TProposeCapture& cap) {
    auto id = tc.Runtime->Register(new TCapturingTxProxy(&cap));
    tc.Runtime->RegisterService(MakeTxProxyID(), id);
}

inline void WaitProposes(TTestContext& tc, TProposeCapture& cap, size_t n = 1) {
    tc.Runtime->WaitFor("TEvProposeTransaction", [&] { return cap.Records.size() >= n; }, TDuration::Seconds(10));
}

} // namespace NKikimr::NPQ
