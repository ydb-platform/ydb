#pragma once

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

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

} // namespace NKikimr::NPQ
