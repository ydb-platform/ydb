#pragma once

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NSchemeChangeRecordTestHelpers {

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

inline TEvSchemeShard::TEvRegisterSubscriberResult* RegisterSubscriber(
    TTestActorRuntime& runtime, const TString& subscriberId,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvRegisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvRegisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

inline TEvSchemeShard::TEvFetchSchemeChangeRecordsResult* FetchSchemeChangeRecords(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 afterSeqId, ui32 maxCount,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecords>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetAfterSequenceId(afterSeqId);
    req->Record.SetMaxCount(maxCount);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFetchSchemeChangeRecordsResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

inline TEvSchemeShard::TEvAckSchemeChangeRecordsResult* AckSchemeChangeRecords(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 upToSeqId,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvAckSchemeChangeRecords>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetUpToSequenceId(upToSeqId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvAckSchemeChangeRecordsResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

inline TEvSchemeShard::TEvForceAdvanceSubscriberResult* ForceAdvanceSubscriber(
    TTestActorRuntime& runtime, const TString& subscriberId,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvForceAdvanceSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvForceAdvanceSubscriberResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

inline TEvSchemeShard::TEvUnregisterSubscriberResult* UnregisterSubscriber(
    TTestActorRuntime& runtime, const TString& subscriberId,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvUnregisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvUnregisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

inline TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult* FetchSchemeChangeRecordBodies(
    TTestActorRuntime& runtime, const TString& subscriberId, const TVector<ui64>& seqIds,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecordBodies>();
    req->Record.SetSubscriberId(subscriberId);
    for (ui64 seqId : seqIds) {
        req->Record.AddSequenceIds(seqId);
    }
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

struct TSchemeChangeRecordEntry {
    ui64 SequenceId = 0;
    ui64 TxId = 0;
    ui64 PlanStep = 0;
    ui32 OperationType = 0;
    ui64 PathOwnerId = 0;
    ui64 PathLocalId = 0;
    TString PathName;
    ui32 ObjectType = 0;
    ui32 Status = 0;
    TString UserSID;
    ui64 SchemaVersion = 0;
    ui64 CompletedAt = 0;
    NKikimrSchemeOp::TModifyScheme Body;
};

struct TSchemeChangeRecordsReadResult {
    TVector<TSchemeChangeRecordEntry> Entries;
    ui64 WatermarkPlanStep = 0;
};

inline TSchemeChangeRecordsReadResult ReadSchemeChangeRecordsFull(
    TTestActorRuntime& runtime)
{
    const TString tempSubId = "__internal_read_sub__";

    // Register temp subscriber
    TAutoPtr<IEventHandle> regHandle;
    RegisterSubscriber(runtime, tempSubId, regHandle);

    // Step 1: Fetch metadata only (body is no longer returned by Fetch).
    TAutoPtr<IEventHandle> fetchHandle;
    auto* fetch = FetchSchemeChangeRecords(runtime, tempSubId, 0, 1000, fetchHandle);

    TSchemeChangeRecordsReadResult result;
    result.WatermarkPlanStep = fetch->Record.GetWatermarkPlanStep();

    TVector<ui64> seqIdsWithBody;
    for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
        const auto& proto = fetch->Record.GetEntries(i);
        TSchemeChangeRecordEntry entry;
        entry.SequenceId = proto.GetSequenceId();
        entry.TxId = proto.GetTxId();
        entry.PlanStep = proto.GetPlanStep();
        entry.OperationType = proto.GetOperationType();
        entry.PathOwnerId = proto.GetPathId().GetOwnerId();
        entry.PathLocalId = proto.GetPathId().GetLocalId();
        entry.PathName = proto.GetPathName();
        entry.ObjectType = proto.GetObjectType();
        entry.Status = proto.GetStatus();
        entry.UserSID = proto.GetUserSID();
        entry.SchemaVersion = proto.GetSchemaVersion();
        entry.CompletedAt = proto.GetCompletedAt();
        if (proto.GetBodySize() > 0) {
            seqIdsWithBody.push_back(proto.GetSequenceId());
        }
        result.Entries.push_back(std::move(entry));
    }

    // Step 2: Fetch bodies for entries with non-zero BodySize; merge back.
    if (!seqIdsWithBody.empty()) {
        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, tempSubId, seqIdsWithBody, bodiesHandle);
        THashMap<ui64, TString> bodyBySeqId;
        for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
            const auto& b = bodies->Record.GetEntries(i);
            bodyBySeqId.emplace(b.GetSequenceId(), b.GetBody());
        }
        for (auto& entry : result.Entries) {
            auto it = bodyBySeqId.find(entry.SequenceId);
            if (it != bodyBySeqId.end() && !it->second.empty()) {
                Y_ABORT_UNLESS(entry.Body.ParseFromString(it->second));
            }
        }
    }

    // Unregister temp subscriber
    TAutoPtr<IEventHandle> unregHandle;
    UnregisterSubscriber(runtime, tempSubId, unregHandle);

    return result;
}

inline TVector<TSchemeChangeRecordEntry> ReadSchemeChangeRecords(
    TTestActorRuntime& runtime)
{
    return ReadSchemeChangeRecordsFull(runtime).Entries;
}

} // namespace NSchemeChangeRecordTestHelpers
