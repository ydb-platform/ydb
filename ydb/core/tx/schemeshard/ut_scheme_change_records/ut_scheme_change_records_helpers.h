#pragma once

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/cms/console/console.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NSchemeChangeRecordTestHelpers {

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// Register with the default start position: the tail. A new subscriber sees
// what happens next, never history.
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
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(),
        (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS,
        "RegisterSubscriber failed: " << result->Record.GetReason());
    return result;
}

// Register with the default start position, asserting a specific status.
inline TEvSchemeShard::TEvRegisterSubscriberResult* RegisterSubscriberExpect(
    TTestActorRuntime& runtime, const TString& subscriberId,
    NKikimrSchemeShard::TSchemeChangeRecordsStatus::EStatus expected,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvRegisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvRegisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(), (ui32)expected,
        "RegisterSubscriber status mismatch: " << result->Record.GetReason());
    return result;
}

// Register at an explicit start position. `startOrder` is an exclusive cursor
// (0 means "everything retained"); values below the retention floor are
// clamped up and reported as STATE_LOST.
inline TEvSchemeShard::TEvRegisterSubscriberResult* RegisterSubscriberAtExpect(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 startOrder,
    NKikimrSchemeShard::TSchemeChangeRecordsStatus::EStatus expected,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvRegisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetStartOrder(startOrder);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvRegisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(), (ui32)expected,
        "RegisterSubscriberAt status mismatch: " << result->Record.GetReason());
    return result;
}

inline TEvSchemeShard::TEvRegisterSubscriberResult* RegisterSubscriberAt(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 startOrder,
    TAutoPtr<IEventHandle>& handle)
{
    return RegisterSubscriberAtExpect(runtime, subscriberId, startOrder,
        NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, handle);
}

inline TEvSchemeShard::TEvFetchSchemeChangeRecordsResult* FetchSchemeChangeRecordsExpect(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 afterOrder, ui32 maxCount,
    NKikimrSchemeShard::TSchemeChangeRecordsStatus::EStatus expected,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecords>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetAfterOrder(afterOrder);
    req->Record.SetMaxCount(maxCount);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFetchSchemeChangeRecordsResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(), (ui32)expected,
        "FetchSchemeChangeRecords status mismatch: " << result->Record.GetReason());
    return result;
}

// Asserts STATUS_SUCCESS, so a failed fetch cannot pass an emptiness check
// meant to verify a sweep happened.
inline TEvSchemeShard::TEvFetchSchemeChangeRecordsResult* FetchSchemeChangeRecords(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 afterOrder, ui32 maxCount,
    TAutoPtr<IEventHandle>& handle)
{
    return FetchSchemeChangeRecordsExpect(runtime, subscriberId, afterOrder, maxCount,
        NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, handle);
}

inline TEvSchemeShard::TEvAckSchemeChangeRecordsResult* AckSchemeChangeRecords(
    TTestActorRuntime& runtime, const TString& subscriberId, ui64 upToOrder,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvAckSchemeChangeRecords>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetUpToOrder(upToOrder);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvAckSchemeChangeRecordsResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(),
        (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS,
        "AckSchemeChangeRecords failed: " << result->Record.GetReason());
    return result;
}

inline TEvSchemeShard::TEvForceAdvanceSubscriberResult* ForceAdvanceSubscriberExpect(
    TTestActorRuntime& runtime, const TString& subscriberId,
    NKikimrSchemeShard::TSchemeChangeRecordsStatus::EStatus expected,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvForceAdvanceSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvForceAdvanceSubscriberResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(), (ui32)expected,
        "ForceAdvanceSubscriber status mismatch: " << result->Record.GetReason());
    return result;
}

inline TEvSchemeShard::TEvForceAdvanceSubscriberResult* ForceAdvanceSubscriber(
    TTestActorRuntime& runtime, const TString& subscriberId,
    TAutoPtr<IEventHandle>& handle)
{
    return ForceAdvanceSubscriberExpect(runtime, subscriberId,
        NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, handle);
}

inline TEvSchemeShard::TEvUnregisterSubscriberResult* UnregisterSubscriberExpect(
    TTestActorRuntime& runtime, const TString& subscriberId,
    NKikimrSchemeShard::TSchemeChangeRecordsStatus::EStatus expected,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvUnregisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvUnregisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(), (ui32)expected,
        "UnregisterSubscriber status mismatch: " << result->Record.GetReason());
    return result;
}

inline TEvSchemeShard::TEvUnregisterSubscriberResult* UnregisterSubscriber(
    TTestActorRuntime& runtime, const TString& subscriberId,
    TAutoPtr<IEventHandle>& handle)
{
    return UnregisterSubscriberExpect(runtime, subscriberId,
        NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, handle);
}

inline TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult* FetchSchemeChangeRecordBodiesExpect(
    TTestActorRuntime& runtime, const TString& subscriberId, const TVector<ui64>& orders,
    NKikimrSchemeShard::TSchemeChangeRecordsStatus::EStatus expected,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecordBodies>();
    req->Record.SetSubscriberId(subscriberId);
    for (ui64 order : orders) {
        req->Record.AddOrders(order);
    }
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetStatus(), (ui32)expected,
        "FetchSchemeChangeRecordBodies status mismatch: " << result->Record.GetReason());
    return result;
}

inline TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult* FetchSchemeChangeRecordBodies(
    TTestActorRuntime& runtime, const TString& subscriberId, const TVector<ui64>& orders,
    TAutoPtr<IEventHandle>& handle)
{
    return FetchSchemeChangeRecordBodiesExpect(runtime, subscriberId, orders,
        NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, handle);
}


// Mirrors NKikimrSchemeShard::TSchemeChangeTarget: Path is the target's
// identity after the change, SourcePaths is empty for a plain create/alter/
// drop and non-empty for a move/rename or copy target.
struct TTestSchemeChangeTarget {
    TString Path;
    TVector<TString> SourcePaths;
};

struct TSchemeChangeRecordEntry {
    ui64 Order = 0;
    ui64 TxId = 0;
    ui64 PlanStep = 0;
    ui32 OperationType = 0;
    ui64 PathOwnerId = 0;
    ui64 PathLocalId = 0;
    TVector<TTestSchemeChangeTarget> Targets;
    ui32 ObjectType = 0;
    ui32 Status = 0;
    TString UserSID;
    ui64 SchemaVersion = 0;
    ui64 CompletedAtUs = 0;
    ui32 PositionKind = 0;
    NKikimrSchemeOp::TModifyScheme Body;
    // Resolved description captured when the record was written; empty if none.
    TString Description;
    // Field paths cleared by redaction; empty when nothing was cleared.
    TVector<TString> RedactedFields;
};

// True if any of the entry's N target paths contains the given substring.
// For single-target test fixtures, where exactly one target is expected.
inline bool AnyPathContains(const TSchemeChangeRecordEntry& entry, const TString& substr) {
    for (const auto& target : entry.Targets) {
        if (target.Path.Contains(substr)) {
            return true;
        }
    }
    return false;
}

// True if the entry has exactly one target whose Path equals the given value.
inline bool SinglePathEquals(const TSchemeChangeRecordEntry& entry, const TString& value) {
    return entry.Targets.size() == 1 && entry.Targets[0].Path == value;
}

// Comma-joined target paths, for diagnostic messages.
inline TString AllTargetPaths(const TSchemeChangeRecordEntry& entry) {
    TVector<TString> paths;
    for (const auto& target : entry.Targets) {
        paths.push_back(target.Path);
    }
    return JoinSeq(",", paths);
}

// The assertion that would have caught the TMoveIndex bug: not "the recorded
// string looks plausible", but "the recorded path resolves, in the live
// scheme, to the very object the operation touched". `root` is the database
// root (e.g. "/MyRoot"); `entry` must carry a resolved PathOwnerId/PathLocalId
// (i.e. this runs after the op has completed, not mid-flight). Fails loudly
// if the database-relative Path does not resolve, or resolves to a different
// object than the one the extractor captured.
inline void AssertRecordedPathResolvesToTouchedObject(
    TTestActorRuntime& runtime, const TString& root, const TSchemeChangeRecordEntry& entry, size_t targetIdx = 0)
{
    UNIT_ASSERT_C(targetIdx < entry.Targets.size(),
        "targetIdx " << targetIdx << " out of range, entry has " << entry.Targets.size() << " targets");
    const TString& relPath = entry.Targets[targetIdx].Path;
    const TString absPath = relPath.empty() ? root : (root + "/" + relPath);

    // showPrivate: a changefeed lives under a table, which is not a common-sense
    // path, so the default describe refuses it before any identity check runs.
    auto describe = DescribePath(runtime, absPath, false, false, true);
    UNIT_ASSERT_C(describe.GetStatus() == NKikimrScheme::StatusSuccess,
        "recorded path \"" << relPath << "\" (absolute: \"" << absPath << "\") does not resolve "
        "in the live scheme -- a bare/approximate path must fail this check, not pass it");

    const ui64 resolvedOwnerId = describe.GetPathDescription().GetSelf().GetSchemeshardId();
    const ui64 resolvedLocalId = describe.GetPathDescription().GetSelf().GetPathId();
    UNIT_ASSERT_C(targetIdx != 0 || (entry.PathOwnerId != 0 || entry.PathLocalId != 0),
        "entry has no resolved PathId to compare against -- was it read before FinalizeSchemeChangeRecord ran?");
    if (targetIdx == 0) {
        UNIT_ASSERT_VALUES_EQUAL_C(resolvedOwnerId, entry.PathOwnerId,
            "recorded path \"" << relPath << "\" resolves to a different object's owner than "
            "the one the operation touched (PathOwnerId mismatch)");
        UNIT_ASSERT_VALUES_EQUAL_C(resolvedLocalId, entry.PathLocalId,
            "recorded path \"" << relPath << "\" resolves to a different object than "
            "the one the operation touched (PathLocalId mismatch): expected "
            << entry.PathLocalId << ", resolved to " << resolvedLocalId);
    }
}

struct TSchemeChangeRecordsReadResult {
    TVector<TSchemeChangeRecordEntry> Entries;
    ui64 ClosedThroughPlanStep = 0;
};

inline TSchemeChangeRecordsReadResult ReadSchemeChangeRecordsFull(
    TTestActorRuntime& runtime)
{
    const TString tempSubId = "__internal_read_sub__";

    // Register at 0, not at the tail: this helper reads the log's whole
    // retained contents. If the floor has advanced, 0 clamps up to it and
    // reports LOST, which correctly reads everything still retained.
    TAutoPtr<IEventHandle> regHandle;
    RegisterSubscriberAt(runtime, tempSubId, 0, regHandle);

    // Step 1: Fetch metadata only (body is no longer returned by Fetch).
    TAutoPtr<IEventHandle> fetchHandle;
    auto* fetch = FetchSchemeChangeRecords(runtime, tempSubId, 0, 1000, fetchHandle);

    TSchemeChangeRecordsReadResult result;
    result.ClosedThroughPlanStep = fetch->Record.GetClosedThroughPlanStep();

    TVector<ui64> ordersWithBody;
    for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
        const auto& proto = fetch->Record.GetEntries(i);
        TSchemeChangeRecordEntry entry;
        entry.Order = proto.GetOrder();
        entry.TxId = proto.GetTxId();
        entry.PlanStep = proto.GetPlanStep();
        entry.OperationType = proto.GetOperationType();
        entry.PathOwnerId = proto.GetPathId().GetOwnerId();
        entry.PathLocalId = proto.GetPathId().GetLocalId();
        for (const auto& t : proto.GetTargets()) {
            TTestSchemeChangeTarget target;
            target.Path = t.GetPath();
            for (const auto& src : t.GetSourcePaths()) {
                target.SourcePaths.push_back(src);
            }
            entry.Targets.push_back(std::move(target));
        }
        entry.ObjectType = proto.GetObjectType();
        entry.Status = proto.GetStatus();
        entry.UserSID = proto.GetUserSID();
        entry.SchemaVersion = proto.GetSchemaVersion();
        entry.CompletedAtUs = proto.GetCompletedAtUs();
        entry.PositionKind = (ui32)proto.GetPositionKind();
        for (const auto& field : proto.GetRedactedFields()) {
            entry.RedactedFields.push_back(field);
        }
        if (proto.GetBodySizeBytes() > 0) {
            ordersWithBody.push_back(proto.GetOrder());
        }
        result.Entries.push_back(std::move(entry));
    }

    // Step 2: Fetch bodies for entries with non-zero BodySizeBytes; merge back.
    if (!ordersWithBody.empty()) {
        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, tempSubId, ordersWithBody, bodiesHandle);
        THashMap<ui64, TString> bodyByOrder;
        THashMap<ui64, TString> descByOrder;
        for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
            const auto& b = bodies->Record.GetEntries(i);
            bodyByOrder.emplace(b.GetOrder(), b.GetBody());
            descByOrder.emplace(b.GetOrder(), b.GetDescription());
        }
        for (auto& entry : result.Entries) {
            auto it = bodyByOrder.find(entry.Order);
            if (it != bodyByOrder.end() && !it->second.empty()) {
                Y_ABORT_UNLESS(entry.Body.ParseFromString(it->second));
            }
            auto dIt = descByOrder.find(entry.Order);
            if (dIt != descByOrder.end()) {
                entry.Description = dIt->second;
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

// Reads the durable rows of tables 141 + 143 directly, with no protocol
// involvement: no subscriber is registered, so unlike ReadSchemeChangeRecordsFull
// this does not perturb GetMinSubscriberOrder(). Asserts what was persisted
// rather than what the fetch handler chose to project.
//
// NOT equivalent to the protocol reader in one respect: this returns every row,
// including reserved-but-unfinalised ones (CompletedAtUs == 0), where a fetch
// stops at that barrier. Fine for a test that drives an op to completion and
// then locates its record; wrong for anything asserting a record COUNT, an
// emptiness, or barrier visibility -- use the protocol reader for those.
//
// SelectRange returns the selected columns ordered by column NAME, not by the
// order they appear in the select list, and reading the wrong index yields an
// empty value rather than an error. Both lists below are kept alphabetical so
// the two orderings coincide.
inline TVector<TSchemeChangeRecordEntry> ReadSchemeChangeRecordsFromTable(
    TTestActorRuntime& runtime)
{
    const TString recordsQuery = R"___(
        (
            (let range '('('Order (Null) (Void))))
            (let select '('BodySizeBytes 'CompletedAtUs 'Description 'ObjectType 'OperationType 'Order 'Path 'PathLocalId 'PathOwnerId 'PlanStep 'PositionKind 'RedactedFields 'SchemaVersion 'Status 'TxId 'UserSID))
            (let result (SelectRange 'SchemeChangeRecords range select '()))
            (return (AsList (SetResult 'R result)))
        )
    )___";
    auto recordsResult = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, recordsQuery);

    auto strOf = [](const NKikimrMiniKQL::TValue& v) -> TString {
        return v.HasBytes() ? TString(v.GetBytes()) : TString(v.GetText());
    };

    TVector<TSchemeChangeRecordEntry> entries;
    const auto& list = recordsResult.GetValue().GetStruct(0).GetOptional().GetStruct(0);
    for (size_t i = 0; i < list.ListSize(); ++i) {
        const auto& row = list.GetList(i);
        TSchemeChangeRecordEntry entry;
        entry.CompletedAtUs  = row.GetStruct(1).GetOptional().GetUint64();
        entry.Description    = strOf(row.GetStruct(2).GetOptional());
        entry.ObjectType     = row.GetStruct(3).GetOptional().GetUint32();
        // "OperationType" sorts before "Order" ('p' < 'r'), so these two are
        // not in the order they read.
        entry.OperationType  = row.GetStruct(4).GetOptional().GetUint32();
        entry.Order          = row.GetStruct(5).GetOptional().GetUint64();

        NKikimrSchemeShard::TSchemeChangeRecordTargets targets;
        const TString rawTargets = strOf(row.GetStruct(6).GetOptional());
        if (!rawTargets.empty()) {
            UNIT_ASSERT_C(targets.ParseFromString(rawTargets),
                "SchemeChangeRecords::Path did not parse as TSchemeChangeRecordTargets");
        }
        for (const auto& t : targets.GetTargets()) {
            TTestSchemeChangeTarget target;
            target.Path = t.GetPath();
            for (const auto& src : t.GetSourcePaths()) {
                target.SourcePaths.push_back(src);
            }
            entry.Targets.push_back(std::move(target));
        }

        entry.PathLocalId    = row.GetStruct(7).GetOptional().GetUint64();
        entry.PathOwnerId    = row.GetStruct(8).GetOptional().GetUint64();
        entry.PlanStep       = row.GetStruct(9).GetOptional().GetUint64();
        entry.PositionKind   = row.GetStruct(10).GetOptional().GetUint32();
        const TString redacted = strOf(row.GetStruct(11).GetOptional());
        if (!redacted.empty()) {
            for (const auto& f : StringSplitter(redacted).Split('\n').SkipEmpty()) {
                entry.RedactedFields.push_back(TString(f.Token()));
            }
        }
        entry.SchemaVersion  = row.GetStruct(12).GetOptional().GetUint64();
        entry.Status         = row.GetStruct(13).GetOptional().GetUint32();
        entry.TxId           = row.GetStruct(14).GetOptional().GetUint64();
        entry.UserSID        = strOf(row.GetStruct(15).GetOptional());
        entries.push_back(std::move(entry));
    }

    // Bodies live in table 143, keyed by the same Order.
    const TString bodiesQuery = R"___(
        (
            (let range '('('Order (Null) (Void))))
            (let select '('Body 'Order))
            (let result (SelectRange 'SchemeChangeRecordDetails range select '()))
            (return (AsList (SetResult 'R result)))
        )
    )___";
    auto bodiesResult = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, bodiesQuery);
    THashMap<ui64, TString> bodyByOrder;
    const auto& bodyList = bodiesResult.GetValue().GetStruct(0).GetOptional().GetStruct(0);
    for (size_t i = 0; i < bodyList.ListSize(); ++i) {
        const auto& row = bodyList.GetList(i);
        bodyByOrder.emplace(row.GetStruct(1).GetOptional().GetUint64(),
                            strOf(row.GetStruct(0).GetOptional()));
    }
    for (auto& entry : entries) {
        auto it = bodyByOrder.find(entry.Order);
        if (it != bodyByOrder.end() && !it->second.empty()) {
            Y_ABORT_UNLESS(entry.Body.ParseFromString(it->second));
        }
    }

    return entries;
}

// Cursor-independent physical-row oracle: returns the subset of `orders`
// still present on disk, gated only on subscriber existence. Pass an existing
// subscriber; a temp one would itself perturb GetMinSubscriberOrder().
inline TVector<ui64> ProbeRecordOrdersPresent(
    TTestActorRuntime& runtime, const TString& subscriberId, const TVector<ui64>& orders)
{
    TAutoPtr<IEventHandle> handle;
    auto* bodies = FetchSchemeChangeRecordBodies(runtime, subscriberId, orders, handle);
    TVector<ui64> present;
    for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
        present.push_back(bodies->Record.GetEntries(i).GetOrder());
    }
    Sort(present);
    return present;
}

// SchemeShard config knobs applied in one config notification. Unset members
// keep the caller's previous value instead of reverting to the default.
struct TSchemeShardConfigOverrides {
    TMaybe<ui64> MaxSchemeChangeRecords;
    TMaybe<ui64> SchemeChangeSubscriberStaleTtlSeconds;
    TMaybe<bool> RedactSensitiveSchemeChangeFields;
};

inline void ApplySchemeShardConfig(
    TTestActorRuntime& runtime, const TSchemeShardConfigOverrides& overrides)
{
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    auto& cfg = *request->Record.MutableConfig()->MutableSchemeShardConfig();
    if (overrides.MaxSchemeChangeRecords) {
        cfg.SetMaxSchemeChangeRecords(*overrides.MaxSchemeChangeRecords);
    }
    if (overrides.SchemeChangeSubscriberStaleTtlSeconds) {
        cfg.SetSchemeChangeSubscriberStaleTtlSeconds(*overrides.SchemeChangeSubscriberStaleTtlSeconds);
    }
    if (overrides.RedactSensitiveSchemeChangeFields) {
        cfg.SetRedactSensitiveSchemeChangeFields(*overrides.RedactSensitiveSchemeChangeFields);
    }
    SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
}

} // namespace NSchemeChangeRecordTestHelpers
