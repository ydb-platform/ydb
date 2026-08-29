#pragma once

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/scheme_change_records.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/cms/console/console.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NSchemeChangeRecordTestHelpers {

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// Mirrors NKikimrSchemeShard::TSchemeChangeTarget: SourcePaths is empty for a
// plain create/alter/drop, non-empty for a move/rename or copy target.
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

// Asserts the recorded path resolves, in the live scheme, to the very object
// the operation touched; `entry` must already carry a resolved PathId.
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

// Reads tables 141 + 143 directly, returning every row including unfinalised
// ones. Select lists stay alphabetical: SelectRange orders columns by name.
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

} // namespace NSchemeChangeRecordTestHelpers
