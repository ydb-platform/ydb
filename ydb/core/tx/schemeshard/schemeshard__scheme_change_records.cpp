#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

namespace NKikimr::NSchemeShard {

namespace {

// Name of the object a user-level TModifyScheme targets, found via reflection
// so a newly added object type is covered without extending a switch.
TString ExtractSchemeChangeTargetName(const NKikimrSchemeOp::TModifyScheme& tx) {
    // The only shape where the name is not directly at <SubMessage>.Name.
    if (tx.HasCreateIndexedTable()) {
        return tx.GetCreateIndexedTable().GetTableDescription().GetName();
    }

    const auto* refl = tx.GetReflection();
    std::vector<const google::protobuf::FieldDescriptor*> setFields;
    refl->ListFields(tx, &setFields);

    for (const auto* field : setFields) {
        if (field->is_repeated()
            || field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        const auto& sub = refl->GetMessage(tx, field);
        const auto* nameField = sub.GetDescriptor()->FindFieldByName("Name");
        if (!nameField
            || nameField->is_repeated()
            || nameField->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
            continue;
        }
        const auto* subRefl = sub.GetReflection();
        if (!subRefl->HasField(sub, nameField)) {
            continue;
        }
        TString name = subRefl->GetString(sub, nameField);
        if (!name.empty()) {
            return name;
        }
    }
    return {};
}

// Database-relative form of a resolved path: the domain prefix and its
// joining slash stripped from PathString(). The database root itself is ""
// (its PathString() equals the domain prefix), so a child of root is "name"
// with no leading slash.
TString RelativeToDomain(const TPath& resolved) {
    TString abs = resolved.PathString();
    TString domain = resolved.GetDomainPathString();
    Y_DEBUG_ABORT_UNLESS(abs.StartsWith(domain));
    TString rel = abs.substr(domain.size());
    if (!rel.empty() && rel[0] == '/') {
        rel = rel.substr(1);
    }
    return rel;
}

struct TResolvedSchemeChangePath {
    // Absolute; used only to re-resolve at finalisation.
    TString Absolute;
    // Database-relative; what gets persisted in the outbox row.
    TString Relative;
};

// Authoritative propose-time path for a user-level TModifyScheme: never a
// string-assembled guess. Not classified by op type -- ConvertToTxType
// collapses many EOperationType values onto TxInvalid, and IsCreate/IsDrop
// hard-abort on TxInvalid, so op-type dispatch here would crash the tablet
// on exactly the ops this function most needs to handle (e.g. CreateCdcStream).
// Instead: try resolving the target itself first (covers drop/alter, whose
// target already exists); if that fails, fall back to resolving WorkingDir
// alone (which exists for any valid DDL) and appending the target name --
// covers create, whose own target does not exist yet. A Drop-by-PathId
// request (Drop.Id set, no Name/WorkingDir -- e.g. ForceDropUnsafe) resolves
// directly by PathId, the same way the subop itself resolves its target.
// Returns Nothing() when nothing resolves; the caller must then reject the
// propose, never store an approximation.
TMaybe<TResolvedSchemeChangePath> ResolveSchemeChangePath(const NKikimrSchemeOp::TModifyScheme& userTx, TSchemeShard* ss) {
    const TString targetName = ExtractSchemeChangeTargetName(userTx);
    if (targetName.empty()) {
        if (userTx.HasDrop() && userTx.GetDrop().GetId() != 0) {
            TPath target = TPath::Init(ss->MakeLocalId(userTx.GetDrop().GetId()), ss);
            if (target.IsResolved()) {
                return TResolvedSchemeChangePath{target.PathString(), RelativeToDomain(target)};
            }
        }
        return Nothing();
    }

    TString abs = userTx.GetWorkingDir();
    if (abs.empty() || abs.back() != '/') abs += '/';
    abs += targetName;

    TPath target = TPath::Resolve(abs, ss);
    if (target.IsResolved()) {
        return TResolvedSchemeChangePath{abs, RelativeToDomain(target)};
    }

    TPath workingDir = TPath::Resolve(userTx.GetWorkingDir(), ss);
    if (!workingDir.IsResolved()) {
        return Nothing();
    }
    TString relParent = RelativeToDomain(workingDir);
    if (!relParent.empty()) {
        relParent += '/';
    }
    return TResolvedSchemeChangePath{abs, relParent + targetName};
}

} // namespace

bool TSchemeShard::CheckSchemeChangeRecordHasPath(const NKikimrSchemeOp::TModifyScheme& userTx, TString& rejectReason) {
    if (IsChurnOp(userTx.GetOperationType()) || IsPathlessOp(userTx.GetOperationType())) {
        return true;
    }
    if (ResolveSchemeChangePath(userTx, this)) {
        return true;
    }
    TabletCounters->Cumulative()[COUNTER_SCHEME_CHANGE_PATH_MISSING].Increment(1);
    rejectReason = TStringBuilder() << "scheme change outbox could not resolve a path for operation type "
        << NKikimrSchemeOp::EOperationType_Name(userTx.GetOperationType())
        << "; this operation is missing from the pathless allowlist";
    return false;
}

bool TSchemeShard::PersistSchemeChangeRecordAtPropose(NIceDb::TNiceDb& db, TTxId txId, ui32 userTxIdx,
        const NKikimrSchemeOp::TModifyScheme& userTx, TOperation::TSchemeChangeSlot& slot,
        const TString& userSid) {
    if (IsChurnOp(userTx.GetOperationType())) {
        return false;
    }

    // CheckSchemeChangeRecordHasPath must have already validated this at the
    // caller, before any record in this batch was written -- a path-bearing
    // op with no path rejects the whole propose, not just this one record.
    TMaybe<TResolvedSchemeChangePath> path = ResolveSchemeChangePath(userTx, this);

    // Redact every (Ydb.sensitive) field before persisting: passwords, access
    // keys, and secret values must never reach the outbox or subscribers.
    NKikimrSchemeOp::TModifyScheme redacted = userTx;
    ClearSensitiveFields(&redacted);

    TString body;
    {
        bool ok = redacted.SerializeToString(&body);
        Y_DEBUG_ABORT_UNLESS(ok);
    }

    const ui64 order = AllocateSchemeChangeOrderInMemory();

    TestSchemeChangeRedoBytesAccum += body.size() + 128;

    const TString relPath = path ? path->Relative : TString();
    const TString absPath = path ? path->Absolute : TString();

    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(order).Update(
        NIceDb::TUpdate<T::TxId>(ui64(txId)),
        NIceDb::TUpdate<T::OperationType>(static_cast<ui32>(userTx.GetOperationType())),
        NIceDb::TUpdate<T::Path>(relPath),
        NIceDb::TUpdate<T::Status>(ui32(NKikimrScheme::StatusAccepted)),
        NIceDb::TUpdate<T::UserSID>(userSid),
        NIceDb::TUpdate<T::BodySizeBytes>(body.size()),
        // Zero until finalisation; the fetch path stops here so an in-flight
        // record is never handed out.
        NIceDb::TUpdate<T::CompletedAtUs>(ui64(0))
    );
    if (!body.empty()) {
        db.Table<Schema::SchemeChangeRecordDetails>().Key(order).Update(
            NIceDb::TUpdate<Schema::SchemeChangeRecordDetails::Body>(body)
        );
    }

    PersistUpdateNextSchemeChangeOrder(db);
    // Absolute form: finalisation re-resolves via TPath::Resolve, which
    // requires an absolute path, not the database-relative one stored above.
    PersistSchemeChangePendingOrder(db, txId, userTxIdx, order, absPath);

    slot.UserTxIdx = userTxIdx;
    slot.Order = order;
    slot.Path = absPath;
    slot.UserSid = userSid;
    return true;
}

void TSchemeShard::FinalizeSchemeChangeRecord(NIceDb::TNiceDb& db, const TActorContext& ctx,
        const TOperation::TSchemeChangeSlot& slot, TStepId planStep, bool aborted) {
    // Ops completing at propose (e.g. TModifyACL) have no coordinator step: they
    // borrow the ceiling and sort as BUCKETED, clamped to >= 1.
    ui64 step = ui64(planStep);
    auto positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT;
    if (step == 0 || planStep == InvalidStepId) {
        step = LastAssignedPlanStep;
        positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_BUCKETED;
    }
    step = Max<ui64>(step, 1);

    // Must be captured now: after a DROP the name is gone.
    TPathId resolvedPathId;
    auto resolvedObjectType = NKikimrSchemeOp::EPathTypeInvalid;
    ui64 schemaVersion = 0;
    // Canonical resolved path, database-relative; only set on success. A drop
    // keeps the propose-time value written by PersistSchemeChangeRecordAtPropose,
    // since the object is already gone by the time this runs.
    TMaybe<TString> canonicalRelativePath;
    if (!slot.Path.empty()) {
        TPath resolved = TPath::Resolve(slot.Path, this);
        if (resolved.IsResolved()) {
            resolvedPathId = resolved.Base()->PathId;
            resolvedObjectType = resolved.Base()->PathType;
            schemaVersion = resolved.Base()->DirAlterVersion;
            canonicalRelativePath = RelativeToDomain(resolved);
        }
    }

    // Must be taken now -- after a DROP the object is gone.
    TString description;
    // A secret's description carries the value, so it is never captured.
    if (resolvedPathId && resolvedObjectType != NKikimrSchemeOp::EPathTypeSecret) {
        // Schema only: partitioning/children/range-key info is unbounded and
        // would bloat the redo log.
        NKikimrSchemeOp::TDescribeOptions opts;
        opts.SetReturnPartitioningInfo(false);
        opts.SetReturnPartitionConfig(false);
        opts.SetReturnChildren(false);
        opts.SetReturnRangeKey(false);
        auto desc = DescribePath(this, ctx, resolvedPathId, opts);
        Y_UNUSED(desc->GetRecord().SerializeToString(&description));
    }

    TestSchemeChangeRedoBytesAccum += description.size();
    TabletCounters->Cumulative()[COUNTER_SCHEME_CHANGE_DESCRIPTION_BYTES].Increment(description.size());

    // CompletedAtUs is non-zero here, making the row visible to subscribers,
    // so all its fields must be set in this same Update.
    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(slot.Order).Update(
        NIceDb::TUpdate<T::PathOwnerId>(resolvedPathId.OwnerId),
        NIceDb::TUpdate<T::PathLocalId>(resolvedPathId.LocalPathId),
        NIceDb::TUpdate<T::ObjectType>(ui32(resolvedObjectType)),
        NIceDb::TUpdate<T::Status>(ui32(aborted
            ? NKikimrScheme::StatusPreconditionFailed
            : NKikimrScheme::StatusSuccess)),
        NIceDb::TUpdate<T::SchemaVersion>(schemaVersion),
        NIceDb::TUpdate<T::PlanStep>(step),
        NIceDb::TUpdate<T::Description>(description),
        NIceDb::TUpdate<T::PositionKind>(static_cast<ui32>(positionKind)),
        NIceDb::TUpdate<T::CompletedAtUs>(ctx.Now().MicroSeconds())
    );
    // Overwrite the propose-time synthesis with the canonical resolved path.
    // Only on success: a drop's target is already gone, so the propose-time
    // value (still database-relative, already correct) is left in place.
    if (canonicalRelativePath) {
        db.Table<T>().Key(slot.Order).Update(
            NIceDb::TUpdate<T::Path>(*canonicalRelativePath)
        );
    }
}

ui64 TSchemeShard::AllocateSchemeChangeOrder(NIceDb::TNiceDb& db) {
    ui64 id = ++NextSchemeChangeOrder;
    PersistUpdateNextSchemeChangeOrder(db);
    return id;
}

ui64 TSchemeShard::AllocateSchemeChangeOrderInMemory() {
    return ++NextSchemeChangeOrder;
}

void TSchemeShard::EnqueueSchemeChangeRecordsCleanup(const TActorContext& ctx) {
    ctx.Schedule(SchemeChangeCleanupInterval, new TEvPrivate::TEvSchemeChangeRecordsCleanup());
}

void TSchemeShard::Handle(TEvPrivate::TEvSchemeChangeRecordsCleanup::TPtr&, const TActorContext& ctx) {
    Execute(CreateTxSchemeChangeRecordsCleanup(), ctx);
}

void TSchemeShard::PersistSchemeChangeRecord(NIceDb::TNiceDb& db, const TSchemeChangeRecordData& entry) {
    TestSchemeChangeRedoBytesAccum += entry.Body.size() + entry.Description.size() + 128;
    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(entry.Order).Update(
        NIceDb::TUpdate<T::TxId>(ui64(entry.TxId)),
        NIceDb::TUpdate<T::OperationType>(entry.OpType),
        NIceDb::TUpdate<T::PathOwnerId>(entry.PathId.OwnerId),
        NIceDb::TUpdate<T::PathLocalId>(entry.PathId.LocalPathId),
        NIceDb::TUpdate<T::Path>(entry.Path),
        NIceDb::TUpdate<T::ObjectType>(ui32(entry.ObjectType)),
        NIceDb::TUpdate<T::Status>(ui32(entry.Status)),
        NIceDb::TUpdate<T::UserSID>(entry.UserSid),
        NIceDb::TUpdate<T::SchemaVersion>(entry.SchemaVersion),
        NIceDb::TUpdate<T::CompletedAtUs>(entry.CompletedAtUs.MicroSeconds()),
        NIceDb::TUpdate<T::PlanStep>(ui64(entry.PlanStep)),
        NIceDb::TUpdate<T::BodySizeBytes>(entry.Body.size()),
        NIceDb::TUpdate<T::Description>(entry.Description),
        NIceDb::TUpdate<T::PositionKind>(entry.PositionKind)
    );
    if (!entry.Body.empty()) {
        db.Table<Schema::SchemeChangeRecordDetails>().Key(entry.Order).Update(
            NIceDb::TUpdate<Schema::SchemeChangeRecordDetails::Body>(entry.Body)
        );
    }
}

bool TSchemeShard::DeleteAckedSchemeChangeRecords(NIceDb::TNiceDb& db, ui64 newMinOrder,
        ui64 limit, bool& hasMore) {
    hasMore = false;
    // SchemeChangeFloorOrder is the only sound lower bound: it is what was
    // actually deleted, not a retention watermark that a prior pass may not
    // have fully drained.
    if (newMinOrder <= SchemeChangeFloorOrder) {
        return true;
    }
    const ui64 from = SchemeChangeFloorOrder + 1;
    // Bound both ends: an unbounded GreaterOrEqual().Select() would precharge
    // the whole tail of the table.
    auto logRowset = db.Table<Schema::SchemeChangeRecords>()
        .GreaterOrEqual(from)
        .LessOrEqual(newMinOrder)
        .Select();
    if (!logRowset.IsReady()) {
        return false;
    }
    ui64 deleted = 0;
    ui64 lastDeleted = 0;
    while (!logRowset.EndOfSet()) {
        TabletCounters->Cumulative()[COUNTER_SCHEME_CHANGE_ROWS_SCANNED].Increment(1);
        ui64 order = logRowset.GetValue<Schema::SchemeChangeRecords::Order>();
        if (order > newMinOrder) {
            break;
        }
        if (deleted >= limit) {
            hasMore = true;
            break;
        }
        db.Table<Schema::SchemeChangeRecords>().Key(order).Delete();
        db.Table<Schema::SchemeChangeRecordDetails>().Key(order).Delete();
        lastDeleted = order;
        ++deleted;
        if (!logRowset.Next()) {
            return false;
        }
    }
    // Advance to the last row actually removed, not newMinOrder: the batch
    // limit may have stopped short.
    if (lastDeleted > SchemeChangeFloorOrder) {
        SchemeChangeFloorOrder = lastDeleted;
        PersistSchemeChangeFloorOrder(db);
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
