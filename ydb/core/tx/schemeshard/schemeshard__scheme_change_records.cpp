#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

namespace NKikimr::NSchemeShard {

namespace {

// Name of the object a user-level TModifyScheme targets.
//
// Generic by construction rather than a switch over operation types: the
// previous hand-rolled 6-case version left targetName empty for every other
// object type, so Path fell back to bare WorkingDir -- which RESOLVES, to the
// parent directory. The record then carried the parent's PathId and
// ObjectType=EPathTypeDir: confidently wrong identity, which is worse for a
// consumer than none. Reflection means a newly added object type is covered
// without anyone remembering to extend a switch.
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

} // namespace

bool TSchemeShard::PersistSchemeChangeRecordAtPropose(NIceDb::TNiceDb& db, TTxId txId, ui32 userTxIdx,
        const NKikimrSchemeOp::TModifyScheme& userTx, TOperation::TSchemeChangeSlot& slot) {
    // Churn ops never reach the outbox. IsChurnOp is the single filter
    // (schemeshard_subop_types.cpp); the other call site is the propose gate,
    // which uses it to avoid rejecting an op on an outbox it never feeds.
    if (IsChurnOp(userTx.GetOperationType())) {
        return false;
    }

    // Redact plaintext secrets before persisting. TSecretSchemaOp.Value is
    // documented as "original, unencrypted value" and is marked sensitive in
    // the proto -- but that only governs logging; without this it would be
    // written verbatim into the outbox and handed to every subscriber.
    NKikimrSchemeOp::TModifyScheme redacted;
    const NKikimrSchemeOp::TModifyScheme* toPersist = &userTx;
    if (userTx.HasCreateSecret() || userTx.HasAlterSecret()) {
        redacted = userTx;
        if (redacted.HasCreateSecret()) {
            redacted.MutableCreateSecret()->ClearValue();
        }
        if (redacted.HasAlterSecret()) {
            redacted.MutableAlterSecret()->ClearValue();
        }
        toPersist = &redacted;
    }

    TString body;
    {
        bool ok = toPersist->SerializeToString(&body);
        Y_DEBUG_ABORT_UNLESS(ok);
    }

    // Best-effort synthesis of a human-friendly Path by joining WorkingDir with
    // the target name extracted from the body's typed sub-message. For ops
    // without a single identifiable target, fall back to WorkingDir.
    const TString targetName = ExtractSchemeChangeTargetName(userTx);
    TString path = userTx.GetWorkingDir();
    if (!targetName.empty()) {
        if (path.empty() || path.back() != '/') path += '/';
        path += targetName;
    }

    const ui64 order = AllocateSchemeChangeOrderInMemory();

    // Approximate the redo cost this record adds: the body dominates, the fixed
    // columns are a small constant. Description is added at finalisation.
    TestSchemeChangeRedoBytesAccum += body.size() + 128;

    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(order).Update(
        NIceDb::TUpdate<T::TxId>(ui64(txId)),
        NIceDb::TUpdate<T::OperationType>(static_cast<ui32>(userTx.GetOperationType())),
        NIceDb::TUpdate<T::Path>(path),
        NIceDb::TUpdate<T::Status>(ui32(NKikimrScheme::StatusAccepted)),
        NIceDb::TUpdate<T::BodySize>(body.size()),
        // Zero until finalisation. This is the marker the fetch path stops at,
        // so a record for an operation still in flight is never handed out.
        NIceDb::TUpdate<T::CompletedAtUs>(ui64(0))
    );
    if (!body.empty()) {
        db.Table<Schema::SchemeChangeRecordDetails>().Key(order).Update(
            NIceDb::TUpdate<Schema::SchemeChangeRecordDetails::Body>(body)
        );
    }

    PersistUpdateNextSchemeChangeOrder(db);
    PersistSchemeChangePendingOrder(db, txId, userTxIdx, order, path);

    slot.UserTxIdx = userTxIdx;
    slot.Order = order;
    slot.Path = path;
    return true;
}

void TSchemeShard::FinalizeSchemeChangeRecord(NIceDb::TNiceDb& db, const TActorContext& ctx,
        const TOperation::TSchemeChangeSlot& slot, TStepId planStep) {
    // Resolve the record's position in the coordinator timeline.
    //
    // Ops that complete at propose time (TModifyACL is the canonical one: no
    // TxState at all, ProgressState/AbortUnsafe both Y_ABORT) never reach a
    // coordinator, so they have no step of their own. They borrow the ceiling:
    // SchemeShard had already applied that step, so the op provably happened
    // after every coordinated tx planned at it -- hence BUCKETED sorts after
    // EXACT at equal step. Safe precisely because such ops touch no shards and
    // so have no data to be interleaved against.
    //
    // Clamp here, at the single write site: PlanStep must never be persisted as
    // 0, or a consumer bucketing by (PlanStep, PositionKind) would sort those
    // records before everything.
    ui64 step = ui64(planStep);
    auto positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT;
    if (step == 0 || planStep == InvalidStepId) {
        step = LastAssignedPlanStep;
        positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_BUCKETED;
    }
    step = Max<ui64>(step, 1);

    // Resolve the record's identity rather than shipping placeholders. A
    // consumer must be able to act on PathId/ObjectType without parsing the
    // request body -- and after a DROP the name is gone, so identity has to be
    // captured here, at completion.
    TPathId resolvedPathId;
    auto resolvedObjectType = NKikimrSchemeOp::EPathTypeInvalid;
    ui64 schemaVersion = 0;
    TString userSid;
    if (!slot.Path.empty()) {
        TPath resolved = TPath::Resolve(slot.Path, this);
        if (resolved.IsResolved()) {
            resolvedPathId = resolved.Base()->PathId;
            resolvedObjectType = resolved.Base()->PathType;
            schemaVersion = resolved.Base()->DirAlterVersion;
            userSid = resolved.Base()->Owner;
        }
    }

    // Capture the resolved object description. In-memory only (NFR1): this is
    // the same serialization the publish path already produces, and it must be
    // taken now -- after a DROP the object is gone, and a later read would race
    // a newer version.
    TString description;
    // A secret's description carries the value, so it is never captured.
    // Consumers get identity for secrets, never content.
    if (resolvedPathId && resolvedObjectType != NKikimrSchemeOp::EPathTypeSecret) {
        // Schema only. The default options return partitioning info, partition
        // config, children and range keys -- and those are the two unbounded
        // terms: MaxShardsInPath is 35k partitions and MaxChildrenInDir is 100k
        // entries, all of which would be serialized into the local-DB redo log
        // on every single DDL. What a consumer needs to recreate the object is
        // its schema; the partition layout is re-derived at incremental-backup
        // time, which is where it belongs.
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

    // CompletedAtUs is written last in intent: a non-zero value is what makes
    // this row visible to subscribers, and everything it promises is set in the
    // same Update.
    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(slot.Order).Update(
        NIceDb::TUpdate<T::PathOwnerId>(resolvedPathId.OwnerId),
        NIceDb::TUpdate<T::PathLocalId>(resolvedPathId.LocalPathId),
        NIceDb::TUpdate<T::ObjectType>(ui32(resolvedObjectType)),
        NIceDb::TUpdate<T::Status>(ui32(NKikimrScheme::StatusSuccess)),
        NIceDb::TUpdate<T::UserSID>(userSid),
        NIceDb::TUpdate<T::SchemaVersion>(schemaVersion),
        NIceDb::TUpdate<T::PlanStep>(step),
        NIceDb::TUpdate<T::Description>(description),
        NIceDb::TUpdate<T::PositionKind>(static_cast<ui32>(positionKind)),
        NIceDb::TUpdate<T::CompletedAtUs>(ctx.Now().MicroSeconds())
    );
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
    // Approximate the redo cost this record adds: the two blobs dominate, the
    // fixed columns are a small constant.
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
        NIceDb::TUpdate<T::BodySize>(entry.Body.size()),
        NIceDb::TUpdate<T::Description>(entry.Description),
        NIceDb::TUpdate<T::PositionKind>(entry.PositionKind)
    );
    if (!entry.Body.empty()) {
        db.Table<Schema::SchemeChangeRecordDetails>().Key(entry.Order).Update(
            NIceDb::TUpdate<Schema::SchemeChangeRecordDetails::Body>(entry.Body)
        );
    }
}

bool TSchemeShard::DeleteAckedSchemeChangeRecords(NIceDb::TNiceDb& db, ui64 oldMinOrder, ui64 newMinOrder,
        ui64 limit, bool& hasMore) {
    hasMore = false;
    if (newMinOrder <= oldMinOrder) {
        return true;
    }
    // Resume above whatever is already physically gone. Callers pass their own
    // notion of "already acked" (0, for the periodic cleanup tx), but deleted
    // rows linger as tombstones until compaction, so restarting at order 1
    // re-seeks the whole dead prefix on every batch -- quadratic while
    // draining a backlog.
    const ui64 from = Max(oldMinOrder, SchemeChangeFloorOrder) + 1;
    if (newMinOrder < from) {
        return true;
    }
    // Bound both ends. The loop below breaks past newMinOrder, so iteration was
    // always bounded -- but a one-sided GreaterOrEqual().Select() precharges
    // with an empty maxKey, which charges the whole tail of the table. That
    // made a cleanup deleting a handful of rows pay for every record sitting
    // above them. LessOrEqual routes to the bounded RangeKeyOperations path.
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
    // Advance the floor to the last row actually removed -- not to newMinOrder,
    // which the batch limit may have stopped short of. Only ever moves forward.
    if (lastDeleted > SchemeChangeFloorOrder) {
        SchemeChangeFloorOrder = lastDeleted;
        PersistSchemeChangeFloorOrder(db);
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
