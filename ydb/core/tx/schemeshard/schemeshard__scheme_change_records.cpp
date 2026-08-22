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

} // namespace

bool TSchemeShard::PersistSchemeChangeRecordAtPropose(NIceDb::TNiceDb& db, TTxId txId, ui32 userTxIdx,
        const NKikimrSchemeOp::TModifyScheme& userTx, TOperation::TSchemeChangeSlot& slot,
        const TString& userSid) {
    if (IsChurnOp(userTx.GetOperationType())) {
        return false;
    }

    // Redact every (Ydb.sensitive) field before persisting: passwords, access
    // keys, and secret values must never reach the outbox or subscribers.
    NKikimrSchemeOp::TModifyScheme redacted = userTx;
    ClearSensitiveFields(&redacted);

    TString body;
    {
        bool ok = redacted.SerializeToString(&body);
        Y_DEBUG_ABORT_UNLESS(ok);
    }

    // If no target name is found, leave path empty rather than falling back
    // to WorkingDir: an unset target is honest, the parent directory is not.
    const TString targetName = ExtractSchemeChangeTargetName(userTx);
    TString path;
    if (!targetName.empty()) {
        path = userTx.GetWorkingDir();
        if (path.empty() || path.back() != '/') path += '/';
        path += targetName;
    }

    const ui64 order = AllocateSchemeChangeOrderInMemory();

    TestSchemeChangeRedoBytesAccum += body.size() + 128;

    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(order).Update(
        NIceDb::TUpdate<T::TxId>(ui64(txId)),
        NIceDb::TUpdate<T::OperationType>(static_cast<ui32>(userTx.GetOperationType())),
        NIceDb::TUpdate<T::Path>(path),
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
    PersistSchemeChangePendingOrder(db, txId, userTxIdx, order, path);

    slot.UserTxIdx = userTxIdx;
    slot.Order = order;
    slot.Path = path;
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
    if (!slot.Path.empty()) {
        TPath resolved = TPath::Resolve(slot.Path, this);
        if (resolved.IsResolved()) {
            resolvedPathId = resolved.Base()->PathId;
            resolvedObjectType = resolved.Base()->PathType;
            schemaVersion = resolved.Base()->DirAlterVersion;
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
