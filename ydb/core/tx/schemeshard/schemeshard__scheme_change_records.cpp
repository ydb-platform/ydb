#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

namespace NKikimr::NSchemeShard {

namespace {

// Name of the object a user-level TModifyScheme targets, found generically via
// reflection so a newly added object type is covered without extending a switch.
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
    if (IsChurnOp(userTx.GetOperationType())) {
        return false;
    }

    // Redact plaintext secrets before persisting: TSecretSchemaOp.Value is
    // sensitive and must never be written to the outbox or handed to subscribers.
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

    const TString targetName = ExtractSchemeChangeTargetName(userTx);
    TString path = userTx.GetWorkingDir();
    if (!targetName.empty()) {
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
        NIceDb::TUpdate<T::BodySize>(body.size()),
        // Zero until finalisation: the marker the fetch path stops at, so a
        // record for an operation still in flight is never handed out.
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
    // Ops completing at propose (e.g. TModifyACL) have no coordinator step:
    // they borrow the ceiling, sort as BUCKETED, and clamp to >= 1.
    ui64 step = ui64(planStep);
    auto positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT;
    if (step == 0 || planStep == InvalidStepId) {
        step = LastAssignedPlanStep;
        positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_BUCKETED;
    }
    step = Max<ui64>(step, 1);

    // Resolved here rather than shipping placeholders: after a DROP the name
    // is gone, so identity must be captured now, at completion.
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

    // Must be taken now -- after a DROP the object is gone.
    TString description;
    // A secret's description carries the value, so it is never captured.
    if (resolvedPathId && resolvedObjectType != NKikimrSchemeOp::EPathTypeSecret) {
        // Schema only: partitioning/children/range-key info is unbounded
        // (up to 35k partitions, 100k dir entries) and would bloat the redo log.
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

    // CompletedAtUs is non-zero here, making this row visible to subscribers,
    // so everything it promises must already be set in this same Update.
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
    // Resume above whatever is already physically gone (tombstones linger
    // until compaction), or restarting at order 1 is quadratic on a backlog.
    const ui64 from = Max(oldMinOrder, SchemeChangeFloorOrder) + 1;
    if (newMinOrder < from) {
        return true;
    }
    // Bound both ends: an unbounded GreaterOrEqual().Select() would precharge
    // the whole tail of the table. LessOrEqual keeps this on the bounded path.
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
    // limit may have stopped short of it.
    if (lastDeleted > SchemeChangeFloorOrder) {
        SchemeChangeFloorOrder = lastDeleted;
        PersistSchemeChangeFloorOrder(db);
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
