#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/protos/schemeshard/scheme_change_records.pb.h>

#include <util/string/join.h>

namespace NKikimr::NSchemeShard {

namespace {

// One (destination, source) pair extracted from the tx, before resolution.
// Source is absolute and empty when the op has no source (plain create).
struct TSchemeChangeRawTarget {
    TString AbsDstPath;
    TString AbsSrcPath;
};

// Ops whose target is named by an absolute DstPath rather than a bare name
// under WorkingDir -- a rename/move, where the object's new identity is its
// destination and SrcPath is where it used to live. Returns empty when tx
// does not carry such a shape.
TMaybe<TSchemeChangeRawTarget> ExtractSchemeChangeMoveTarget(const NKikimrSchemeOp::TModifyScheme& tx) {
    if (tx.HasMoveTable()) {
        return TSchemeChangeRawTarget{tx.GetMoveTable().GetDstPath(), tx.GetMoveTable().GetSrcPath()};
    }
    if (tx.HasMoveTableIndex()) {
        return TSchemeChangeRawTarget{tx.GetMoveTableIndex().GetDstPath(), tx.GetMoveTableIndex().GetSrcPath()};
    }
    if (tx.HasMoveSequence()) {
        return TSchemeChangeRawTarget{tx.GetMoveSequence().GetDstPath(), tx.GetMoveSequence().GetSrcPath()};
    }
    if (tx.HasMoveIndex()) {
        // Unlike MoveTable/MoveTableIndex/MoveSequence, TMoveIndex.SrcPath/
        // DstPath are index names relative to TablePath, not absolute paths
        // (confirmed against index/operation_move_index.cpp, which resolves
        // them via mainTablePath.Child(...)). Join with TablePath so the
        // result matches what every other move shape carries.
        const auto& moveIndex = tx.GetMoveIndex();
        return TSchemeChangeRawTarget{
            TStringBuilder() << moveIndex.GetTablePath() << '/' << moveIndex.GetDstPath(),
            TStringBuilder() << moveIndex.GetTablePath() << '/' << moveIndex.GetSrcPath()};
    }
    return Nothing();
}

// Ops that name N (destination, source) pairs rather than one -- a consistent
// copy of N tables in a single user-level tx, each carrying its own SrcPath.
// Returns empty when tx does not carry such a shape, distinct from a shape
// that carries zero targets.
TVector<TSchemeChangeRawTarget> ExtractSchemeChangeCopyTargets(const NKikimrSchemeOp::TModifyScheme& tx) {
    TVector<TSchemeChangeRawTarget> result;
    if (tx.HasCreateConsistentCopyTables()) {
        for (const auto& copy : tx.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
            result.push_back(TSchemeChangeRawTarget{copy.GetDstPath(), copy.GetSrcPath()});
        }
    }
    return result;
}

// Name of the object a user-level TModifyScheme targets, found via reflection
// so a newly added object type is covered without extending a switch.
TString ExtractSchemeChangeTargetName(const NKikimrSchemeOp::TModifyScheme& tx) {
    // Shapes where the name is not at a direct submessage's scalar Name field,
    // so the generic one-level reflection walk below cannot find it.
    if (tx.HasCreateIndexedTable()) {
        return tx.GetCreateIndexedTable().GetTableDescription().GetName();
    }
    // TAlterUserAttributes names its target PathName, not Name.
    if (tx.HasAlterUserAttributes()) {
        return tx.GetAlterUserAttributes().GetPathName();
    }
    // TTruncateTable/TCreateContinuousBackup/TAlterContinuousBackup/
    // TDropContinuousBackup/TRestoreTask all name their target table
    // TableName, not Name. Restore's AbortPropose is an unconditional
    // Y_ABORT stub, so an unresolved path here crashes the tablet rather
    // than cleanly refusing -- this case must never reach that fallback.
    if (tx.HasTruncateTable()) {
        return tx.GetTruncateTable().GetTableName();
    }
    if (tx.HasCreateContinuousBackup()) {
        return tx.GetCreateContinuousBackup().GetTableName();
    }
    if (tx.HasAlterContinuousBackup()) {
        return tx.GetAlterContinuousBackup().GetTableName();
    }
    if (tx.HasDropContinuousBackup()) {
        return tx.GetDropContinuousBackup().GetTableName();
    }
    if (tx.HasRestore()) {
        return tx.GetRestore().GetTableName();
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

struct TResolvedSchemeChangeTarget {
    // Absolute; used only to re-resolve at finalisation.
    TString Absolute;
    // Database-relative; what gets persisted in the outbox row as Path.
    TString Relative;
    // Database-relative source(s), empty unless this target is a move/copy.
    // Never re-resolved (a source may no longer exist by finalisation).
    TVector<TString> RelativeSources;
};

// Best-effort database-relative form of an absolute source path: resolved if
// possible (canonical form), else the raw prefix strip if resolution fails
// (e.g. a copy's source was concurrently dropped). Empty input yields empty
// output -- a target with no source must not synthesize one.
TString RelativeSourceToDomain(const TString& absSrcPath, TSchemeShard* ss) {
    if (absSrcPath.empty()) {
        return {};
    }
    TPath src = TPath::Resolve(absSrcPath, ss);
    if (src.IsResolved()) {
        return RelativeToDomain(src);
    }
    // Fallback: same domain-prefix-strip the resolved path uses, without
    // requiring the source to still exist.
    TString domain = TPath::Root(ss).PathString();
    if (absSrcPath.StartsWith(domain)) {
        TString rel = absSrcPath.substr(domain.size());
        if (!rel.empty() && rel[0] == '/') {
            rel = rel.substr(1);
        }
        return rel;
    }
    return absSrcPath;
}

// Resolves a single absolute destination path like a create: try the path
// itself (a rename/move target may already exist by the time this reruns at
// finalisation), else fall back to its parent directory and append the leaf.
// Returns Nothing() when neither resolves. Today every extractor produces at
// most one source per target (see ExtractSchemeChangeMoveTarget /
// ExtractSchemeChangeCopyTargets), so raw.AbsSrcPath is singular here; the
// wire/in-memory shape is still a list to allow a future many-sources op
// without another schema change.
TMaybe<TResolvedSchemeChangeTarget> ResolveRawTarget(const TSchemeChangeRawTarget& raw, TSchemeShard* ss) {
    TVector<TString> relSources;
    if (!raw.AbsSrcPath.empty()) {
        relSources.push_back(RelativeSourceToDomain(raw.AbsSrcPath, ss));
    }
    TPath target = TPath::Resolve(raw.AbsDstPath, ss);
    if (target.IsResolved()) {
        return TResolvedSchemeChangeTarget{raw.AbsDstPath, RelativeToDomain(target), relSources};
    }
    TPath parent = TPath::Resolve(TString(TStringBuf(raw.AbsDstPath).RBefore('/')), ss);
    if (parent.IsResolved()) {
        TString relParent = RelativeToDomain(parent);
        if (!relParent.empty()) {
            relParent += '/';
        }
        TString leaf = TString(TStringBuf(raw.AbsDstPath).RAfter('/'));
        return TResolvedSchemeChangeTarget{raw.AbsDstPath, relParent + leaf, relSources};
    }
    return Nothing();
}

// Authoritative propose-time targets for a user-level TModifyScheme: never a
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
// Returns Nothing() when nothing resolves, or any of N multi-target paths
// fails to resolve; the caller must then reject the propose, never store a
// partial or approximate result.
TMaybe<TVector<TResolvedSchemeChangeTarget>> ResolveSchemeChangeTargets(const NKikimrSchemeOp::TModifyScheme& userTx, TSchemeShard* ss) {
    // Multi-target: every destination must resolve, or the whole propose is
    // refused -- there is no such thing as a partially-authoritative record.
    const TVector<TSchemeChangeRawTarget> copyTargets = ExtractSchemeChangeCopyTargets(userTx);
    if (!copyTargets.empty()) {
        TVector<TResolvedSchemeChangeTarget> result;
        for (const auto& raw : copyTargets) {
            TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(raw, ss);
            if (!resolved) {
                return Nothing();
            }
            result.push_back(std::move(*resolved));
        }
        return result;
    }

    // A rename/move's target is its new (destination) location, not a bare
    // name under WorkingDir -- WorkingDir is not even set on these ops.
    // At propose time DstPath does not exist yet (the move has not executed),
    // so resolve like a create: fall back to its parent directory if the
    // full path itself does not resolve.
    TMaybe<TSchemeChangeRawTarget> moveTarget = ExtractSchemeChangeMoveTarget(userTx);
    if (moveTarget) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(*moveTarget, ss);
        if (!resolved) {
            return Nothing();
        }
        return TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)};
    }

    // AlterLogin has no path-bearing target at all -- it edits the
    // database's user/group registry, not a child scheme object. Its 8
    // sub-shapes (CreateUser/ModifyUser/RemoveUser/CreateGroup/...) each name
    // an identity in a different field, none of them a scheme path, so no
    // per-shape extraction is meaningful here. Attribute the record to the
    // database root itself (WorkingDir), the same object AlterSubDomain
    // targets when altering the root.
    if (userTx.HasAlterLogin()) {
        TPath workingDir = TPath::Resolve(userTx.GetWorkingDir(), ss);
        if (workingDir.IsResolved()) {
            return TVector<TResolvedSchemeChangeTarget>{
                TResolvedSchemeChangeTarget{userTx.GetWorkingDir(), RelativeToDomain(workingDir), {}}};
        }
        return Nothing();
    }

    const TString targetName = ExtractSchemeChangeTargetName(userTx);
    if (targetName.empty()) {
        if (userTx.HasDrop() && userTx.GetDrop().GetId() != 0) {
            TPath target = TPath::Init(ss->MakeLocalId(userTx.GetDrop().GetId()), ss);
            if (target.IsResolved()) {
                return TVector<TResolvedSchemeChangeTarget>{
                    TResolvedSchemeChangeTarget{target.PathString(), RelativeToDomain(target), {}}};
            }
        }
        return Nothing();
    }

    TString abs = userTx.GetWorkingDir();
    if (abs.empty() || abs.back() != '/') abs += '/';
    abs += targetName;

    TPath target = TPath::Resolve(abs, ss);
    if (target.IsResolved()) {
        return TVector<TResolvedSchemeChangeTarget>{TResolvedSchemeChangeTarget{abs, RelativeToDomain(target), {}}};
    }

    TPath workingDir = TPath::Resolve(userTx.GetWorkingDir(), ss);
    if (!workingDir.IsResolved()) {
        return Nothing();
    }
    TString relParent = RelativeToDomain(workingDir);
    if (!relParent.empty()) {
        relParent += '/';
    }
    return TVector<TResolvedSchemeChangeTarget>{TResolvedSchemeChangeTarget{abs, relParent + targetName, {}}};
}

} // namespace

// Wire encoding for the local-DB Path column: a serialized repeated-message
// protobuf, never a delimited string (a path could contain any delimiter).
TString TSchemeShard::EncodeSchemeChangeTargets(const TVector<TOperation::TSchemeChangeTarget>& targets) {
    NKikimrSchemeShard::TSchemeChangeRecordTargets msg;
    for (const auto& t : targets) {
        auto* target = msg.AddTargets();
        target->SetPath(t.Path);
        for (const auto& src : t.SourcePaths) {
            target->AddSourcePaths(src);
        }
    }
    TString result;
    Y_DEBUG_ABORT_UNLESS(msg.SerializeToString(&result));
    return result;
}

TVector<TOperation::TSchemeChangeTarget> TSchemeShard::DecodeSchemeChangeTargets(const TString& encoded) {
    NKikimrSchemeShard::TSchemeChangeRecordTargets msg;
    TVector<TOperation::TSchemeChangeTarget> result;
    if (encoded.empty() || !msg.ParseFromString(encoded)) {
        return result;
    }
    for (const auto& t : msg.GetTargets()) {
        TOperation::TSchemeChangeTarget target;
        target.Path = t.GetPath();
        for (const auto& src : t.GetSourcePaths()) {
            target.SourcePaths.push_back(src);
        }
        result.push_back(std::move(target));
    }
    return result;
}

bool TSchemeShard::CheckSchemeChangeRecordHasPath(const NKikimrSchemeOp::TModifyScheme& userTx, TString& rejectReason) {
    if (IsChurnOp(userTx.GetOperationType()) || IsPathlessOp(userTx.GetOperationType())) {
        return true;
    }
    if (ResolveSchemeChangeTargets(userTx, this)) {
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
    TMaybe<TVector<TResolvedSchemeChangeTarget>> targets = ResolveSchemeChangeTargets(userTx, this);

    // Redact every (Ydb.sensitive) field before persisting: passwords, access
    // keys, and secret values must never reach the outbox or subscribers --
    // unless the operator has explicitly disabled this via config.
    NKikimrSchemeOp::TModifyScheme redacted = userTx;
    TVector<TString> redactedFields;
    if (RedactSchemeChangeSensitiveFields) {
        ClearSensitiveFields(&redacted, redactedFields);
    }

    TString body;
    {
        bool ok = redacted.SerializeToString(&body);
        Y_DEBUG_ABORT_UNLESS(ok);
    }

    const ui64 order = AllocateSchemeChangeOrderInMemory();

    TestSchemeChangeRedoBytesAccum += body.size() + 128;

    // Relative targets (persisted as Path) and absolute targets (carried in
    // memory / table 144; finalisation re-resolves via TPath::Resolve, which
    // requires the absolute form).
    TVector<TOperation::TSchemeChangeTarget> relTargets;
    TVector<TOperation::TSchemeChangeTarget> absTargets;
    if (targets) {
        for (const auto& t : *targets) {
            relTargets.push_back(TOperation::TSchemeChangeTarget{t.Relative, t.RelativeSources});
            absTargets.push_back(TOperation::TSchemeChangeTarget{t.Absolute, t.RelativeSources});
        }
    }

    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(order).Update(
        NIceDb::TUpdate<T::TxId>(ui64(txId)),
        NIceDb::TUpdate<T::OperationType>(static_cast<ui32>(userTx.GetOperationType())),
        NIceDb::TUpdate<T::Path>(EncodeSchemeChangeTargets(relTargets)),
        NIceDb::TUpdate<T::Status>(ui32(NKikimrScheme::StatusAccepted)),
        NIceDb::TUpdate<T::UserSID>(userSid),
        NIceDb::TUpdate<T::BodySizeBytes>(body.size()),
        NIceDb::TUpdate<T::RedactedFields>(JoinSeq("\n", redactedFields)),
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
    PersistSchemeChangePendingOrder(db, txId, userTxIdx, order, absTargets);

    slot.UserTxIdx = userTxIdx;
    slot.Order = order;
    slot.Targets = absTargets;
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

    // Must be captured now: after a DROP the name is gone. Identity/schema
    // fields describe a single object, so they are captured from the first
    // target only; a multi-target op's other targets are still canonicalized
    // below, just without their own PathId/ObjectType/SchemaVersion.
    TPathId resolvedPathId;
    auto resolvedObjectType = NKikimrSchemeOp::EPathTypeInvalid;
    ui64 schemaVersion = 0;
    // Canonical resolved targets, database-relative Path (SourcePath is
    // carried through unchanged, never re-resolved); only replaced on
    // successful resolution. A drop keeps the propose-time value written by
    // PersistSchemeChangeRecordAtPropose, since the object is already gone
    // by the time this runs.
    TVector<TOperation::TSchemeChangeTarget> canonicalTargets = slot.Targets;
    bool anyResolved = false;
    for (size_t i = 0; i < slot.Targets.size(); ++i) {
        if (slot.Targets[i].Path.empty()) {
            continue;
        }
        TPath resolved = TPath::Resolve(slot.Targets[i].Path, this);
        if (!resolved.IsResolved()) {
            continue;
        }
        canonicalTargets[i].Path = RelativeToDomain(resolved);
        anyResolved = true;
        if (i == 0) {
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
    // Overwrite the propose-time synthesis with the canonical resolved
    // targets. Only on success: a drop's target is already gone, so the
    // propose-time value (still database-relative, already correct) is left
    // in place.
    if (anyResolved) {
        db.Table<T>().Key(slot.Order).Update(
            NIceDb::TUpdate<T::Path>(EncodeSchemeChangeTargets(canonicalTargets))
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
        NIceDb::TUpdate<T::Path>(entry.Path.empty() ? TString()
            : EncodeSchemeChangeTargets({TOperation::TSchemeChangeTarget{entry.Path, {}}})),
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
