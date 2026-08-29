#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/protos/schemeshard/scheme_change_records.pb.h>

#include <util/string/join.h>

namespace NKikimr::NSchemeShard {

namespace {

struct TSchemeChangeRawTarget {
    TString AbsDstPath;
    TString AbsSrcPath;
};

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
        // TMoveIndex Src/DstPath are relative to TablePath, not absolute
        // (index/operation_move_index.cpp: mainTablePath.Child).
        const auto& moveIndex = tx.GetMoveIndex();
        return TSchemeChangeRawTarget{
            TStringBuilder() << moveIndex.GetTablePath() << '/' << moveIndex.GetDstPath(),
            TStringBuilder() << moveIndex.GetTablePath() << '/' << moveIndex.GetSrcPath()};
    }
    return Nothing();
}

TVector<TSchemeChangeRawTarget> ExtractSchemeChangeCopyTargets(const NKikimrSchemeOp::TModifyScheme& tx) {
    TVector<TSchemeChangeRawTarget> result;
    if (tx.HasCreateConsistentCopyTables()) {
        for (const auto& copy : tx.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
            result.push_back(TSchemeChangeRawTarget{copy.GetDstPath(), copy.GetSrcPath()});
        }
    }
    return result;
}

// Found via reflection so a newly added object type needs no switch entry.
TString ExtractSchemeChangeTargetName(const NKikimrSchemeOp::TModifyScheme& tx) {
    if (tx.HasCreateIndexedTable()) {
        return tx.GetCreateIndexedTable().GetTableDescription().GetName();
    }
    // Only the standalone alter sets PathName. Create-with-attrs carries the same
    // message with PathName unset, and its target is named by its own payload.
    if (tx.HasAlterUserAttributes() && !tx.GetAlterUserAttributes().GetPathName().empty()) {
        return tx.GetAlterUserAttributes().GetPathName();
    }
    // These name their target TableName, not Name. Restore's and Backup's AbortPropose
    // are unconditional Y_ABORT stubs: an unresolved path crashes the tablet.
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
    if (tx.HasBackup()) {
        return tx.GetBackup().GetTableName();
    }
    if (tx.HasInitiateBuildIndexMainTable()) {
        return tx.GetInitiateBuildIndexMainTable().GetTableName();
    }
    if (tx.HasFinalizeBuildIndexMainTable()) {
        return tx.GetFinalizeBuildIndexMainTable().GetTableName();
    }
    if (tx.HasDropIndex()) {
        return TStringBuilder() << tx.GetDropIndex().GetTableName() << '/' << tx.GetDropIndex().GetIndexName();
    }
    // WorkingDir is already the index path, so the impl table name completes it.
    if (tx.HasPrepareIndexValidation()) {
        return tx.GetPrepareIndexValidation().GetTableName();
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

// Anchored on TPath::Root, not GetDomainPathString: a new subdomain is already its own domain root.
TString RelativeToDomain(const TPath& resolved, TSchemeShard* ss) {
    TString abs = resolved.PathString();
    TString domain = TPath::Root(ss).PathString();
    Y_DEBUG_ABORT_UNLESS(abs.StartsWith(domain));
    TString rel = abs.substr(domain.size());
    if (!rel.empty() && rel[0] == '/') {
        rel = rel.substr(1);
    }
    return rel;
}

struct TResolvedSchemeChangeTarget {
    TString Absolute;
    TString Relative;
    TVector<TString> RelativeSources;
};

TString RelativeSourceToDomain(const TString& absSrcPath, TSchemeShard* ss) {
    if (absSrcPath.empty()) {
        return {};
    }
    TPath src = TPath::Resolve(absSrcPath, ss);
    if (src.IsResolved()) {
        return RelativeToDomain(src, ss);
    }
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

TMaybe<TResolvedSchemeChangeTarget> ResolveRawTarget(const TSchemeChangeRawTarget& raw, TSchemeShard* ss) {
    TVector<TString> relSources;
    if (!raw.AbsSrcPath.empty()) {
        relSources.push_back(RelativeSourceToDomain(raw.AbsSrcPath, ss));
    }
    TPath target = TPath::Resolve(raw.AbsDstPath, ss);
    if (target.IsResolved()) {
        return TResolvedSchemeChangeTarget{raw.AbsDstPath, RelativeToDomain(target, ss), relSources};
    }
    TPath parent = TPath::Resolve(TString(TStringBuf(raw.AbsDstPath).RBefore('/')), ss);
    if (parent.IsResolved()) {
        TString relParent = RelativeToDomain(parent, ss);
        if (!relParent.empty()) {
            relParent += '/';
        }
        TString leaf = TString(TStringBuf(raw.AbsDstPath).RAfter('/'));
        return TResolvedSchemeChangeTarget{raw.AbsDstPath, relParent + leaf, relSources};
    }
    return Nothing();
}

// Deliberately not dispatched on op type: ConvertToTxType collapses many EOperationType onto TxInvalid.
// Changefeed ops name their target <TableName>/<StreamName> under WorkingDir -- two
// levels down, so neither a direct-child lookup nor the reflection walk finds it.
// TDropCdcStream.StreamName is repeated: one op can drop N changefeeds.
TVector<TSchemeChangeRawTarget> ExtractSchemeChangeCdcTargets(const NKikimrSchemeOp::TModifyScheme& tx) {
    TVector<TSchemeChangeRawTarget> result;
    auto stream = [&tx](const TString& table, const TString& name) {
        return TString(TStringBuilder() << tx.GetWorkingDir() << '/' << table << '/' << name);
    };
    if (tx.HasCreateCdcStream()) {
        const auto& op = tx.GetCreateCdcStream();
        result.push_back(TSchemeChangeRawTarget{
            stream(op.GetTableName(), op.GetStreamDescription().GetName()), {}});
    } else if (tx.HasAlterCdcStream()) {
        const auto& op = tx.GetAlterCdcStream();
        result.push_back(TSchemeChangeRawTarget{
            stream(op.GetTableName(), op.GetStreamName()), {}});
    } else if (tx.HasDropCdcStream()) {
        const auto& op = tx.GetDropCdcStream();
        for (const auto& name : op.GetStreamName()) {
            result.push_back(TSchemeChangeRawTarget{stream(op.GetTableName(), name), {}});
        }
    } else if (tx.HasRotateCdcStream()) {
        // The new stream is the identity afterwards; the retired one is its source.
        const auto& op = tx.GetRotateCdcStream();
        result.push_back(TSchemeChangeRawTarget{
            stream(op.GetTableName(), op.GetNewStream().GetStreamDescription().GetName()),
            stream(op.GetTableName(), op.GetOldStreamName())});
    }
    return result;
}

TMaybe<TVector<TResolvedSchemeChangeTarget>> ResolveSchemeChangeTargets(const NKikimrSchemeOp::TModifyScheme& requestTx, TSchemeShard* ss) {
    const TVector<TSchemeChangeRawTarget> copyTargets = ExtractSchemeChangeCopyTargets(requestTx);
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

    const TVector<TSchemeChangeRawTarget> cdcTargets = ExtractSchemeChangeCdcTargets(requestTx);
    if (!cdcTargets.empty()) {
        TVector<TResolvedSchemeChangeTarget> result;
        for (const auto& raw : cdcTargets) {
            TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(raw, ss);
            if (!resolved) {
                return Nothing();
            }
            result.push_back(std::move(*resolved));
        }
        return result;
    }

    TMaybe<TSchemeChangeRawTarget> moveTarget = ExtractSchemeChangeMoveTarget(requestTx);
    if (moveTarget) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(*moveTarget, ss);
        if (!resolved) {
            return Nothing();
        }
        return TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)};
    }

    if (requestTx.HasAlterLogin()) {
        TPath workingDir = TPath::Resolve(requestTx.GetWorkingDir(), ss);
        if (workingDir.IsResolved()) {
            return TVector<TResolvedSchemeChangeTarget>{
                TResolvedSchemeChangeTarget{requestTx.GetWorkingDir(), RelativeToDomain(workingDir, ss), {}}};
        }
        return Nothing();
    }

    if (requestTx.HasInitiateIndexBuild()) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(
            TSchemeChangeRawTarget{requestTx.GetInitiateIndexBuild().GetTable(), {}}, ss);
        return resolved
            ? TMaybe<TVector<TResolvedSchemeChangeTarget>>{TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)}}
            : Nothing();
    }
    // Column builds carry an absolute table path, like InitiateIndexBuild.
    if (requestTx.HasInitiateColumnBuild()) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(
            TSchemeChangeRawTarget{requestTx.GetInitiateColumnBuild().GetTable(), {}}, ss);
        return resolved
            ? TMaybe<TVector<TResolvedSchemeChangeTarget>>{TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)}}
            : Nothing();
    }
    if (requestTx.HasDropColumnBuild()) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(
            TSchemeChangeRawTarget{requestTx.GetDropColumnBuild().GetSettings().GetTable(), {}}, ss);
        return resolved
            ? TMaybe<TVector<TResolvedSchemeChangeTarget>>{TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)}}
            : Nothing();
    }
    if (requestTx.HasCancelIndexBuild()) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(
            TSchemeChangeRawTarget{requestTx.GetCancelIndexBuild().GetTablePath(), {}}, ss);
        return resolved
            ? TMaybe<TVector<TResolvedSchemeChangeTarget>>{TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)}}
            : Nothing();
    }
    if (requestTx.HasApplyIndexBuild()) {
        TMaybe<TResolvedSchemeChangeTarget> resolved = ResolveRawTarget(
            TSchemeChangeRawTarget{requestTx.GetApplyIndexBuild().GetTablePath(), {}}, ss);
        return resolved
            ? TMaybe<TVector<TResolvedSchemeChangeTarget>>{TVector<TResolvedSchemeChangeTarget>{std::move(*resolved)}}
            : Nothing();
    }

    const TString targetName = ExtractSchemeChangeTargetName(requestTx);
    if (targetName.empty()) {
        // Drop and AlterTable may name their target by path id instead, and then
        // WorkingDir is not a path at all ("not used").
        TPathId byId;
        if (requestTx.HasDrop() && requestTx.GetDrop().GetId() != 0) {
            byId = ss->MakeLocalId(requestTx.GetDrop().GetId());
        } else if (requestTx.HasAlterTable()) {
            const auto& alter = requestTx.GetAlterTable();
            if (alter.HasPathId()) {
                byId = TPathId::FromProto(alter.GetPathId());
            } else if (alter.HasId_Deprecated()) {
                byId = ss->MakeLocalId(alter.GetId_Deprecated());
            }
        }
        if (byId) {
            TPath target = TPath::Init(byId, ss);
            if (target.IsResolved()) {
                return TVector<TResolvedSchemeChangeTarget>{
                    TResolvedSchemeChangeTarget{target.PathString(), RelativeToDomain(target, ss), {}}};
            }
        }
        return Nothing();
    }

    TString abs = requestTx.GetWorkingDir();
    if (abs.empty() || abs.back() != '/') abs += '/';
    abs += targetName;

    TPath target = TPath::Resolve(abs, ss);
    if (target.IsResolved()) {
        return TVector<TResolvedSchemeChangeTarget>{TResolvedSchemeChangeTarget{abs, RelativeToDomain(target, ss), {}}};
    }

    TPath workingDir = TPath::Resolve(requestTx.GetWorkingDir(), ss);
    if (!workingDir.IsResolved()) {
        return Nothing();
    }
    TString relParent = RelativeToDomain(workingDir, ss);
    if (!relParent.empty()) {
        relParent += '/';
    }
    return TVector<TResolvedSchemeChangeTarget>{TResolvedSchemeChangeTarget{abs, relParent + targetName, {}}};
}

} // namespace

// Serialized protobuf, never a delimited string: a path may contain any delimiter.
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

bool TSchemeShard::CheckSchemeChangeRecordHasPath(const NKikimrSchemeOp::TModifyScheme& requestTx, TString& rejectReason) {
    if (IsChurnOp(requestTx.GetOperationType()) || IsPathlessOp(requestTx.GetOperationType())) {
        return true;
    }
    if (ResolveSchemeChangeTargets(requestTx, this)) {
        return true;
    }
    rejectReason = TStringBuilder() << "scheme change outbox could not resolve a path for operation type "
        << NKikimrSchemeOp::EOperationType_Name(requestTx.GetOperationType());
    return false;
}

bool TSchemeShard::PersistSchemeChangeRecordAtPropose(NIceDb::TNiceDb& db, TTxId txId, ui32 requestIdx,
        const NKikimrSchemeOp::TModifyScheme& requestTx, TOperation::TSchemeChangeSlot& slot,
        const TString& userSid) {
    if (IsChurnOp(requestTx.GetOperationType())) {
        return false;
    }

    TMaybe<TVector<TResolvedSchemeChangeTarget>> targets = ResolveSchemeChangeTargets(requestTx, this);

    // Redact every (Ydb.sensitive) field before persisting: passwords, access keys
    // and secret values must never reach subscribers unless config disables this.
    NKikimrSchemeOp::TModifyScheme redacted = requestTx;
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
        NIceDb::TUpdate<T::OperationType>(static_cast<ui32>(requestTx.GetOperationType())),
        NIceDb::TUpdate<T::Path>(EncodeSchemeChangeTargets(relTargets)),
        NIceDb::TUpdate<T::Status>(ui32(NKikimrScheme::StatusAccepted)),
        NIceDb::TUpdate<T::UserSID>(userSid),
        NIceDb::TUpdate<T::BodySizeBytes>(body.size()),
        NIceDb::TUpdate<T::RedactedFields>(JoinSeq("\n", redactedFields)),
        // Zero keeps the row hidden from fetch until finalisation.
        NIceDb::TUpdate<T::CompletedAtUs>(ui64(0))
    );
    if (!body.empty()) {
        db.Table<Schema::SchemeChangeRecordDetails>().Key(order).Update(
            NIceDb::TUpdate<Schema::SchemeChangeRecordDetails::Body>(body)
        );
    }

    PersistUpdateNextSchemeChangeOrder(db);
    PersistSchemeChangePendingOrder(db, txId, requestIdx, order, absTargets);

    slot.RequestIdx = requestIdx;
    slot.Order = order;
    slot.Targets = absTargets;
    slot.UserSid = userSid;
    return true;
}

void TSchemeShard::FinalizeSchemeChangeRecord(NIceDb::TNiceDb& db, const TActorContext& ctx,
        const TOperation::TSchemeChangeSlot& slot, TStepId planStep, bool aborted) {
    // Ops completing at propose have no coordinator step, so they borrow the ceiling.
    ui64 step = ui64(planStep);
    auto positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT;
    if (step == 0 || planStep == InvalidStepId) {
        step = LastAssignedPlanStep;
        positionKind = NKikimrSchemeShard::TSchemeChangePosition::KIND_BUCKETED;
    }
    step = Max<ui64>(step, 1);

    TPathId resolvedPathId;
    auto resolvedObjectType = NKikimrSchemeOp::EPathTypeInvalid;
    ui64 schemaVersion = 0;
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
        canonicalTargets[i].Path = RelativeToDomain(resolved, this);
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
        // Schema only: partitioning/children would bloat the redo log.
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

    // CompletedAtUs non-zero makes the row visible, so all fields must be set in this Update.
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
    if (anyResolved) {
        db.Table<T>().Key(slot.Order).Update(
            NIceDb::TUpdate<T::Path>(EncodeSchemeChangeTargets(canonicalTargets))
        );
    }
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

bool TSchemeShard::DeleteAckedSchemeChangeRecords(NIceDb::TNiceDb& db, ui64 newMinOrder,
        ui64 limit, bool& hasMore) {
    hasMore = false;
    // SchemeChangeFloorOrder is the only sound lower bound: it is what was actually
    // deleted, not a watermark a prior pass may have stopped short of.
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
