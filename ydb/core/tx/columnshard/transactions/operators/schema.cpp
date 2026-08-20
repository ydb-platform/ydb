#include "schema.h"

#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/subscriber/abstract/subscriber/subscriber.h>
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/tx_completed/event.h>
#include <ydb/core/tx/columnshard/transactions/transactions/tx_finish_async.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#include <util/stream/output.h>
#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

class TWaitOnProposeTxSubscriberBase: public NSubscriber::ISubscriber {
    const ui64 TxId;

protected:
    TWaitOnProposeTxSubscriberBase(const ui64 txId)
        : TxId(txId)
    {
    }

    void OnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard& shard) {
        const YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD_WRITE,
                  {"event", "on_subscriber_event"},
                  {"txId", TxId},
                  {"ev", ev->DebugString()});
        AFL_VERIFY(!IsFinished());
        DoOnEvent(ev);
        if (IsFinished()) {
            shard.Execute(new TTxFinishAsyncTransaction(shard, TxId));
        }
    }

protected:
    virtual void DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) = 0;
};

class TWaitEraseTablesTxSubscriber: public TWaitOnProposeTxSubscriberBase {
private:
    THashSet<TInternalPathId> WaitTables;

public:
    TWaitEraseTablesTxSubscriber(const ui64 txId, THashSet<TInternalPathId>&& waitTables)
        : TWaitOnProposeTxSubscriberBase(txId)
        , WaitTables(std::move(waitTables))
    {
    }

    virtual std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::TablesErased };
    }

    virtual void DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::TablesErased);
        auto* evErased = static_cast<const NSubscriber::TEventTablesErased*>(ev.get());
        for (auto&& i : evErased->GetPathIds()) {
            WaitTables.erase(i);
        }
        YDB_LOG_NOTICE("",
            {"remained", JoinSeq(",", WaitTables)});
    }

    virtual bool IsFinished() const override {
        return WaitTables.empty();
    }
};

class TWaitTxs: public TWaitOnProposeTxSubscriberBase {
    THashSet<ui64> TxIdsToWait;

public:
    TWaitTxs(const ui64 txId, const THashSet<ui64>&& txIdsToWait)
        : TWaitOnProposeTxSubscriberBase(txId)
        , TxIdsToWait(std::move(txIdsToWait))
    {
    }

    std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::TxCompleted };
    }

    bool IsFinished() const override {
        return TxIdsToWait.empty();
    }

    virtual void DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::TxCompleted);
        const auto* evCompleted = static_cast<const NSubscriber::TEventTxCompleted*>(ev.get());
        // Subscribers receive every TxCompleted on the shard. Wait set is a snapshot from propose
        // time (GetTxs), so later/unrelated completions must be ignored — otherwise VERIFY fails
        if (!TxIdsToWait.erase(evCompleted->GetTxId())) {
            return;
        }
        YDB_LOG_DEBUG("",
            {"completed", evCompleted->GetTxId()},
            {"remained", JoinSeq(",", TxIdsToWait)});
    }
};

TTxController::TProposeResult TSchemaTransactionOperator::DoStartProposeOnExecute(
    TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(!WaitOnPropose);
    auto seqNo = SeqNoFromProto(SchemaTxBody.GetSeqNo());
    auto lastSeqNo = owner.LastSchemaSeqNo;

    // Independent seq no for path-specific schema ops (Drop / Copy / Truncate)
    std::optional<ui64> targetPathId;
    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            targetPathId = TSchemeShardLocalPathId::FromProto(SchemaTxBody.GetDropTable()).GetRawValue();
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kCopyTable:
            targetPathId = SchemaTxBody.GetCopyTable().GetDstPathId();
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable:
            targetPathId = SchemaTxBody.GetMoveTable().GetDstPathId();
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kTruncateTable:
            targetPathId = SchemaTxBody.GetTruncateTable().GetPathId();
            break;
        default:
            break;
    }

    if (targetPathId) {
        // For path-specific operations, check SeqNo against the per-path SeqNo
        TSchemeShardLocalPathId targetPathIdObj = TSchemeShardLocalPathId::FromRawValue(*targetPathId);
        auto pathSeqNoIt = owner.LastSchemaSeqNoByPath.find(targetPathIdObj);
        if (pathSeqNoIt != owner.LastSchemaSeqNoByPath.end() && seqNo < pathSeqNoIt->second) {
            auto errorMessage = TStringBuilder() << "Ignoring outdated schema tx proposal at tablet " << owner.TabletID() << " txId "
                                                 << GetTxId() << " ssId " << owner.CurrentSchemeShardId << " seqNo " << seqNo << " lastSeqNo "
                                                 << pathSeqNoIt->second << " pathId " << *targetPathId;
            return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_CHANGED, errorMessage);
        }
    } else {
        // For shard-wide operations, use the global SeqNo check
        if (seqNo < lastSeqNo) {
            auto errorMessage = TStringBuilder() << "Ignoring outdated schema tx proposal at tablet " << owner.TabletID() << " txId "
                                                 << GetTxId() << " ssId " << owner.CurrentSchemeShardId << " seqNo " << seqNo << " lastSeqNo "
                                                 << lastSeqNo;
            return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_CHANGED, errorMessage);
        }
    }

    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard: {
            if (owner.InitShardCounter.Add(1) != 1) {
                YDB_LOG_WARN("",
                    {"event", "repeated_initialization"},
                    {"txId", GetTxId()},
                    {"counter", owner.InitShardCounter.Val()});
            }
            auto validationStatus = ValidateTables(SchemaTxBody.GetInitShard().GetTables());
            if (validationStatus.IsFail()) {
                return TProposeResult(
                    NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            auto pathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetInitShard().GetTables());
            if (!pathIdsToErase.empty()) {
                WaitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(GetTxId(), std::move(pathIdsToErase));
            }
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables: {
            const auto& tables = SchemaTxBody.GetEnsureTables().GetTables();
            auto validationStatus = ValidateTables(tables);
            if (validationStatus.IsFail()) {
                return TProposeResult(
                    NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Invalid schema: " + validationStatus.GetErrorMessage());
            }
            auto waitPathIdsToErase = GetNotErasedTableIds(owner, SchemaTxBody.GetEnsureTables().GetTables());
            if (!waitPathIdsToErase.empty()) {
                WaitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(GetTxId(), std::move(waitPathIdsToErase));
            }
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable: {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(SchemaTxBody.GetMoveTable());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetDstPathId());
            YDB_LOG_INFO("",
                {"proposeExecute", "move_table"},
                {"src", srcSchemeShardLocalPathId},
                {"dst", dstSchemeShardLocalPathId});
            const auto srcInternalPathId = owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false);
            if (!srcInternalPathId) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "No such table");
            }
            if (owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Rename to existing table");
            }
            if (auto tableTtl = owner.TablesManager.GetTableTtl(*srcInternalPathId)) {
                if (!tableTtl->GetUsedTiers().empty()) {
                    return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Cannot move a table that has tiering configured");
                }
            }
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();   //TODO #8650 Get transaction for moving pathId only
            if (!txIdsToWait.empty()) {
                AFL_VERIFY(!txIdsToWait.contains(GetTxId()))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIdsToWait));
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
            owner.TablesManager.MoveTablePropose(srcSchemeShardLocalPathId);
            break;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kCopyTable: {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(SchemaTxBody.GetCopyTable());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetDstPathId());
            YDB_LOG_INFO("",
                {"proposeExecute", "copy_table"},
                {"src", srcSchemeShardLocalPathId},
                {"dst", dstSchemeShardLocalPathId});
            if (!owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "No such table");
            }
            if (owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false)) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "Copy to existing table");
            }
            // CopyTable is a read-only metadata operation that creates a new path pointing to the
            // same data. Unlike MoveTable, it does not modify the source path mapping, so it does
            // not conflict with existing transactions and should not wait for them. Waiting for all
            // txs via GetTxs() caused hangs when other long-running transactions existed on the shard
            // (e.g., during export when backup txs are pending on the same shards).
            owner.TablesManager.CopyTablePropose(srcSchemeShardLocalPathId);
            break;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kTruncateTable: {
            // TRUNCATE requires GenerateInternalPathId because it allocates a new InternalPathId
            // for the truncated table (the old generation is dropped, the new one is registered).
            if (!owner.TablesManager.IsGenerateInternalPathId()) {
                return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR,
                    "Cannot truncate column table without GenerateInternalPathId");
            }
            // TRUNCATE is only supported for standalone column tables.
            // Tables in a column store share a tablet and cannot be truncated independently.
            if (owner.TablesManager.IsStoreTablet()) {
                return TProposeResult(
                    NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR, "TRUNCATE is not supported for tables in a column store");
            }
            const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(SchemaTxBody.GetTruncateTable());
            if (const auto internalPathId = owner.TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false)) {
                if (owner.TablesManager.HasTable(*internalPathId)) {
                    const auto& table = owner.TablesManager.GetTable(*internalPathId);
                    // Check 1: Read-only tables (created via CopyTable) cannot be truncated.
                    // The IsReadOnly flag is set per SchemeShardLocalPathId when CopyTable registers
                    // the destination path. Only the copy (destination) is marked read-only.
                    if (table.IsReadOnly(schemeShardLocalPathId)) {
                        return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR,
                            TStringBuilder() << "Cannot truncate read-only table " << schemeShardLocalPathId);
                    }
                    // Check 3: Tables with tiering cannot be truncated (tiering migration state
                    // would be lost on the new InternalPathId). Pure TTL (delete action) is fine.
                    if (const auto ttl = owner.TablesManager.GetTableTtl(*internalPathId)) {
                        if (!ttl->GetUsedTiers().empty()) {
                            return TProposeResult(NKikimrTxColumnShard::EResultStatus::SCHEMA_ERROR,
                                "Cannot truncate column table with tiering");
                        }
                    }
                }
            }
            // Fence the path like MoveTablePropose: new EvWrites and CommitWriteLock fail with
            // "unknown table" until plan applies the generation swap. Without this, a write that
            // resolved the old InternalPathId before PREPARED could commit into PathsToDrop.
            if (owner.TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false)) {
                owner.TablesManager.TruncateTablePropose(schemeShardLocalPathId);
            }
            // TODO #8650: Optimize to get only transactions for the truncated pathId instead of
            // all in-flight transactions. Currently waits for all txs, which can be slow when
            // unrelated long-running transactions (e.g., backup/export) are pending.
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();
            if (!txIdsToWait.empty()) {
                AFL_VERIFY(!txIdsToWait.contains(GetTxId()))("tx_id", GetTxId())("tx_ids", JoinSeq(",", txIdsToWait));
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (WaitOnPropose) {
        owner.Subscribers->RegisterSubscriber(WaitOnPropose);
    }

    owner.UpdateSchemaSeqNo(seqNo, txc);
    // Update per-path SeqNo for path-specific operations
    if (targetPathId) {
        TSchemeShardLocalPathId targetPathIdObj = TSchemeShardLocalPathId::FromRawValue(*targetPathId);
        owner.LastSchemaSeqNoByPath[targetPathIdObj] = seqNo;
    }
    return TProposeResult();
}

NKikimr::TConclusionStatus TSchemaTransactionOperator::ValidateTableSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) const {
    namespace NTypeIds = NScheme::NTypeIds;
    static const THashSet<NScheme::TTypeId> pkSupportedTypes = { NTypeIds::Bool, NTypeIds::Timestamp, NTypeIds::Date32, NTypeIds::Datetime64,
        NTypeIds::Timestamp64, NTypeIds::Interval64, NTypeIds::Interval, NTypeIds::Int8, NTypeIds::Int16, NTypeIds::Int32, NTypeIds::Int64,
        NTypeIds::Uint8, NTypeIds::Uint16, NTypeIds::Uint32, NTypeIds::Uint64, NTypeIds::Date, NTypeIds::Datetime,
        //NTypeIds::Float,
        //NTypeIds::Double,
        NTypeIds::String, NTypeIds::Utf8, NTypeIds::Decimal, NTypeIds::DyNumber, NTypeIds::Uuid };

    if (!schema.KeyColumnNamesSize()) {
        return TConclusionStatus::Fail("There is no key columns");
    }

    THashSet<TString> keyColumns(schema.GetKeyColumnNames().begin(), schema.GetKeyColumnNames().end());
    TVector<TString> columnErrors;
    for (const NKikimrSchemeOp::TOlapColumnDescription& column : schema.GetColumns()) {
        TString name = column.GetName();
        NScheme::TTypeId typeId = column.GetTypeId();
        NScheme::TTypeInfo schemeType;
        if (column.HasTypeInfo()) {
            schemeType = NScheme::TypeInfoFromProto(typeId, column.GetTypeInfo());
        } else {
            schemeType = typeId;
        }

        if (keyColumns.contains(name) && !pkSupportedTypes.contains(column.GetTypeId())) {
            columnErrors.emplace_back("key column " + name + " has unsupported type " + column.GetTypeName());
        }
        auto arrowType = NArrow::GetArrowType(schemeType);
        if (!arrowType.ok()) {
            columnErrors.emplace_back("column " + name + ": " + arrowType.status().ToString());
        }
        keyColumns.erase(name);
    }
    if (!columnErrors.empty()) {
        return TConclusionStatus::Fail("Column errors: " + JoinSeq("; ", columnErrors));
    }

    if (!keyColumns.empty()) {
        return TConclusionStatus::Fail("Key columns not in scheme: " + JoinSeq(", ", keyColumns));
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TSchemaTransactionOperator::ValidateTables(
    ::google::protobuf::RepeatedPtrField<::NKikimrTxColumnShard::TCreateTable> tables) const {
    for (auto& table : tables) {
        if (table.HasSchemaPreset()) {
            const auto validationStatus = ValidateTablePreset(table.GetSchemaPreset());
            if (validationStatus.IsFail()) {
                return validationStatus;
            }
        }
        if (table.HasSchema()) {
            const auto validationStatus = ValidateTableSchema(table.GetSchema());
            if (validationStatus.IsFail()) {
                return validationStatus;
            }
        }
    }
    return TConclusionStatus::Success();
}

void TSchemaTransactionOperator::DoOnTabletInit(TColumnShard& owner) {
    AFL_VERIFY(!WaitOnPropose);
    switch (SchemaTxBody.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables: {
            THashSet<TInternalPathId> waitPathIdsToErase;
            for (auto&& i : SchemaTxBody.GetEnsureTables().GetTables()) {
                const auto& schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(i);
                if (const auto internalPathId = owner.TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false)) {
                    if (owner.TablesManager.HasTable(*internalPathId, true)) {
                        waitPathIdsToErase.emplace(*internalPathId);
                    }
                }
            }
            if (!waitPathIdsToErase.empty()) {
                WaitOnPropose = std::make_shared<TWaitEraseTablesTxSubscriber>(GetTxId(), std::move(waitPathIdsToErase));
            }
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable: {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetSrcPathId());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetMoveTable().GetDstPathId());

            AFL_VERIFY(owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false));
            AFL_VERIFY(!owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false));
            owner.TablesManager.MoveTablePropose(srcSchemeShardLocalPathId);
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();
            AFL_VERIFY(txIdsToWait.erase(GetTxId()));
            if (!txIdsToWait.empty()) {
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::kCopyTable: {
            const auto srcSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetSrcPathId());
            const auto dstSchemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(SchemaTxBody.GetCopyTable().GetDstPathId());
            const auto srcInternalPathId = owner.TablesManager.ResolveInternalPathId(srcSchemeShardLocalPathId, false);
            AFL_VERIFY(srcInternalPathId);
            // CopyTablePlanStep persists dst in TableInfoV1 before progress completes. After tablet restart
            // dst is already in GenerationIndex.Live, so replay must be idempotent (same as CopyTableProgress).
            if (const auto dstInternalPathId = owner.TablesManager.ResolveInternalPathId(dstSchemeShardLocalPathId, false)) {
                AFL_VERIFY(*dstInternalPathId == *srcInternalPathId)("src", *srcInternalPathId)("dst", *dstInternalPathId);
            }
            owner.TablesManager.CopyTablePropose(srcSchemeShardLocalPathId);
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::kTruncateTable: {
            AFL_VERIFY(owner.TablesManager.IsGenerateInternalPathId())("error", "truncate requires GenerateInternalPathId");
            if (owner.TablesManager.IsStoreTablet()) {
                break;
            }
            const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(SchemaTxBody.GetTruncateTable());
            // After restart Truncating fence is empty and GenerationIndex.Live is
            // rebuilt from DB. Re-fence the path (same as MoveTablePropose replay) so writes stay
            // blocked while TRUNCATE is still pending.
            if (const auto internalPathId = owner.TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false)) {
                if (owner.TablesManager.HasTable(*internalPathId)) {
                    const auto& table = owner.TablesManager.GetTable(*internalPathId);
                    // Propose rejects these; on restart skip re-fence / wait setup.
                    if (table.IsReadOnly(schemeShardLocalPathId)) {
                        break;
                    }
                    if (const auto ttl = owner.TablesManager.GetTableTtl(*internalPathId); ttl && !ttl->GetUsedTiers().empty()) {
                        break;
                    }
                }
                owner.TablesManager.TruncateTablePropose(schemeShardLocalPathId);
            }
            auto txIdsToWait = owner.GetProgressTxController().GetTxs();
            AFL_VERIFY(txIdsToWait.erase(GetTxId()));
            if (!txIdsToWait.empty()) {
                WaitOnPropose = std::make_shared<TWaitTxs>(GetTxId(), std::move(txIdsToWait));
            }
        } break;
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    if (WaitOnPropose) {
        YDB_LOG_WARN("",
            {"event", "wait_on_propose"},
            {"txId", GetTxId()});
        owner.Subscribers->RegisterSubscriber(WaitOnPropose);
    }   // else we need to wait for SS resend
}

void TSchemaTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) {
}

}   //namespace NKikimr::NColumnShard
