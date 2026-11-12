#include "datashard_impl.h"
#include "datashard_integrity_trails.h"
#include "datashard_tli.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"
#include "const.h"
#include "setup_sys_locks.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/engine/minikql/minikql_engine_host_counters.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/backend/catalog/pg_type_d.h>

#include <util/string/escape.h>
#include <util/system/byteorder.h>

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

namespace {

    template<bool BE, typename T>
    static bool CellToInt64(const NKikimr::TCell& cell, i64& value) {
        TString err;
        T typed = 0;
        if (!cell.ToValue<T>(typed, err)) {
            return false;
        }
        if constexpr(BE) {
            value = (i64)InetToHost(typed);
        } else {
            value = (i64)typed;
        }
        return true;
    }

    // Sequence defaults only support integer columns, and replication applies typed cells.
    bool TryGetSequenceValue(const NScheme::TTypeInfo& type, const NKikimr::TCell& cell, i64& value) {
        if (cell.IsNull()) {
            return false;
        }

        switch (type.GetTypeId()) {
            case NScheme::NTypeIds::Int64:
                return CellToInt64<false, i64>(cell, value);
            case NScheme::NTypeIds::Uint64:
                return CellToInt64<false, ui64>(cell, value);
            case NScheme::NTypeIds::Int32:
                return CellToInt64<false, i32>(cell, value);
            case NScheme::NTypeIds::Uint32:
                return CellToInt64<false, ui32>(cell, value);
            case NScheme::NTypeIds::Int16:
                return CellToInt64<false, i16>(cell, value);
            case NScheme::NTypeIds::Uint16:
                return CellToInt64<false, ui16>(cell, value);
            case NScheme::NTypeIds::Pg: {
                // A PostgreSQL serial column is a pg integer with a native sequence behind it.
                // Fixed-width pg values are stored in the network (big-endian) byte order.
                switch (NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc())) {
                    case INT2OID:
                        return CellToInt64<true, i16>(cell, value);
                    case INT4OID:
                        return CellToInt64<true, i32>(cell, value);
                    case INT8OID:
                        return CellToInt64<true, i64>(cell, value);
                    default:
                        return false;
                }
            }
            default:
                return false;
        }
    }

    using TSequenceRange = std::pair<i64, i64>; // min, max

    // Collects the range of values applied to every column with a sequence default, together with
    // the absolute path of that sequence. Both the key and the update columns are checked, since a
    // sequence column is often a part of the primary key.
    TVector<std::pair<TString, TSequenceRange>> CollectSequenceAdvances(
            const NKikimrTxDataShard::TEvApplyReplicationChanges& msg, const TUserTable& userTable)
    {
        TVector<std::pair<TString, TSequenceRange>> result;

        if (!userTable.IsReplicated()) {
            return result;
        }

        THashMap<ui32, TString> sequenceColumns;
        for (const auto& [tag, column] : userTable.Columns) {
            if (column.DefaultFromSequence.empty()) {
                continue;
            }
            const TString& reference = column.DefaultFromSequence;
            sequenceColumns.emplace(tag, reference.StartsWith('/')
                ? reference
                : JoinPath({userTable.Path, reference}));
        }

        if (sequenceColumns.empty()) {
            return result;
        }

        THashMap<ui32, TSequenceRange> observed;
        const auto observe = [&observed, &userTable](ui32 tag, i64 value) {
            // __ydb_row_id always uses the spread layout: the query compiler applies it by column
            // name (both when filling the column and in the backfill scan), so the sequence position
            // is the dense part of the value. The column is never created by users, so the layout is
            // hardcoded here instead of being taken from the scheme.
            if (auto it = userTable.Columns.find(tag); it != userTable.Columns.end()
                    && it->second.Name == NTableIndex::NFulltext::RowIdColumn) {
                value = i64(NTableIndex::NFulltext::SeqFromRowId(ui64(value)));
            }

            auto it = observed.find(tag);
            if (it == observed.end()) {
                observed.emplace(tag, TSequenceRange(value, value));
            } else {
                it->second.first = Min(it->second.first, value);
                it->second.second = Max(it->second.second, value);
            }
        };

        for (const auto& change : msg.GetChanges()) {
            TSerializedCellVec keyCells;
            if (!TSerializedCellVec::TryParse(change.GetKey(), keyCells)
                    || keyCells.GetCells().size() != userTable.KeyColumnIds.size())
            {
                // The change is going to be rejected while applying, skip it here.
                continue;
            }

            for (size_t i = 0; i < userTable.KeyColumnIds.size(); ++i) {
                const ui32 tag = userTable.KeyColumnIds[i];
                if (!sequenceColumns.contains(tag)) {
                    continue;
                }
                i64 value = 0;
                if (TryGetSequenceValue(userTable.KeyColumnTypes[i], keyCells.GetCells()[i], value)) {
                    observe(tag, value);
                }
            }

            const NKikimrTxDataShard::TEvApplyReplicationChanges::TUpdates* updates = nullptr;
            switch (change.RowOperation_case()) {
                case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kUpsert:
                    updates = &change.GetUpsert();
                    break;
                case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kReset:
                    updates = &change.GetReset();
                    break;
                default:
                    break;
            }
            if (!updates) {
                continue;
            }

            TSerializedCellVec updateCells;
            const auto& tags = updates->GetTags();
            if (!TSerializedCellVec::TryParse(updates->GetData(), updateCells)
                || updateCells.GetCells().size() != (size_t)tags.size())
            {
                continue;
            }

            for (int i = 0; i < tags.size(); ++i) {
                if (!sequenceColumns.contains(tags[i])) {
                    continue;
                }
                auto column = userTable.Columns.find(tags[i]);
                if (column == userTable.Columns.end()) {
                    continue;
                }
                i64 value = 0;
                if (TryGetSequenceValue(column->second.Type, updateCells.GetCells()[i], value)) {
                    observe(tags[i], value);
                }
            }
        }

        result.reserve(observed.size());
        for (const auto& [tag, range] : observed) {
            auto it = sequenceColumns.find(tag);
            Y_ENSURE(it != sequenceColumns.end());
            result.emplace_back(it->second, range);
        }

        return result;
    }

} // anonymous namespace

class TDataShard::TTxApplyReplicationChanges : public TTransactionBase<TDataShard> {
public:
    explicit TTxApplyReplicationChanges(TDataShard* self, TPipeline& pipeline,
            TEvDataShard::TEvApplyReplicationChanges::TPtr&& ev)
        : TTransactionBase(self)
        , Pipeline(pipeline)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_APPLY_REPLICATION_CHANGES;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Result.Reset();
        MvccVersion.reset();
        Changes.clear();
        CommittingOpRegistered = false;

        TDataShardLocksDb locksDb(*Self, txc);
        TSetupSysLocks guardLocks(*Self, &locksDb);

        if (Self->State != TShardState::Ready || Self->IsDatabaseFrozen()) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_WRONG_STATE,
                TStringBuilder() << "DataShard is not ready");
            return true;
        }

        if (!Self->IsReplicated() && !Self->IsIncrementalRestore()) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Table is nor replicated nor under incremental restore");
            return true;
        }

        const auto& msg = Ev->Get()->Record;

        const auto& tableId = msg.GetTableId();
        const TTableId fullTableId(tableId.GetOwnerId(), tableId.GetTableId());

        const auto& userTables = Self->GetUserTables();
        auto it = userTables.find(fullTableId.PathId.LocalPathId);
        if (fullTableId.PathId.OwnerId != Self->GetPathOwnerId() || it == userTables.end()) {
            TString error = TStringBuilder()
                << "DataShard " << Self->TabletID() << " does not have a table "
                << tableId.GetOwnerId() << ":" << tableId.GetTableId();
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_SCHEME_ERROR,
                std::move(error));
            return true;
        }

        const auto& userTable = *it->second;
        if (tableId.GetSchemaVersion() != 0 && userTable.GetTableSchemaVersion() != tableId.GetSchemaVersion()) {
            TString error = TStringBuilder()
                << "DataShard " << Self->TabletID() << " has table "
                << tableId.GetOwnerId() << ":" << tableId.GetTableId()
                << " with schema version " << userTable.GetTableSchemaVersion()
                << " and cannot apply changes for schema version " << tableId.GetSchemaVersion();
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                tableId.GetSchemaVersion() < userTable.GetTableSchemaVersion()
                    ? NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OUTDATED_SCHEME
                    : NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_SCHEME_ERROR,
                std::move(error));
            return true;
        }

        if (userTable.HasAsyncIndexes() && Self->CheckChangesQueueOverflow()) {
            Self->IncCounter(COUNTER_CHANGE_QUEUE_OVERFLOW_REJECTS);
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OVERLOADED,
                "Change queue overflow");
            return true;
        }

        auto* replicatedTable = Self->EnsureReplicatedTable(fullTableId.PathId);
        Y_ENSURE(replicatedTable);
        const bool sourceCreated = !replicatedTable->Sources.contains(msg.GetSource());
        const ui64 savedNextSourceId = replicatedTable->NextSourceId;
        TReplicationSourceOffsetsDb rdb(txc);
        auto& source = replicatedTable->EnsureSource(rdb, msg.GetSource());

        if (userTable.HasAsyncIndexes()) {
            // Index collection may fault while reading the old row. Restore the
            // in-memory source state together with the database transaction.
            const ui64 sourceId = source.Id;
            const TString sourceName = source.Name;
            auto savedOffsets = source.OffsetBySplitKeyId;
            const ui64 savedNextSplitKeyId = source.NextSplitKeyId;
            const ui64 savedStatBytes = source.StatBytes;
            txc.DB.OnRollback([replicatedTable, &source, sourceCreated, sourceId, sourceName, savedNextSourceId,
                    savedOffsets = std::move(savedOffsets), savedNextSplitKeyId, savedStatBytes]() mutable
            {
                if (sourceCreated) {
                    replicatedTable->Sources.erase(sourceName);
                    replicatedTable->SourceById.erase(sourceId);
                    if (replicatedTable->NextSourceId == sourceId + 1) {
                        replicatedTable->NextSourceId = savedNextSourceId;
                    }
                    return;
                }
                source.Offsets.clear();
                source.OffsetBySplitKeyId = std::move(savedOffsets);
                for (auto& [_, state] : source.OffsetBySplitKeyId) {
                    source.Offsets.insert(&state);
                }
                source.NextSplitKeyId = savedNextSplitKeyId;
                source.StatBytes = savedStatBytes;
            });
        }

        NMiniKQL::TEngineHostCounters counters;
        TDataShardUserDb userDb(*Self, txc.DB, 0, Self->GetMvccVersion(), counters, ctx.Now());
        TDataShardChangeGroupProvider groupProvider(*Self, txc.DB);
        THolder<IDataShardChangeCollector> collector;
        if (userTable.HasAsyncIndexes()) {
            const auto indexKeySizePolicy = userTable.IsReplicated()
                ? EAsyncIndexKeySizePolicy::SkipOversizedIntermediateKeys
                : EAsyncIndexKeySizePolicy::Reject;
            collector.Reset(CreateChangeCollector(*Self, userDb, groupProvider, txc.DB, userTable, indexKeySizePolicy));
            // Previously staged base writes are the logical pre-image for new
            // global-consistency stream records, including after a shard reboot.
            for (ui64 openTxId : txc.DB.GetOpenTxs(userTable.LocalTid)) {
                userDb.AddCommitTxId(fullTableId, openTxId);
            }
        }

        try {
            for (const auto& change : msg.GetChanges()) {
                if (!ApplyChange(txc, fullTableId, userTable, source, change, userDb, collector.Get())) {
                    Y_ENSURE(Result);
                    break;
                }
            }
        } catch (const TNotReadyTabletException&) {
            txc.Reschedule();
            return false;
        } catch (const TKeySizeConstraintException&) {
            // Reject the whole batch, including previously applied rows and offsets.
            txc.DB.RollbackChanges();
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Size of key in secondary index is more than " << NLimits::MaxWriteKeySize);
            return true;
        }

        if (collector) {
            Changes = std::move(collector->GetCollected());
        }

        if (MvccVersion) {
            Self->PromoteImmediatePostExecuteEdges(*MvccVersion, TDataShard::EPromotePostExecuteEdges::ReadWrite, txc);
            Pipeline.AddCommittingOp(*MvccVersion);
            CommittingOpRegistered = true;
        }

        if (!Result) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK);
        }

        auto [_, locksBrokenByReplication] = Self->SysLocksTable().ApplyLocks();
        if (!locksBrokenByReplication.empty()) {
            auto victimQuerySpanIds = Self->SysLocksTable().ExtractVictimQuerySpanIds(locksBrokenByReplication);
            NDataIntegrity::LogLocksBroken(ctx, Self->TabletID(), "Replication apply broke locks on replicated rows", locksBrokenByReplication,
                                           Nothing(), victimQuerySpanIds);
        }
        return true;
    }

    bool ApplyChange(
            TTransactionContext& txc, const TTableId& tableId, const TUserTable& userTable,
            TReplicationSourceState& source, const NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& change,
            TDataShardUserDb& userDb, IDataShardChangeCollector* collector)
    {
        Y_ENSURE(userTable.IsReplicated() || Self->IsIncrementalRestore());

        // TODO: check source and offset, persist new values
        i64 sourceOffset = change.GetSourceOffset();

        ui64 writeTxId = change.GetWriteTxId();
        if (userTable.ReplicationConfig.HasRowConsistency() || userTable.IncrementalBackupConfig.HasWeakConsistency()) {
            if (writeTxId) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    "WriteTxId cannot be specified for row consistency");
                return false;
            }
        }

        // Global replicas also accept immediate writes during index initial
        // scan. The replication protocol chooses when to use WriteTxId.

        TSerializedCellVec keyCellVec;
        if (!TSerializedCellVec::TryParse(change.GetKey(), keyCellVec) ||
            keyCellVec.GetCells().size() != userTable.KeyColumnTypes.size())
        {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Key at " << EscapeC(source.Name) << ":" << sourceOffset << " is not a valid primary key");
            return false;
        }

        auto offset = source.Offsets.upper_bound(keyCellVec.GetCells());
        Y_ENSURE(offset != source.Offsets.begin());
        if (sourceOffset <= (*std::prev(offset))->MaxOffset) {
            // We have already seen this offset and ignore it
            return true;
        }

        TVector<TRawTypeValue> key;
        key.reserve(keyCellVec.GetCells().size());
        for (size_t i = 0; i < keyCellVec.GetCells().size(); ++i) {
            key.emplace_back(keyCellVec.GetCells()[i].AsRef(), userTable.KeyColumnTypes[i].GetTypeId());
        }

        NTable::ERowOp rop = NTable::ERowOp::Absent;
        TSerializedCellVec updateCellVec;
        TVector<NTable::TUpdateOp> update;
        switch (change.RowOperation_case()) {
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kUpsert: {
                rop = NTable::ERowOp::Upsert;
                if (!ParseUpdatesProto(userTable, source, sourceOffset, change.GetUpsert(), updateCellVec, update)) {
                    return false;
                }
                break;
            }
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kErase: {
                rop = NTable::ERowOp::Erase;
                break;
            }
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kReset: {
                rop = NTable::ERowOp::Reset;
                if (!ParseUpdatesProto(userTable, source, sourceOffset, change.GetReset(), updateCellVec, update)) {
                    return false;
                }
                break;
            }
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::ROWOPERATION_NOT_SET: {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_UNEXPECTED_ROW_OPERATION,
                    TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " has an unexpected row operation");
                return false;
            }
        }

        // Rejected records must not advance the persisted deduplication offset.
        TReplicationSourceOffsetsDb rdb(txc);
        source.AdvanceMaxOffset(rdb, keyCellVec.GetCells(), sourceOffset);

        if (writeTxId) {
            if (collector) {
                if (!MvccVersion) {
                    MvccVersion = Self->GetMvccVersion();
                }
                if (!collector->OnUpdate(tableId, userTable.LocalTid, rop, key, update, *MvccVersion, nullptr)) {
                    throw TNotReadyTabletException();
                }
            }
            txc.DB.UpdateTx(userTable.LocalTid, rop, key, update, writeTxId);
            if (collector) {
                userDb.AddCommitTxId(tableId, writeTxId);
            }
            Self->GetConflictsCache().GetTableCache(userTable.LocalTid).AddUncommittedWrite(keyCellVec.GetCells(), writeTxId, txc.DB);
        } else {
            if (!MvccVersion) {
                MvccVersion = Self->GetMvccVersion();
            }

            Self->SysLocksTable().BreakLocks(tableId, keyCellVec.GetCells());
            if (collector && !collector->OnUpdate(tableId, userTable.LocalTid, rop, key, update, *MvccVersion, nullptr)) {
                throw TNotReadyTabletException();
            }
            txc.DB.Update(userTable.LocalTid, rop, key, update, *MvccVersion);
            Self->GetConflictsCache().GetTableCache(userTable.LocalTid).RemoveUncommittedWrites(keyCellVec.GetCells(), txc.DB);
        }

        return true;
    }

    bool ParseUpdatesProto(
            const TUserTable& userTable,
            TReplicationSourceState& source, ui64 sourceOffset,
            const NKikimrTxDataShard::TEvApplyReplicationChanges::TUpdates& proto,
            TSerializedCellVec& updateCellVec,
            TVector<NTable::TUpdateOp>& update)
    {
        const auto& tags = proto.GetTags();
        size_t count = tags.size();
        if (!TSerializedCellVec::TryParse(proto.GetData(), updateCellVec) ||
            updateCellVec.GetCells().size() != count)
        {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " has invalid data");
            return false;
        }
        update.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            ui32 tag = tags[i];
            auto it = userTable.Columns.find(tag);
            if (it == userTable.Columns.end()) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " is updating an unknown column " << tag);
                return false;
            }
            if (it->second.IsKey) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " is updating a primary key column " << tag);
                return false;
            }
            update.emplace_back(tag, NTable::ECellOp::Set, TRawTypeValue(updateCellVec.GetCells()[i].AsRef(), it->second.Type.GetTypeId()));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_ENSURE(Ev);
        Y_ENSURE(Result);

        if (CommittingOpRegistered) {
            Y_ENSURE(MvccVersion);
            Pipeline.RemoveCommittingOp(*MvccVersion);
            Self->SendImmediateWriteResult(*MvccVersion, Ev->Sender, Result.Release(), Ev->Cookie);
        } else {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }

        if (Changes) {
            Self->EnqueueChangeRecords(std::move(Changes));
        }
    }

private:
    TPipeline& Pipeline;
    TEvDataShard::TEvApplyReplicationChanges::TPtr Ev;
    THolder<TEvDataShard::TEvApplyReplicationChangesResult> Result;
    std::optional<TRowVersion> MvccVersion;
    bool CommittingOpRegistered = false;
    TVector<IDataShardChangeCollector::TChange> Changes;
}; // TTxApplyReplicationChanges

void TDataShard::Handle(TEvDataShard::TEvApplyReplicationChanges::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = ev->Get()->Record;

    TVector<std::pair<TString, TSequenceRange>> advances;
    {
        const auto& tableId = msg.GetTableId();
        const auto& userTables = GetUserTables();
        auto it = userTables.find(tableId.GetTableId());
        if (it != userTables.end() && tableId.GetOwnerId() == GetPathOwnerId()) {
            advances = CollectSequenceAdvances(msg, *it->second);
        }
    }

    if (advances.empty()) {
        Execute(new TTxApplyReplicationChanges(this, Pipeline, std::move(ev)), ctx);
        return;
    }

    const auto& database = msg.GetDatabaseName();
    if (database.empty()) {
        auto result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
            NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
            NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OVERLOADED,
            TStringBuilder() << "Cannot advance sequences of table " << msg.GetTableId().GetTableId()
                << ": the replication request has no database name");
        ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
        return;
    }

    const ui64 pendingId = NextApplyReplicationChangesId++;
    auto& pending = PendingApplyReplicationChanges[pendingId];
    pending.Ev = std::move(ev);

    // The batch must not be applied if the sequences could not be advanced in time: reject it and
    // let the replication service retry.
    ctx.Schedule(TDuration::Seconds(10), new TEvPrivate::TEvAdvanceSequencesTimeout(pendingId));

    for (auto& [path, range] : advances) {
        const ui64 cookie = NextAdvanceSequenceCookie++;
        pending.Cookies.insert(cookie);
        AdvanceSequenceCookies.emplace(cookie, pendingId);

        auto request = MakeHolder<NSequenceProxy::TEvSequenceProxy::TEvAdvanceSequence>(
            database, path, range.first, range.second);
        ctx.Send(NSequenceProxy::MakeSequenceProxyServiceID(), request.Release(), 0, cookie);
    }
}

void TDataShard::Handle(NSequenceProxy::TEvSequenceProxy::TEvAdvanceSequenceResult::TPtr& ev, const TActorContext& ctx) {
    auto cookieIt = AdvanceSequenceCookies.find(ev->Cookie);
    if (cookieIt == AdvanceSequenceCookies.end()) {
        // A stale response from a batch which has already been rejected.
        return;
    }

    auto pendingIt = PendingApplyReplicationChanges.find(cookieIt->second);
    if (pendingIt == PendingApplyReplicationChanges.end()) {
        AdvanceSequenceCookies.erase(cookieIt);
        return;
    }
    auto& pending = pendingIt->second;

    if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
        // The data must not be applied while the sequence is behind it: reject the batch and let
        // the replication service retry it later.
        auto result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
            NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
            NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OVERLOADED,
            TStringBuilder() << "Cannot advance sequences: " << ev->Get()->Status
                << ", " << ev->Get()->Issues.ToOneLineString());
        ctx.Send(pending.Ev->Sender, result.Release(), 0, pending.Ev->Cookie);

        for (const ui64 cookie : pending.Cookies) {
            AdvanceSequenceCookies.erase(cookie);
        }
        PendingApplyReplicationChanges.erase(pendingIt);
        return;
    }

    pending.Cookies.erase(ev->Cookie);
    AdvanceSequenceCookies.erase(cookieIt);

    if (!pending.Cookies.empty()) {
        return;
    }

    auto applyEv = std::move(pending.Ev);
    PendingApplyReplicationChanges.erase(pendingIt);
    Execute(new TTxApplyReplicationChanges(this, Pipeline, std::move(applyEv)), ctx);
}

void TDataShard::Handle(TEvPrivate::TEvAdvanceSequencesTimeout::TPtr& ev, const TActorContext& ctx) {
    auto pendingIt = PendingApplyReplicationChanges.find(ev->Get()->PendingId);
    if (pendingIt == PendingApplyReplicationChanges.end()) {
        // The batch has already been applied or rejected.
        return;
    }

    auto& pending = pendingIt->second;
    auto result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
        NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
        NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OVERLOADED,
        "Cannot advance sequences: timeout");
    ctx.Send(pending.Ev->Sender, result.Release(), 0, pending.Ev->Cookie);

    for (const ui64 cookie : pending.Cookies) {
        AdvanceSequenceCookies.erase(cookie);
    }
    PendingApplyReplicationChanges.erase(pendingIt);
}

} // NDataShard
} // NKikimr
