#include "kqp_tx.h"

#include <contrib/libs/protobuf/src/google/protobuf/util/message_differencer.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

NYql::TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TKikimrPathId& pathId) {
    TStringBuilder message;
    message << "Transaction locks invalidated.";

    if (pathId.OwnerId() != 0) {
        auto table = txCtx.TableByIdMap.FindPtr(pathId);
        if (!table) {
            return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message << " Unknown table.");
        }
        return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message << " Table: " << "`" << *table << "`");
    } else {
        // Olap tables don't return SchemeShard in locks, thus we use tableId here.
        for (const auto& [pathId, table] : txCtx.TableByIdMap) {
            if (pathId.TableId() == pathId.TableId()) {
                return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message << " Table: " << "`" << table << "`");
            }
        }
        return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message << " Unknown table."); 
    }
}

TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TKqpTxLock& invalidatedLock) {
    return GetLocksInvalidatedIssue(
        txCtx,
        TKikimrPathId(
            invalidatedLock.GetSchemeShard(),
            invalidatedLock.GetPathId()));
}

NYql::TIssue GetLocksInvalidatedIssue(const TShardIdToTableInfo& shardIdToTableInfo, const ui64& shardId) {
    TStringBuilder message;
    message << "Transaction locks invalidated.";

    if (auto tableInfoPtr = shardIdToTableInfo.GetPtr(shardId); tableInfoPtr) {
        message << " Tables: ";
        bool first = true;
        for (const auto& path : tableInfoPtr->Pathes) {
            if (!first) {
                message << ", ";
                first = false;
            }
            message << "`" << path << "`";
        }
        return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
    } else {
        message << " Unknown table.";   
    }
    return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
}

std::pair<bool, std::vector<TIssue>> MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
    TKqpTransactionContext& txCtx)
{
    std::pair<bool, std::vector<TIssue>> res;
    auto& locks = txCtx.Locks;

    YQL_ENSURE(type.GetKind() == NKikimrMiniKQL::ETypeKind::List);
    auto locksListType = type.GetList();

    if (!locks.HasLocks()) {
        locks.LockType = locksListType.GetItem();
        locks.LocksListType = locksListType;
    }

    YQL_ENSURE(locksListType.GetItem().GetKind() == NKikimrMiniKQL::ETypeKind::Struct);
    auto lockType = locksListType.GetItem().GetStruct();
    YQL_ENSURE(lockType.MemberSize() == 7);
    YQL_ENSURE(lockType.GetMember(0).GetName() == "Counter");
    YQL_ENSURE(lockType.GetMember(1).GetName() == "DataShard");
    YQL_ENSURE(lockType.GetMember(2).GetName() == "Generation");
    YQL_ENSURE(lockType.GetMember(3).GetName() == "LockId");
    YQL_ENSURE(lockType.GetMember(4).GetName() == "PathId");
    YQL_ENSURE(lockType.GetMember(5).GetName() == "SchemeShard");
    YQL_ENSURE(lockType.GetMember(6).GetName() == "HasWrites");

    res.first = true;
    for (auto& lockValue : value.GetList()) {
        TKqpTxLock txLock(lockValue);
        if (auto counter = txLock.GetCounter(); counter >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin) {
            switch (counter) {
                case NKikimr::TSysTables::TLocksTable::TLock::ErrorAlreadyBroken:
                case NKikimr::TSysTables::TLocksTable::TLock::ErrorBroken:
                    res.second.emplace_back(GetLocksInvalidatedIssue(txCtx, txLock));
                    break;
                default:
                    res.second.emplace_back(YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_ACQUIRE_FAILURE));
                    break;
            }
            res.first = false;

        } else if (auto curTxLock = locks.LocksMap.FindPtr(txLock.GetKey())) {
            if (txLock.HasWrites()) {
                curTxLock->SetHasWrites();
            }

            if (curTxLock->Invalidated(txLock)) {
                res.second.emplace_back(GetLocksInvalidatedIssue(txCtx, txLock));
                res.first = false;
            }
        } else {
            // despite there were some errors we need to proceed merge to erase remaining locks properly
            locks.LocksMap.insert(std::make_pair(txLock.GetKey(), txLock));
        }
    }

    return res;
}

TKqpTransactionInfo TKqpTransactionContext::GetInfo() const {
    TKqpTransactionInfo txInfo;

    // Status
    if (Invalidated) {
        txInfo.Status = TKqpTransactionInfo::EStatus::Aborted;
    } else if (Closed) {
        txInfo.Status = TKqpTransactionInfo::EStatus::Committed;
    } else {
        txInfo.Status = TKqpTransactionInfo::EStatus::Active;
    }

    // Kind
    bool hasReads = false;
    bool hasWrites = false;
    for (auto& pair : TableOperations) {
        hasReads = hasReads || (pair.second & KikimrReadOps());
        hasWrites = hasWrites || (pair.second & KikimrModifyOps());
    }

    if (hasReads) {
        txInfo.Kind = hasWrites
            ? TKqpTransactionInfo::EKind::ReadWrite
            : TKqpTransactionInfo::EKind::ReadOnly;
    } else {
        txInfo.Kind = hasWrites
            ? TKqpTransactionInfo::EKind::WriteOnly
            : TKqpTransactionInfo::EKind::Pure;
    }

    txInfo.TotalDuration = FinishTime - CreationTime;
    txInfo.ServerDuration = QueriesDuration;
    txInfo.QueriesCount = QueriesCount;

    return txInfo;
}

bool NeedSnapshot(const TKqpTransactionContext& txCtx, const NYql::TKikimrConfiguration& config, bool rollbackTx,
    bool commitTx, const NKqpProto::TKqpPhyQuery& physicalQuery)
{
    Y_UNUSED(config);

    if (*txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE &&
        *txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SNAPSHOT_RO &&
        *txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SNAPSHOT_RW)
        return false;

    if (txCtx.GetSnapshot().IsValid())
        return false;

    if (rollbackTx)
        return false;
    if (!commitTx)
        return true;

    size_t readPhases = 0;
    bool hasEffects = false;
    bool hasStreamLookup = false;
    bool hasSinkWrite = false;
    bool hasSinkInsert = false;
    bool hasVectorResolve = false;

    for (const auto &tx : physicalQuery.GetTransactions()) {
        switch (tx.GetType()) {
            case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                // ignore pure computations
                break;

            default:
                ++readPhases;
                break;
        }

        if (tx.GetHasEffects()) {
            hasEffects = true;
        }

        for (const auto &stage : tx.GetStages()) {
            hasSinkWrite |= !stage.GetSinks().empty();

            for (const auto &sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink
                    && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>())
                {
                    NKikimrKqp::TKqpTableSinkSettings sinkSettings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings));
                    if (sinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT) {
                        hasSinkInsert = true;
                    }
                }
            }

            for (const auto &input : stage.GetInputs()) {
                hasStreamLookup |= input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup;
                hasVectorResolve |= input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kVectorResolve;
            }

            for (const auto &tableOp : stage.GetTableOps()) {
                if (tableOp.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
                    // always need snapshot for OLAP reads
                    return true;
                }
            }
        }
    }

    if (txCtx.NeedUncommittedChangesFlush || AppData()->FeatureFlags.GetEnableForceImmediateEffectsExecution()) {
        return true;
    }

    YQL_ENSURE(!hasSinkWrite || hasEffects);

    // We need snapshot for stream lookup, besause it's used for dependent reads
    if (hasStreamLookup || hasVectorResolve) {
        return true;
    }

    if (*txCtx.EffectiveIsolationLevel == NKikimrKqp::ISOLATION_LEVEL_SNAPSHOT_RW) {
        if (hasEffects && !txCtx.HasTableRead) {
            YQL_ENSURE(txCtx.HasTableWrite);
            // Don't need snapshot for WriteOnly transaction.
            return false;
        } else if (hasEffects) {
            YQL_ENSURE(txCtx.HasTableWrite);
            // ReadWrite transaction => need snapshot
            return true;
        }
        // ReadOnly transaction here
    }

    if (hasSinkInsert && readPhases > 0) {
        YQL_ENSURE(hasEffects);
        // Insert operations create new read phases,
        // so in presence of other reads we have to acquire snapshot.
        // This is unique to INSERT operation, because it can fail.
        return true;
    }

    // We need snapshot when there are multiple table read phases, most
    // likely it involves multiple tables and we would have to use a
    // distributed commit otherwise. Taking snapshot helps as avoid TLI
    // for read-only transactions, and costs less than a final distributed
    // commit.
    // NOTE: In case of read from single shard, we won't take snapshot.
    return readPhases > 1;
}

bool HasOlapTableReadInTx(const NKqpProto::TKqpPhyQuery& physicalQuery) {
    for (const auto &tx : physicalQuery.GetTransactions()) {
        for (const auto &stage : tx.GetStages()) {
            for (const auto &tableOp : stage.GetTableOps()) {
                if (tableOp.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool HasOlapTableWriteInStage(const NKqpProto::TKqpPhyStage& stage) {
    for (const auto& sink : stage.GetSinks()) {
        if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            NKikimrKqp::TKqpTableSinkSettings settings;
            YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
            return settings.GetIsOlap();
        }
    }
    return false;
}

bool HasOlapTableWriteInTx(const NKqpProto::TKqpPhyQuery& physicalQuery) {
    for (const auto &tx : physicalQuery.GetTransactions()) {
        for (const auto &stage : tx.GetStages()) {
            if (HasOlapTableWriteInStage(stage)) {
                return true;
            }
        }
    }
    return false;
}

bool HasOltpTableReadInTx(const NKqpProto::TKqpPhyQuery& physicalQuery) {
    for (const auto &tx : physicalQuery.GetTransactions()) {
        for (const auto &stage : tx.GetStages()) {
            for (const auto &source : stage.GetSources()) {
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource){
                    return true;
                }
            }
            for (const auto &tableOp : stage.GetTableOps()) {
                switch (tableOp.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                        return true;
                    case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
                    case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                    case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                        break;
                    default:
                        YQL_ENSURE(false, "unexpected type");
                }
            }
        }
    }
    return false;
}

bool HasOltpTableWriteInTx(const NKqpProto::TKqpPhyQuery& physicalQuery) {
    for (const auto &tx : physicalQuery.GetTransactions()) {
        for (const auto &stage : tx.GetStages()) {
            for (const auto &tableOp : stage.GetTableOps()) {
                if (tableOp.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kUpsertRows
                    || tableOp.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kDeleteRows) {
                    return true;
                }
            }

            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    return !settings.GetIsOlap();
                }
            }
        }
    }
    return false;
}

bool HasUncommittedChangesRead(THashSet<NKikimr::TTableId>& modifiedTables, const NKqpProto::TKqpPhyQuery& physicalQuery, const bool commit) {
    auto getTable = [](const NKqpProto::TKqpPhyTableId& table) {
        return NKikimr::TTableId(table.GetOwnerId(), table.GetTableId());
    };

    for (size_t index = 0; index < physicalQuery.TransactionsSize(); ++index) {
        const auto &tx = physicalQuery.GetTransactions()[index];
        for (const auto &stage : tx.GetStages()) {
            for (const auto &tableOp : stage.GetTableOps()) {
                switch (tableOp.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadRanges: {
                        if (modifiedTables.contains(getTable(tableOp.GetTable()))) {
                            return true;
                        }
                        break;
                    }
                    case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
                    case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                    case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                        modifiedTables.insert(getTable(tableOp.GetTable()));
                        break;
                    default:
                        YQL_ENSURE(false, "unexpected type");
                }
            }

            for (const auto& input : stage.GetInputs()) {
                switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                    if (modifiedTables.contains(getTable(input.GetStreamLookup().GetTable()))) {
                        return true;
                    }
                    break;
                case NKqpProto::TKqpPhyConnection::kSequencer:
                    return true;
                case NKqpProto::TKqpPhyConnection::kVectorResolve: // FIXME: Maybe, when prefix tables are enabled
                case NKqpProto::TKqpPhyConnection::kUnionAll:
                case NKqpProto::TKqpPhyConnection::kParallelUnionAll:
                case NKqpProto::TKqpPhyConnection::kMap:
                case NKqpProto::TKqpPhyConnection::kHashShuffle:
                case NKqpProto::TKqpPhyConnection::kBroadcast:
                case NKqpProto::TKqpPhyConnection::kMapShard:
                case NKqpProto::TKqpPhyConnection::kShuffleShard:
                case NKqpProto::TKqpPhyConnection::kResult:
                case NKqpProto::TKqpPhyConnection::kValue:
                case NKqpProto::TKqpPhyConnection::kMerge:
                case NKqpProto::TKqpPhyConnection::TYPE_NOT_SET:
                    break;
                }
            }

            for (const auto& source : stage.GetSources()) {
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    if (modifiedTables.contains(getTable(source.GetReadRangesSource().GetTable()))) {
                        return true;
                    }
                } else {
                    return true;
                }
            }

            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink) {
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>());
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    modifiedTables.insert(getTable(settings.GetTable()));
                    if (settings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT && !commit) {
                        // INSERT with sink should be executed immediately, because it returns an error in case of duplicate rows.
                        return true;
                    }
                } else {
                    return true;
                }
            }
        }
    }
    return false;
}

} // namespace NKqp
} // namespace NKikimr
