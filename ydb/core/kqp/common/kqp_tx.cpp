#include "kqp_tx.h"

#include <contrib/libs/protobuf/src/google/protobuf/util/message_differencer.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

void AppendQuerySpanIdInfo(TStringBuilder& message, TMaybe<ui64> victimQuerySpanId) {
    if (victimQuerySpanId && *victimQuerySpanId != 0) {
        message << " VictimQuerySpanId: " << *victimQuerySpanId << ".";
    }
}

} // anonymous namespace

NYql::TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TKikimrPathId& pathId,
    TMaybe<ui64> victimQuerySpanId)
{
    TStringBuilder message;
    message << "Transaction locks invalidated.";

    if (pathId.OwnerId() != 0) {
        auto table = txCtx.TableByIdMap.FindPtr(pathId);
        if (!table) {
            message << " Unknown table, pathId: " << pathId.ToString() << ".";
            AppendQuerySpanIdInfo(message, victimQuerySpanId);
            return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
        }
        message << " Table: `" << *table << "`.";
        AppendQuerySpanIdInfo(message, victimQuerySpanId);
        return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
    } else {
        // Olap tables don't return SchemeShard in locks, thus we use tableId here.
        for (const auto& [candidatePathId, table] : txCtx.TableByIdMap) {
            if (candidatePathId.TableId() == pathId.TableId()) {
                message << " Table: `" << table << "`.";
                AppendQuerySpanIdInfo(message, victimQuerySpanId);
                return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
            }
        }
        message << " Unknown table, pathId: " << pathId.ToString() << ".";
        AppendQuerySpanIdInfo(message, victimQuerySpanId);
        return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
    }
}

NYql::TIssue GetLocksInvalidatedIssue(const TShardIdToTableInfo& shardIdToTableInfo, const ui64& shardId,
    TMaybe<ui64> victimQuerySpanId)
{
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
        message << ".";
        AppendQuerySpanIdInfo(message, victimQuerySpanId);
        return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
    } else {
        message << " Unknown table, tabletId: " << shardId << ".";
        AppendQuerySpanIdInfo(message, victimQuerySpanId);
    }
    return YqlIssue(TPosition(), TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message);
}

// Non-TxManager (obsolete) path: no QuerySpanId tracking
TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TKqpTxLock& invalidatedLock) {
    return GetLocksInvalidatedIssue(
        txCtx,
        TKikimrPathId(
            invalidatedLock.GetSchemeShard(),
            invalidatedLock.GetPathId()));
}

// Non-TxManager (obsolete) lock merge path: no QuerySpanId tracking
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

    if (*txCtx.EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_SERIALIZABLE &&
        *txCtx.EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RO &&
        *txCtx.EffectiveIsolationLevel != NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW)
        return false;

    if (txCtx.GetSnapshot().IsValid())
        return false;

    if (rollbackTx)
        return false;
    if (!commitTx)
        return true;

    size_t readPhases = 0;
    bool hasEffects = false;
    bool hasInsert = false;

    for (const auto &tx : physicalQuery.GetTransactions()) {
        if (tx.GetHasEffects()) {
            hasEffects = true;
        }

        for (const auto &stage : tx.GetStages()) {
            readPhases += stage.SourcesSize();

            for (const auto &sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink
                    && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>())
                {
                    AFL_ENSURE(tx.GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE);
                    NKikimrKqp::TKqpTableSinkSettings sinkSettings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings));
                    AFL_ENSURE(tx.GetHasEffects() || sinkSettings.GetInconsistentTx());
                    if (sinkSettings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT) {
                        hasInsert = true;
                        // Insert operations create new read phases,
                        // so in presence of other reads we have to acquire snapshot.
                        // This is unique to INSERT operation, because it can fail.
                        ++readPhases;
                    }

                    if (!sinkSettings.GetLookupColumns().empty()) {
                        // Lookup for index update
                        ++readPhases;
                    }

                    for (const auto& index : sinkSettings.GetIndexes()) {
                        if (index.GetIsUniq()) {
                            // Unique index check
                            ++readPhases;
                        }
                    }
                }
            }

            for (const auto &input : stage.GetInputs()) {
                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup
                        || input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kVectorResolve) {
                    // We need snapshot for stream lookup, besause it's used for dependent reads
                    return true;
                }
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

    if (*txCtx.EffectiveIsolationLevel == NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW && hasEffects) {
        // Avoid acquiring snapshot for WriteOnly transactions.
        // If there are more than one INSERT, we have to acquiring snapshot,
        // because INSERT has output (error or no error).
        return hasInsert ? readPhases > 1 : readPhases > 0;
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
    for (const auto& transform : stage.GetOutputTransforms()) {
        if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink && transform.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            NKikimrKqp::TKqpTableSinkSettings settings;
            YQL_ENSURE(transform.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
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

                if (source.GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource) {
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

            for (const auto& transform : stage.GetOutputTransforms()) {
                if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink && transform.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(transform.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
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
                case NKqpProto::TKqpPhyConnection::kDqSourceStreamLookup:
                case NKqpProto::TKqpPhyConnection::TYPE_NOT_SET:
                    break;
                }
            }

            for (const auto& source : stage.GetSources()) {
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    if (modifiedTables.contains(getTable(source.GetReadRangesSource().GetTable()))) {
                        return true;
                    }
                } else if (source.GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource) {
                    if (modifiedTables.contains(getTable(source.GetFullTextSource().GetTable()))) {
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

                    const bool tableModifiedBefore = modifiedTables.contains(getTable(settings.GetTable()));
                    modifiedTables.insert(getTable(settings.GetTable()));
                    for (const auto& index : settings.GetIndexes()) {
                        modifiedTables.insert(getTable(index.GetTable()));
                    }

                    // For plans compatibility with old indexes. Don't need it for new.
                    if (!settings.GetLookupColumns().empty() && tableModifiedBefore) {
                        AFL_ENSURE(settings.GetType() != NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);
                        return true;
                    }

                    // For plans compatibility with old indexes. Don't need it for new.
                    for (const auto& index : settings.GetIndexes()) {
                        if (index.GetIsUniq() && tableModifiedBefore) {
                            return true;
                        }
                    }

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
