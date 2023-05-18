#include "kqp_tx.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;

TIssue GetLocksInvalidatedIssue(const TKqpTransactionContext& txCtx, const TMaybe<TKqpTxLock>& invalidatedLock) {
    TStringBuilder message;
    message << "Transaction locks invalidated.";

    TMaybe<TString> tableName;
    if (invalidatedLock) {
        TKikimrPathId id(invalidatedLock->GetSchemeShard(), invalidatedLock->GetPathId());
        auto table = txCtx.TableByIdMap.FindPtr(id);
        if (table) {
            tableName = *table;
        }
    }

    if (tableName) {
        message << " Table: " << *tableName;
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
    if (*txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE)
        return false;

    if (!config.FeatureFlags.GetEnableMvccSnapshotReads())
        return false;

    if (txCtx.GetSnapshot().IsValid())
        return false;

    if (rollbackTx)
        return false;
    if (!commitTx)
        return true;

    size_t readPhases = 0;
    bool hasEffects = false;

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
    }

    if (txCtx.HasUncommittedChangesRead) {
        YQL_ENSURE(txCtx.EnableImmediateEffects);
        return true;
    }

    // We don't want snapshot when there are effects at the moment,
    // because it hurts performance when there are multiple single-shard
    // reads and a single distributed commit. Taking snapshot costs
    // similar to an additional distributed transaction, and it's very
    // hard to predict when that happens, causing performance
    // degradation.
    if (hasEffects) {
        return false;
    }

    // We need snapshot when there are multiple table read phases, most
    // likely it involves multiple tables and we would have to use a
    // distributed commit otherwise. Taking snapshot helps as avoid TLI
    // for read-only transactions, and costs less than a final distributed
    // commit.
    return readPhases > 1;
}

} // namespace NKqp
} // namespace NKikimr
