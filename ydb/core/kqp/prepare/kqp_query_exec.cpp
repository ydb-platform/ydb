#include "kqp_prepare.h"

#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/tx/datashard/sys_tables.h>

#include <ydb/library/yql/utils/log/log.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NCommon;
using namespace NThreading;

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
        TKqpTransactionContext& txCtx) {

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
    YQL_ENSURE(lockType.MemberSize() == 6);
    YQL_ENSURE(lockType.GetMember(0).GetName() == "Counter");
    YQL_ENSURE(lockType.GetMember(1).GetName() == "DataShard");
    YQL_ENSURE(lockType.GetMember(2).GetName() == "Generation");
    YQL_ENSURE(lockType.GetMember(3).GetName() == "LockId");
    YQL_ENSURE(lockType.GetMember(4).GetName() == "PathId");
    YQL_ENSURE(lockType.GetMember(5).GetName() == "SchemeShard");

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

bool MergeLocks(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value, TKqpTransactionContext& txCtx,
        TExprContext& ctx) {
    auto [success, issues] = MergeLocks(type, value, txCtx);
    if (!success) {
        if (!txCtx.GetSnapshot().IsValid()) {
            for (auto& issue : issues) {
                ctx.AddError(std::move(issue));
            }
            return false;
        } else {
            txCtx.Locks.MarkBroken(issues.back());
            if (!txCtx.DeferredEffects.Empty()) {
                txCtx.Locks.ReportIssues(ctx);
                return false;
            }
        }
    }
    return true;
}

bool UnpackMergeLocks(const NKikimrMiniKQL::TResult& result, TKqpTransactionContext& txCtx, TExprContext& ctx) {
    auto structType = result.GetType().GetStruct();
    ui32 locksIndex;
    bool found = GetRunResultIndex(structType, TString(NKikimr::NMiniKQL::TxLocksResultLabel2), locksIndex);
    YQL_ENSURE(found ^ txCtx.Locks.Broken());

    if (found) {
        auto locksType = structType.GetMember(locksIndex).GetType().GetOptional().GetItem();
        auto locksValue = result.GetValue().GetStruct(locksIndex).GetOptional();

        return MergeLocks(locksType, locksValue, txCtx, ctx);
    }

    return false;
}

} // namespace NKqp
} // namespace NKikimr
