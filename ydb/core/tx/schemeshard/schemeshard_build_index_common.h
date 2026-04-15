#pragma once

#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr {
namespace NSchemeShard {


TPath GetBuildPath(TSchemeShard* ss, const TIndexBuildInfo& buildInfo, const TString& tableName);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose
    (TSchemeShard* ss, const TIndexBuildInfo& buildInfo, TTxId txId, const TPath& path);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose
    (TSchemeShard* ss, const TIndexBuildInfo& buildInfo);

template<typename TOperationInfo, typename TDoFunc>
THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTableProposeTemplate(
    TSchemeShard* ss, const TOperationInfo& operationInfo, TTxId txId, TDoFunc&& doFunc)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(
        ui64(txId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
    modifyScheme.SetInternal(true);
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(operationInfo.LockTxId));

    TPath path = TPath::Init(operationInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableAlterTable()->SetName(path.LeafName());

    doFunc(operationInfo, modifyScheme);

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "AlterMainTablePropose " << operationInfo.Id << " " << propose->Record.ShortDebugString());

    return propose;
}

} // namespace NSchemeShard
} // namespace NKikimr
