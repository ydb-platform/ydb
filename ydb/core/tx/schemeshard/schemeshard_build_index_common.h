#pragma once

#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_index_build_info.h"
#include "schemeshard_index_utils.h"

namespace NKikimr {
namespace NSchemeShard {


TPath GetBuildPath(TSchemeShard* ss, const TIndexBuildInfo& buildInfo, const TString& tableName);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose
    (TSchemeShard* ss, const TIndexBuildInfo& buildInfo, TTxId txId, const TPath& path);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose
    (TSchemeShard* ss, const TIndexBuildInfo& buildInfo, TVector<TPath> additionalPaths = {});

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
