#include "schemeshard_build_index_common.h"

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo, TTxId txId, const TPath& path)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    propose->Record.SetFailOnExist(false);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLock);
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableLockConfig()->SetName(path.LeafName());
    modifyScheme.MutableLockConfig()->SetLockTxId(ui64(buildInfo.LockTxId));

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "LockPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

} // namespace NSchemeShard
} // namespace NKikimr
