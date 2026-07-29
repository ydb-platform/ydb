#include <ydb/core/tx/schemeshard/index/common.h>

namespace NKikimr {
namespace NSchemeShard {

TPath GetBuildPath(TSchemeShard* ss, const TIndexBuildInfo& buildInfo, const TString& tableName) {
    return TPath::Init(buildInfo.TablePathId, ss)
        .Dive(buildInfo.IndexName)
        .Dive(tableName);
}

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

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.UnlockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto addUnlock = [&](TPath path) {
        NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropLock);
        modifyScheme.SetInternal(true);
        modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

        modifyScheme.SetWorkingDir(path.Parent().PathString());

        auto& lockConfig = *modifyScheme.MutableLockConfig();
        lockConfig.SetName(path.LeafName());
    };

    addUnlock(TPath::Init(buildInfo.TablePathId, ss));

    if (buildInfo.IsValidatingUniqueIndex()
        || buildInfo.IsFlatRelevanceFulltext())
    {
        // Unlock also indexImplTable
        TPath indexImplTablePath = GetBuildPath(ss, buildInfo, NTableIndex::ImplTable);
        if (indexImplTablePath.IsResolved() && !indexImplTablePath.IsDeleted() && indexImplTablePath.IsLocked()) {
            addUnlock(std::move(indexImplTablePath));
        }
    }

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "UnlockPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

} // namespace NSchemeShard
} // namespace NKikimr
