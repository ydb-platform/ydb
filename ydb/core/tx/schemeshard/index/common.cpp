#include <ydb/core/tx/schemeshard/index/common.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

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

    YDB_LOG_NOTICE("LockPropose",
        {"buildInfoId", buildInfo.Id},
        {"buildInfoState", buildInfo.State},
        {"propose", propose->Record});

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

    YDB_LOG_NOTICE("UnlockPropose",
        {"buildInfoId", buildInfo.Id},
        {"buildInfoState", buildInfo.State},
        {"propose", propose->Record});

    return propose;
}

} // namespace NSchemeShard
} // namespace NKikimr
