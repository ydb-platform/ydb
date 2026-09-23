#include <ydb/core/tx/schemeshard/index/common.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr {
namespace NSchemeShard {

TPath GetBuildPath(TSchemeShard* ss, const TIndexBuildInfo& buildInfo, const TString& tableName) {
    return TPath::Init(buildInfo.TablePathId, ss)
        .Dive(buildInfo.IndexName)
        .Dive(tableName);
}

TPath GetShardsPath(TSchemeShard* ss, const TIndexBuildInfo& buildInfo) {
    switch (buildInfo.BuildKind) {
        case TIndexBuildInfo::EBuildKind::BuildSecondaryIndex:
        case TIndexBuildInfo::EBuildKind::BuildColumns:
        case TIndexBuildInfo::EBuildKind::BuildFulltext:
            if (buildInfo.SubState == TIndexBuildInfo::ESubState::FulltextIndexDictionary) {
                if (buildInfo.IsBuildFulltextCompact()) {
                    return GetBuildPath(ss, buildInfo, TString::Join(NTableIndex::ImplTable, NTableIndex::NKMeans::BuildSuffix0));
                }
                return GetBuildPath(ss, buildInfo, NTableIndex::ImplTable);
            }
            // Compact rowid-mode: the posting fill (SubState None) scans the row-id source table;
            // the prepass (FulltextRowIdSrc) and all other builds scan the main table.
            if (buildInfo.SubState == TIndexBuildInfo::ESubState::None && buildInfo.IsBuildFulltextCompactRowId()) {
                return GetBuildPath(ss, buildInfo, TString::Join(NTableIndex::ImplTable, NTableIndex::NFulltext::RowIdSrcBuildSuffix));
            }
            return TPath::Init(buildInfo.TablePathId, ss);
        case TIndexBuildInfo::EBuildKind::BuildSecondaryUniqueIndex:
            return buildInfo.IsValidatingUniqueIndex()
                ? GetBuildPath(ss, buildInfo, NTableIndex::ImplTable)
                : TPath::Init(buildInfo.TablePathId, ss);
        case TIndexBuildInfo::EBuildKind::BuildVectorIndex:
        case TIndexBuildInfo::EBuildKind::BuildPrefixedVectorIndex:
            if (buildInfo.KMeans.Level == 1 &&
                buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::Filter &&
                buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::FilterBorders) {
                return TPath::Init(buildInfo.TablePathId, ss);
            } else {
                return GetBuildPath(ss, buildInfo, buildInfo.KMeans.ReadFrom());
            }
        default:
            Y_ENSURE(false, buildInfo.InvalidBuildKind());
    }
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
        {"buildId", buildInfo.Id},
        {"state", buildInfo.State},
        {"propose", propose->Record.ShortDebugString()},
    );

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
        {"buildId", buildInfo.Id},
        {"state", buildInfo.State},
        {"propose", propose->Record.ShortDebugString()},
    );

    return propose;
}

} // namespace NSchemeShard
} // namespace NKikimr

#undef YDB_LOG_THIS_FILE_COMPONENT
