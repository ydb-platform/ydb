#include "resolve_local_db_table.h"

#include <ydb/core/tablet_flat/flat_dbase_apply.h>

namespace NKikimr {
namespace NGRpcService {

    TMaybe<ui64> TryParseLocalDbPath(const TVector<TString>& path) {
        if (path.size() != 4 || path[1] != ".sys_tablets") {
            return {};
        }

        ui64 tabletId = -1;
        TString tabletIdStr = path[2];
        if (TryFromString<ui64>(tabletIdStr, tabletId)) {
            return tabletId;
        } else {
            return {};
        }
    }

    void FillLocalDbTableSchema(
            NSchemeCache::TSchemeCacheNavigate& result,
            const NTabletFlatScheme::TSchemeChanges& fullScheme,
            const TString& tableName)
    {
        NTable::TScheme scheme;
        NTable::TSchemeModifier applier(scheme);
        applier.Apply(fullScheme);

        result.ResultSet.resize(1);
        NSchemeCache::TSchemeCacheNavigate::TEntry& entry = result.ResultSet.back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;

        entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;

        auto* ti = scheme.TableNames.FindPtr(tableName);
        if (!ti) {
            entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown;
            return;
        }

        entry.Kind = NSchemeCache::TSchemeCacheNavigate::KindTable;

        const NTable::TScheme::TTableInfo* tableInfo = scheme.Tables.FindPtr(*ti);

        for (const auto& col : tableInfo->Columns) {
            entry.Columns[col.first] = TSysTables::TTableColumnInfo(
                col.second.Name, col.first, col.second.PType, col.second.PTypeMod, col.second.KeyOrder,
                {}, TSysTables::TTableColumnInfo::EDefaultKind::DEFAULT_UNDEFINED, {}, false, col.second.NotNull);
        }
    }

    bool IsSuperUser(const NACLib::TUserToken& userToken, const TAppData& appData) {
        const auto& adminSids = appData.AdministrationAllowedSIDs;
        for (const auto& sid : adminSids) {
            if (userToken.IsExist(sid))
                return true;
        }
        return false;
    }

} // namespace NKikimr
} // namespace NGRpcService
