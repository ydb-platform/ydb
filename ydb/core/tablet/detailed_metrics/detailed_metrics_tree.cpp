#include "detailed_metrics_tree.h"

#include <util/string/cast.h>

namespace NKikimr::NDetailedMetrics {

    const TString DATABASE_LABEL = "database";
    const TString TABLE_LABEL = "table";
    const TString TABLET_ID_LABEL = "tablet_id";
    const TString FOLLOWER_ID_LABEL = "follower_id";

    const TString TYPE_LABEL = "type";
    const TString CATEGORY_LABEL = "category";
    const TString EXECUTOR_CATEGORY = "executor";
    const TString APP_CATEGORY = "app";

    TStringBuf ChopTrailingSlash(TStringBuf path) {
        path.ChopSuffix("/");
        return path;
    }

    TStringBuf MakeRelativeTablePath(const TStringBuf databasePrefix, const TString& tablePath) {
        TStringBuf relativePath(tablePath);
        // Require the separator: /Root/db10/table is not inside /Root/db1.
        if (relativePath.SkipPrefix(databasePrefix) && relativePath.SkipPrefix("/") && !relativePath.empty()) {
            return relativePath;
        }
        return TStringBuf(tablePath);
    }

    NMonitoring::TDynamicCounterPtr GetOrCreateTabletGroup(
        NMonitoring::TDynamicCounterPtr parentGroup, const TTabletKey& tablet)
    {
        return parentGroup->GetSubgroup(TABLET_ID_LABEL, ToString(tablet.first))
            ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(tablet.second));
    }

    NMonitoring::TDynamicCounterPtr GetOrCreateTypeGroup(
        NMonitoring::TDynamicCounterPtr bucketGroup, TTabletTypes::EType tabletType)
    {
        return bucketGroup->GetSubgroup(TYPE_LABEL, TTabletTypes::TypeToStr(tabletType));
    }

    TSubgroupPath MakeTabletPath(const TTabletKey& tablet, TSubgroupPath prefix) {
        prefix.emplace_back(TABLET_ID_LABEL, ToString(tablet.first));
        prefix.emplace_back(FOLLOWER_ID_LABEL, ToString(tablet.second));
        return prefix;
    }

    TSubgroupPath MakeRawBucketPath(TTabletTypes::EType tabletType, TSubgroupPath prefix) {
        prefix.emplace_back(TYPE_LABEL, TTabletTypes::TypeToStr(tabletType));
        return prefix;
    }

} // namespace NKikimr::NDetailedMetrics
