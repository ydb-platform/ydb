#pragma once

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/string/cast.h>

#include <utility>
#include <vector>

namespace NKikimr::NDetailedMetrics {

// A single tablet, either a leader or a follower. An empty bucket key identifies
// the TABLE partial; a nonempty key identifies a PARTITION leaf.
using TTabletKey = std::pair<ui64, ui32>;
using TBucketKey = TMaybe<TTabletKey>;
using TSubgroupPath = std::vector<std::pair<TString, TString>>;

inline const TString DATABASE_LABEL = "database";
inline const TString TABLE_LABEL = "table";
inline const TString DETAILED_METRICS_LABEL = "detailed_metrics";
inline const TString TABLET_ID_LABEL = "tablet_id";
inline const TString FOLLOWER_ID_LABEL = "follower_id";
inline const TString PER_PARTITION_VALUE = "per_partition";

// The low level tablet counters use the same layout as the "tablets" group.
inline const TString TYPE_LABEL = "type";
inline const TString CATEGORY_LABEL = "category";
inline const TString EXECUTOR_CATEGORY = "executor";
inline const TString APP_CATEGORY = "app";

inline TStringBuf ChopTrailingSlash(TStringBuf path) {
    path.ChopSuffix("/");
    return path;
}

// Reports carry absolute table paths; counter trees use database-relative paths.
// databasePrefix must already have its trailing slash removed. The returned view
// aliases tablePath, including when it is outside the database and stays intact.
inline TStringBuf MakeRelativeTablePath(const TStringBuf databasePrefix, const TString& tablePath) {
    TStringBuf relativePath(tablePath);
    // Require the separator: /Root/db10/table is not inside /Root/db1.
    if (relativePath.SkipPrefix(databasePrefix) && relativePath.SkipPrefix("/") && !relativePath.empty()) {
        return relativePath;
    }
    return TStringBuf(tablePath);
}

inline NMonitoring::TDynamicCounterPtr GetOrCreatePerPartitionGroup(
    NMonitoring::TDynamicCounterPtr tableGroup)
{
    return tableGroup->GetSubgroup(DETAILED_METRICS_LABEL, PER_PARTITION_VALUE);
}

inline NMonitoring::TDynamicCounterPtr GetOrCreateTabletGroup(
    NMonitoring::TDynamicCounterPtr parentGroup, const TTabletKey& tablet)
{
    return parentGroup->GetSubgroup(TABLET_ID_LABEL, ToString(tablet.first))
        ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(tablet.second));
}

inline NMonitoring::TDynamicCounterPtr GetOrCreateTypeGroup(
    NMonitoring::TDynamicCounterPtr bucketGroup, TTabletTypes::EType tabletType)
{
    return bucketGroup->GetSubgroup(TYPE_LABEL, TTabletTypes::TypeToStr(tabletType));
}

inline TSubgroupPath MakeTabletPath(const TTabletKey& tablet, TSubgroupPath prefix = {}) {
    prefix.emplace_back(TABLET_ID_LABEL, ToString(tablet.first));
    prefix.emplace_back(FOLLOWER_ID_LABEL, ToString(tablet.second));
    return prefix;
}

// Callers choose the removal root: the node prepends database/table to prune
// empty ancestors across its shared leader/follower tree; the processor starts
// at the table and manages table ownership separately.
inline TSubgroupPath MakeRawBucketPath(
    const TBucketKey& key, TTabletTypes::EType tabletType, TSubgroupPath prefix = {})
{
    if (key) {
        prefix.emplace_back(DETAILED_METRICS_LABEL, PER_PARTITION_VALUE);
        return MakeTabletPath(*key, std::move(prefix));
    }
    prefix.emplace_back(TYPE_LABEL, TTabletTypes::TypeToStr(tabletType));
    return prefix;
}

} // namespace NKikimr::NDetailedMetrics
