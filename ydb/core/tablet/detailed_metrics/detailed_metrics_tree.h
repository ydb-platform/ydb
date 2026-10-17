#pragma once

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <utility>
#include <vector>

namespace NKikimr::NDetailedMetrics {

    // A single tablet, either a leader or a follower. An empty bucket key identifies
    // the TABLE partial; a nonempty key identifies a PARTITION leaf.
    using TTabletKey = std::pair<ui64, ui32>;
    using TBucketKey = TMaybe<TTabletKey>;
    using TSubgroupPath = std::vector<std::pair<TString, TString>>;

    extern const TString DATABASE_LABEL;
    extern const TString TABLE_LABEL;
    extern const TString DETAILED_METRICS_LABEL;
    extern const TString TABLET_ID_LABEL;
    extern const TString FOLLOWER_ID_LABEL;
    extern const TString PER_PARTITION_VALUE;

    // The low level tablet counters use the same layout as the "tablets" group.
    extern const TString TYPE_LABEL;
    extern const TString CATEGORY_LABEL;
    extern const TString EXECUTOR_CATEGORY;
    extern const TString APP_CATEGORY;

    TStringBuf ChopTrailingSlash(TStringBuf path);

    // Reports carry absolute table paths; counter trees use database-relative paths.
    // databasePrefix must already have its trailing slash removed. The returned view
    // aliases tablePath, including when it is outside the database and stays intact.
    TStringBuf MakeRelativeTablePath(const TStringBuf databasePrefix, const TString& tablePath);

    NMonitoring::TDynamicCounterPtr GetOrCreatePerPartitionGroup(
        NMonitoring::TDynamicCounterPtr tableGroup);

    NMonitoring::TDynamicCounterPtr GetOrCreateTabletGroup(
        NMonitoring::TDynamicCounterPtr parentGroup, const TTabletKey& tablet);

    NMonitoring::TDynamicCounterPtr GetOrCreateTypeGroup(
        NMonitoring::TDynamicCounterPtr bucketGroup, TTabletTypes::EType tabletType);

    TSubgroupPath MakeTabletPath(const TTabletKey& tablet, TSubgroupPath prefix = {});

    // Callers choose the removal root: the node prepends database/table to prune
    // empty ancestors across its shared leader/follower tree; the processor starts
    // at the table and manages table ownership separately.
    TSubgroupPath MakeRawBucketPath(
        const TBucketKey& key, TTabletTypes::EType tabletType, TSubgroupPath prefix = {});

} // namespace NKikimr::NDetailedMetrics
