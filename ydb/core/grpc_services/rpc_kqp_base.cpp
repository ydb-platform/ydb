#include "rpc_kqp_base.h"

#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr {
namespace NGRpcService {



Ydb::Table::QueryStatsCollection::Mode GetCollectStatsMode(Ydb::Query::StatsMode mode) {
    switch (mode) {
        case Ydb::Query::StatsMode::STATS_MODE_NONE:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        case Ydb::Query::StatsMode::STATS_MODE_BASIC:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC;
        case Ydb::Query::StatsMode::STATS_MODE_FULL:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL;
        case Ydb::Query::StatsMode::STATS_MODE_PROFILE:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE;
        default:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_UNSPECIFIED;
    }
}

} // namespace NGRpcService
} // namespace NKikimr
