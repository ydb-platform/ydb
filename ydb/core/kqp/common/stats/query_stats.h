#pragma once

namespace Ydb::TableStats {
class QueryStats;
}

namespace NKqpProto {
class TKqpStatsQuery;
}

namespace NKikimrKqp {
class TQueryResponse;
}

namespace NKikimr::NKqp {

void FillQueryStats(Ydb::TableStats::QueryStats& queryStats, const NKqpProto::TKqpStatsQuery& kqpStats);
void FillQueryStats(Ydb::TableStats::QueryStats& queryStats, const NKikimrKqp::TQueryResponse& kqpResponse);

} // namespace NKikimr::NKqp
