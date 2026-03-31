#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <yql/essentials/core/yql_statistics.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

struct TColumnStatisticsResponse: public NYql::IKikimrGateway::TGenericResult {
    // Uses NYql:: type because it feeds into TypeCtx.ColumnStatisticsByTableName
    // which is typed THashMap<TString, TIntrusivePtr<NYql::TOptimizerStatistics::TColumnStatMap>>.
    THashMap<TString, NYql::TOptimizerStatistics::TColumnStatMap> ColumnStatisticsByTableName;
};

void AddStatRequest(TActorSystem* actorSystem, TVector<NThreading::TFuture<TColumnStatisticsResponse>>& futures, TKikimrTablesData& tables,
                    const TString& cluster, const TString& database, TTypeAnnotationContext& typesCtx, const NKikimr::NStat::EStatType type,
                    const THashMap<TString, THashSet<TString>>& columnsByTableName, std::function<bool(const NYql::TColumnStatistics&)> alreadyHasStatistics);

} // namespace NKikimr::NKqp