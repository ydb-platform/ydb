#include "ydb_control_plane_storage_impl.h"

#include <cstdint>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>

#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/schema.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>
#include <ydb/core/yq/libs/quota_manager/quota_manager.h>

#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/digest/multi.h>

namespace NYq {

struct TCloudAggregator {
    TAtomic Started = 0;
    TAtomic Finished = 0;
};

void TYdbControlPlaneStorageActor::Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev)
{
    if (ev->Get()->SubjectType != SUBJECT_TYPE_CLOUD || ev->Get()->MetricName != QUOTA_COUNT_LIMIT) {
        Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->MetricName, 0));
    }

    if (QuotasUpdatedAt + Config.QuotaTtl > Now()) {
        Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, ev->Get()->SubjectId, QUOTA_COUNT_LIMIT, QueryQuotas.Value(ev->Get()->SubjectId, 0)));
    }

    QueryQuotaRequests[ev->Get()->SubjectId] = ev;

    if (QuotasUpdating) {
        return;
    }

    QuotasUpdating = true;
    QuotaGeneration++;
    QueryQuotas.clear();

    TRequestCountersPtr requestCounters = Counters.GetCommonCounters(RTS_QUOTA_USAGE);

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "CountPendingQueries");

    queryBuilder.AddText(
        "SELECT `" SCOPE_COLUMN_NAME "`, COUNT(`" SCOPE_COLUMN_NAME "`) AS PENDING_COUNT\n"
        "FROM `" PENDING_SMALL_TABLE_NAME "`\n"
        "GROUP BY `" SCOPE_COLUMN_NAME "`\n"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);

    auto aggregator = std::make_shared<TCloudAggregator>();

    result.Apply([=, resultSets=resultSets, generation=QuotaGeneration](const auto& future) {
        try {
            TStatus status = future.GetValue();
            if (status.IsSuccess() && resultSets->size() == 1) {
                TResultSetParser parser(resultSets->front());
                while (parser.TryNextRow()) {
                    auto scope = *parser.ColumnParser(SCOPE_COLUMN_NAME).GetOptionalString();
                    auto count = parser.ColumnParser("PENDING_COUNT").GetUint64();
                    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "GetQueryCloudId");

                    queryBuilder.AddText(
                        "SELECT `" INTERNAL_COLUMN_NAME "`\n"
                        "FROM `" QUERIES_TABLE_NAME "`\n"
                        "WHERE `" SCOPE_COLUMN_NAME "` = $scope LIMIT 1;\n"
                    );

                    queryBuilder.AddString("scope", scope);

                    const auto query = queryBuilder.Build();
                    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};

                    AtomicIncrement(aggregator->Started);
                    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);

                    result.Apply([=, resultSets=resultSets](const auto& future) {
                        try {
                            TStatus status = future.GetValue();
                            if (status.IsSuccess() && resultSets->size() == 1) {
                                TResultSetParser parser(resultSets->front());
                                if (parser.TryNextRow()) {
                                    YandexQuery::Internal::QueryInternal internal;
                                    if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                                        ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
                                    }
                                    Send(SelfId(), new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, internal.cloud_id(), QUOTA_COUNT_LIMIT, count), 0, generation);
                                }
                            }
                            AtomicIncrement(aggregator->Finished);
                            if (AtomicGet(aggregator->Started) == AtomicGet(aggregator->Finished)) {
                                Send(SelfId(), new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, "", QUOTA_COUNT_LIMIT, 0), 0, generation);
                            }
                        } catch (...) {
                            // Cerr << "EX2 " << CurrentExceptionMessage() << Endl;
                        }
                    });
                }
                if (AtomicGet(aggregator->Started) == 0) {
                    Send(SelfId(), new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, "", QUOTA_COUNT_LIMIT, 0), 0, generation);
                }
            }
        } catch (...) {
            // Cerr << "EX1 " << CurrentExceptionMessage() << Endl;
        }
    });
}

void TYdbControlPlaneStorageActor::Handle(TEvQuotaService::TQuotaUsageResponse::TPtr& ev)
{
    if (ev->Cookie == QuotaGeneration) {
        auto subjectId = ev->Get()->SubjectId;
        if (subjectId == "") {
            QuotasUpdatedAt = Now();
            for (auto& it : QueryQuotaRequests) {
                auto ev = it.second;
                Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, it.first, QUOTA_COUNT_LIMIT, QueryQuotas.Value(it.first, 0)));
            }
            QueryQuotaRequests.clear();
            QuotasUpdating = false;
        } else {
            QueryQuotas[subjectId] += ev->Get()->Usage;
        }
    }
}

} // NYq
