#include "ydb_control_plane_storage_impl.h"

#include <cstdint>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>

#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <util/digest/multi.h>

#include <ydb/core/fq/libs/shared_resources/db_exec.h>

namespace NFq {


void TYdbControlPlaneStorageActor::Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev) {

    if (ev->Get()->SubjectType != SUBJECT_TYPE_CLOUD
        || (ev->Get()->MetricName != QUOTA_ANALYTICS_COUNT_LIMIT && ev->Get()->MetricName != QUOTA_STREAMING_COUNT_LIMIT) ) {
        Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->MetricName, 0));
    }

    if (QuotasUpdatedAt + Config->QuotaTtl > Now()) {
        ui64 usage = 0;
        auto quotaIt = this->QueryQuotas.find(ev->Get()->SubjectId);
        if (quotaIt != this->QueryQuotas.end()) {
            auto queryType = ev->Get()->MetricName == QUOTA_ANALYTICS_COUNT_LIMIT
                ? FederatedQuery::QueryContent::QueryType::QueryContent_QueryType_ANALYTICS
                : FederatedQuery::QueryContent::QueryType::QueryContent_QueryType_STREAMING;
            usage = quotaIt->second[queryType];
        }
        Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, ev->Get()->SubjectId, ev->Get()->MetricName, usage));
    }

    QueryQuotaRequests[ev->Get()->SubjectId] = ev;

    if (QuotasUpdating) {
        return;
    }

    QuotasUpdating = true;
    QueryQuotas.clear();

    using TQuotaCountExecuter = TDbExecuter<TQueryQuotasMap>;

    TDbExecutable::TPtr executable;
    auto& executer = TQuotaCountExecuter::Create(executable, false, [](TQuotaCountExecuter& executer) { executer.State.clear(); } );

    executer.Read(
        [=](TQuotaCountExecuter&, TSqlQueryBuilder& builder) {
            builder.AddText(
                "SELECT `" SCOPE_COLUMN_NAME "`, `" QUERY_TYPE_COLUMN_NAME "` AS QUERY_TYPE, COUNT(`" SCOPE_COLUMN_NAME "`) AS PENDING_COUNT\n"
                "FROM `" PENDING_SMALL_TABLE_NAME "`\n"
                "GROUP BY `" QUERY_TYPE_COLUMN_NAME "`, `" SCOPE_COLUMN_NAME "`\n"
            );
        },
        [=](TQuotaCountExecuter& executer, const std::vector<NYdb::TResultSet>& resultSets) {
            TResultSetParser parser(resultSets.front());
            while (parser.TryNextRow()) {
                auto scope = *parser.ColumnParser(SCOPE_COLUMN_NAME).GetOptionalString();
                auto queryType = static_cast<FederatedQuery::QueryContent::QueryType>(parser.ColumnParser("QUERY_TYPE").GetOptionalInt64().value_or(1));
                auto count = parser.ColumnParser("PENDING_COUNT").GetUint64();
                executer.Read(
                    [=](TQuotaCountExecuter&, TSqlQueryBuilder& builder) {
                        builder.AddText(
                            "SELECT `" INTERNAL_COLUMN_NAME "`\n"
                            "FROM `" QUERIES_TABLE_NAME "`\n"
                            "WHERE `" SCOPE_COLUMN_NAME "` = $scope LIMIT 1;\n"
                        );
                        builder.AddString("scope", TString{scope});
                    },
                    [=](TQuotaCountExecuter& executer, const std::vector<NYdb::TResultSet>& resultSets) {
                        TResultSetParser parser(resultSets.front());
                        if (parser.TryNextRow()) {
                            FederatedQuery::Internal::QueryInternal internal;
                            ParseProto(executer, internal, parser, INTERNAL_COLUMN_NAME);
                            executer.State[internal.cloud_id()][queryType] += count;
                        }
                    },
                    "GetScopeCloud_" + scope, true
                );
            }
        },
        "GroupByPendingSmall", true
    ).Process(SelfId(),
        [=, this](TQuotaCountExecuter& executer) {
            this->QuotasUpdatedAt = Now();
            this->QueryQuotas.swap(executer.State);
            for (auto& it : this->QueryQuotaRequests) {
                auto ev = it.second;
                ui64 analyticCount = 0;
                ui64 streamingCount = 0;
                auto quotaIt = this->QueryQuotas.find(it.first);
                if (quotaIt != this->QueryQuotas.end()) {
                    analyticCount = quotaIt->second[FederatedQuery::QueryContent::QueryType::QueryContent_QueryType_ANALYTICS];
                    streamingCount = quotaIt->second[FederatedQuery::QueryContent::QueryType::QueryContent_QueryType_STREAMING];
                }
                this->Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, it.first, QUOTA_ANALYTICS_COUNT_LIMIT, analyticCount));
                this->Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, it.first, QUOTA_STREAMING_COUNT_LIMIT, streamingCount));
            }
            this->QueryQuotaRequests.clear();
            this->QuotasUpdating = false;
        }
    );

    Exec(DbPool, executable, TablePathPrefix).Apply([=, this, actorSystem=NActors::TActivationContext::ActorSystem(), selfId=SelfId()](const auto& future) {
        actorSystem->Send(selfId, new TEvents::TEvCallback([this, executable, future]() {
            auto issues = GetIssuesFromYdbStatus(executable, future);
            if (issues) {
                for (auto& it : this->QueryQuotaRequests) {
                    auto ev = it.second;
                    this->Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, it.first, QUOTA_ANALYTICS_COUNT_LIMIT, *issues));
                    this->Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_CLOUD, it.first, QUOTA_STREAMING_COUNT_LIMIT, *issues));
                }
                this->QueryQuotaRequests.clear();
                this->QuotasUpdating = false;
            }
        }));
    });
}

void TYdbControlPlaneStorageActor::Handle(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev) {
    auto& record = *ev->Get();
    // LimitRequested is always accepted
    Send(ev->Sender, new TEvQuotaService::TQuotaLimitChangeResponse(record.SubjectType, record.SubjectId, record.MetricName,
      record.LimitRequested, record.LimitRequested));
}

} // NFq
