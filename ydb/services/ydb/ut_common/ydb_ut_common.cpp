#include "ydb_ut_common.h"

#include <grpcpp/client_context.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <yql/essentials/core/issue/yql_issue.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace Tests;

Ydb::Table::ExecuteQueryResult ExecYql(
    std::shared_ptr<grpc::Channel> channel,
    const TString& sessionId,
    const TString& yql,
    bool withStat)
{
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    Ydb::Table::ExecuteDataQueryRequest request;
    request.set_session_id(sessionId);
    request.mutable_query()->set_yql_text(yql);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);
    if (withStat) {
        request.set_collect_stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC);
    }
    Ydb::Table::ExecuteDataQueryResponse response;
    auto status = stub->ExecuteDataQuery(&context, request, &response);
    UNIT_ASSERT(status.ok());
    auto deferred = response.operation();
    UNIT_ASSERT(deferred.ready() == true);
    NYql::TIssues issues;
    NYql::IssuesFromMessage(deferred.issues(), issues);
    issues.PrintTo(Cerr);

    UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Table::ExecuteQueryResult result;
    Y_ABORT_UNLESS(deferred.result().UnpackTo(&result));
    return result;
}

TString CreateSession(std::shared_ptr<grpc::Channel> channel) {
    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    Ydb::Table::CreateSessionRequest request;
    Ydb::Table::CreateSessionResponse response;

    auto status = stub->CreateSession(&context, request, &response);
    auto deferred = response.operation();
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(deferred.ready() == true);
    Ydb::Table::CreateSessionResult result;

    deferred.result().UnpackTo(&result);
    return result.session_id();
}

TStoragePools CreatePoolsForTenant(
    Tests::TClient& client,
    const TDomainsInfo::TDomain::TStoragePoolKinds& poolTypes,
    const TString& tenant)
{
    TStoragePools result;
    for (auto& poolType : poolTypes) {
        auto& poolKind = poolType.first;
        result.emplace_back(client.CreateStoragePool(poolKind, tenant), poolKind);
    }
    return result;
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclarationSetting(const TString& name) {
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    return subdomain;
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSetting(const TString& name, const TStoragePools& pools) {
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    subdomain.SetCoordinators(1);
    subdomain.SetMediators(1);
    subdomain.SetPlanResolution(10);
    subdomain.SetTimeCastBucketsPerMediator(2);
    for (auto& pool : pools) {
        *subdomain.AddStoragePools() = pool;
    }
    return subdomain;
}

} // namespace NKikimr
