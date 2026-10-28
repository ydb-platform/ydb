#pragma once

#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NYql::NGatewayTest {

inline constexpr const char* TestCluster = "kikimr";

inline TIntrusivePtr<NKikimr::NKqp::IKqpGateway> GetIcGateway(NKikimr::Tests::TServer& server) {
    auto counters = MakeIntrusive<NKikimr::NKqp::TKqpRequestCounters>();
    counters->Counters = new NKikimr::NKqp::TKqpCounters(server.GetRuntime()->GetAppData(/* nodeIndex */ 0).Counters);
    counters->TxProxyMon = new NKikimr::NTxProxy::TTxProxyMon(server.GetRuntime()->GetAppData(/* nodeIndex */ 0).Counters);

    auto loader = std::make_shared<NKikimr::NKqp::TKqpTableMetadataLoader>(TestCluster,
        server.GetRuntime()->GetAnyNodeActorSystem(), TIntrusivePtr<TKikimrConfiguration>(), /* needCollectSchemeData */ false);
    return NKikimr::NKqp::CreateKikimrIcGateway(TestCluster, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY, "/Root", "/Root", std::move(loader), server.GetRuntime()->GetAnyNodeActorSystem(),
        server.GetRuntime()->GetNodeId(/* index */ 0), counters, server.GetSettings().AppConfig->GetQueryServiceConfig());
}

inline THolder<NKikimr::NSchemeCache::TSchemeCacheNavigate> DoGatewayOperation(NKikimr::TTestActorRuntime& runtime, const TString& path, std::function<NThreading::TFuture<IKikimrGateway::TGenericResult>()> gatewayOperation, bool fail = false) {
    const auto& responseFuture = gatewayOperation();
    responseFuture.Wait();
    const auto& response = responseFuture.GetValue();
    response.Issues().PrintTo(Cerr);

    if (fail) {
        UNIT_ASSERT_C(!response.Success(), response.Issues().ToString());
        return nullptr;
    }

    UNIT_ASSERT_C(response.Success(), response.Issues().ToString());
    return NKikimr::NKqp::Navigate(runtime, runtime.AllocateEdgeActor(), path, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
}

inline NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry TestCreateObjectCommon(NKikimr::TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TCreateObjectSettings& settings, const TString& path) {
    return DoGatewayOperation(runtime, path, [gateway, settings]() {
        return gateway->CreateObject(TestCluster, settings);
    })->ResultSet.at(/* pos */ 0);
}

inline NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry TestAlterObjectCommon(NKikimr::TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TAlterObjectSettings& settings, const TString& path) {
    return DoGatewayOperation(runtime, path, [gateway, settings]() {
        return gateway->AlterObject(TestCluster, settings);
    })->ResultSet.at(/* pos */ 0);
}

inline void TestDropObjectCommon(NKikimr::TTestActorRuntime& runtime, TIntrusivePtr<IKikimrGateway> gateway, const TDropObjectSettings& settings, const TString& path) {
    const auto objectDescription = DoGatewayOperation(runtime, path, [gateway, settings]() {
        return gateway->DropObject(TestCluster, settings);
    });
    const auto& object = objectDescription->ResultSet.at(/* pos */ 0);

    UNIT_ASSERT_VALUES_EQUAL(objectDescription->ErrorCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(object.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
}

} // namespace NYql::NGatewayTest
