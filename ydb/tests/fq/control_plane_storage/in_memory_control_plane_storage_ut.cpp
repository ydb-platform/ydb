#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRuntimePtr = std::unique_ptr<TTestActorRuntime>;

struct TTestBootstrap {
    NConfig::TControlPlaneStorageConfig Config;
    TRuntimePtr Runtime;

    const TDuration RequestTimeout = TDuration::Seconds(10);

    TTestBootstrap()
    {
        Runtime = PrepareTestActorRuntime();
    }

    std::pair<FederatedQuery::CreateQueryResult, NYql::TIssues> CreateQuery()
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::CreateQueryRequest proto;

        FederatedQuery::QueryContent& content = *proto.mutable_content();
        content.set_name("my_query_1");
        content.set_text("SELECT 1;");
        content.set_type(FederatedQuery::QueryContent::ANALYTICS);
        content.mutable_acl()->set_visibility(FederatedQuery::Acl::SCOPE);

        auto request = std::make_unique<TEvControlPlaneStorage::TEvCreateQueryRequest>("yandexcloud://my_cloud_1/my_folder_1", proto, "user@staff", "", "mock_cloud", TPermissions{}, TQuotaMap{}, std::make_shared<TTenantInfo>(), FederatedQuery::Internal::ComputeDatabaseInternal{});
        Runtime->Send(new IEventHandle(ControlPlaneStorageServiceActorId(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        TEvControlPlaneStorage::TEvCreateQueryResponse* event = Runtime->GrabEdgeEvent<TEvControlPlaneStorage::TEvCreateQueryResponse>(handle);
        return {event->Result, event->Issues};
    }

private:
    TRuntimePtr PrepareTestActorRuntime() {
        TRuntimePtr runtime(new TTestBasicRuntime(1));
        runtime->SetLogPriority(NKikimrServices::YQ_CONTROL_PLANE_STORAGE, NLog::PRI_DEBUG);

        auto controlPlaneProxy = CreateInMemoryControlPlaneStorageServiceActor(Config);

        runtime->AddLocalService(
            ControlPlaneStorageServiceActorId(),
            TActorSetupCmd(controlPlaneProxy, TMailboxType::Simple, 0));

        SetupTabletServices(*runtime);

        return runtime;
    }
};

}

Y_UNIT_TEST_SUITE(CreateQueryRequest) {
    Y_UNIT_TEST(ShouldCreateSimpleQuery)
    {
        TTestBootstrap bootstrap;
        const auto [result, issues] = bootstrap.CreateQuery();
        UNIT_ASSERT(!issues);
    }
}


} // NFq
