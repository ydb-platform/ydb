#include "ydb_test_bootstrap.h"

#include <util/datetime/base.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

constexpr auto SUBJECT_TYPE_GROUP = "group";
constexpr auto SUBJECT_TYPE_FAMILY = "family";

constexpr auto QUOTA_APPLE_COUNT   = "apple.count";
constexpr auto QUOTA_APPLE_SIZE    = "apple.size";
constexpr auto QUOTA_BANANA_COUNT  = "banana.count";
constexpr auto QUOTA_BANANA_LENGTH = "banana.length";
constexpr auto QUOTA_CHERRY_COUNT  = "cherry.count";
constexpr auto QUOTA_CHERRY_WEIGHT = "cherry.weight";

class TUsageHandlerMock : public NActors::TActorBootstrapped<TUsageHandlerMock> {
public:
    void Bootstrap() {
        Become(&TUsageHandlerMock::StateFunc);
    }
private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaUsageRequest, Handle)
    );

    void Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev)
    {
        auto& request = *ev->Get();

        if (request.SubjectId == "on") {
            ControlMark = true;
        }

        if (request.SubjectId == "off") {
            ControlMark = false;
        }

        if (request.MetricName == QUOTA_APPLE_SIZE) {
            Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(request.SubjectType, request.SubjectId, request.MetricName, Size++));
            return;
        }

        if (ControlMark && request.MetricName == QUOTA_CHERRY_WEIGHT) {
            Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(request.SubjectType, request.SubjectId, request.MetricName, Weight++));
            return;
        }
    }

    bool ControlMark = false;
    ui64 Size = 7;
    ui64 Weight = 2;
};

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageQuotas) {

    Y_UNIT_TEST(GetDefaultQuotas)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        NConfig::TQuotasManagerConfig cfg;
        auto* desc = cfg.AddQuotaDescriptions();
        desc->SetSubjectType(SUBJECT_TYPE_FAMILY);
        desc->SetMetricName(QUOTA_CHERRY_COUNT);
        desc->SetDefaultLimit(42);

        auto quotaServiceActor = bootstrap.Runtime->Register(NFq::CreateQuotaServiceActor(
            cfg,
            bootstrap.Config.GetStorage(),
            bootstrap.YqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            {
                TQuotaDescription(SUBJECT_TYPE_GROUP,  QUOTA_APPLE_COUNT,  10),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_BANANA_COUNT, 20),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_CHERRY_COUNT, 30)
            }));
        bootstrap.Runtime->RegisterService(NFq::MakeQuotaServiceActorId(0), quotaServiceActor);

        TActorId sender = bootstrap.Runtime->AllocateEdgeActor();
        bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id")));

        TAutoPtr<IEventHandle> handle;
        NFq::TEvQuotaService::TQuotaGetResponse* event = bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL(event->Quotas.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_BANANA_COUNT)->second.Limit.Value, 20);
        UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_CHERRY_COUNT)->second.Limit.Value, 42);
    }

    Y_UNIT_TEST(OverrideQuotas)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        NConfig::TQuotasManagerConfig config;

        {
            auto& quotas = *config.AddQuotas();
            quotas.SetSubjectType(SUBJECT_TYPE_FAMILY);
            quotas.SetSubjectId("id1");
            auto& limit = *quotas.AddLimit();
            limit.SetName(QUOTA_BANANA_COUNT);
            limit.SetLimit(15);
        }

        {
            auto& quotas = *config.AddQuotas();
            quotas.SetSubjectType(SUBJECT_TYPE_FAMILY);
            quotas.SetSubjectId("id2");
            auto& limit = *quotas.AddLimit();
            limit.SetName(QUOTA_BANANA_LENGTH);
            limit.SetLimit(150);
        }

        auto quotaServiceActor = bootstrap.Runtime->Register(NFq::CreateQuotaServiceActor(
            config,
            bootstrap.Config.GetStorage(),
            bootstrap.YqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            {
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_BANANA_COUNT, 20),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_BANANA_LENGTH, 100)
            }));
        bootstrap.Runtime->RegisterService(NFq::MakeQuotaServiceActorId(0), quotaServiceActor);

        TActorId sender = bootstrap.Runtime->AllocateEdgeActor();
        bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "id2")));

        TAutoPtr<IEventHandle> handle;
        NFq::TEvQuotaService::TQuotaGetResponse* event = bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL(event->Quotas.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_BANANA_COUNT)->second.Limit.Value, 20);
        UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_BANANA_LENGTH)->second.Limit.Value, 150);
    }

    Y_UNIT_TEST(GetStaleUsage)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        auto usageUpdateActor = bootstrap.Runtime->Register(new TUsageHandlerMock());

        NConfig::TQuotasManagerConfig config;
        config.SetUsageRefreshPeriod("1d");

        auto quotaServiceActor = bootstrap.Runtime->Register(NFq::CreateQuotaServiceActor(
            config,
            bootstrap.Config.GetStorage(),
            bootstrap.YqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            {
                TQuotaDescription(SUBJECT_TYPE_GROUP, QUOTA_APPLE_SIZE, 100, 0, usageUpdateActor),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_APPLE_SIZE, 100, 0, usageUpdateActor),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_CHERRY_WEIGHT, 100, 0, usageUpdateActor)
            }));
        bootstrap.Runtime->RegisterService(NFq::MakeQuotaServiceActorId(0), quotaServiceActor);

        TActorId sender = bootstrap.Runtime->AllocateEdgeActor();

        {
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id", true)));

            TAutoPtr<IEventHandle> handle;
            NFq::TEvQuotaService::TQuotaGetResponse* event = bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);

            UNIT_ASSERT_VALUES_EQUAL(event->Quotas.size(), 2);
            UNIT_ASSERT(!event->Quotas.find(QUOTA_APPLE_SIZE)->second.Usage);
            UNIT_ASSERT(!event->Quotas.find(QUOTA_CHERRY_WEIGHT)->second.Usage);
        }
        {
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id")));
            // reply is not expected
        }
        while (true) { // wait for reply for QUOTA_APPLE_SIZE
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id", true)));

            TAutoPtr<IEventHandle> handle;
            NFq::TEvQuotaService::TQuotaGetResponse* event = bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);

            if (event->Quotas.find(QUOTA_APPLE_SIZE)->second.Usage){
                break;
            }
        }
        {
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id", true)));

            TAutoPtr<IEventHandle> handle;
            NFq::TEvQuotaService::TQuotaGetResponse* event = bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);

            UNIT_ASSERT_VALUES_EQUAL(event->Quotas.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_APPLE_SIZE)->second.Usage->Value, 7);
            UNIT_ASSERT(!event->Quotas.find(QUOTA_CHERRY_WEIGHT)->second.Usage);
        }
    }

    Y_UNIT_TEST(PushUsageUpdate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        auto usageUpdateActor = bootstrap.Runtime->Register(new TUsageHandlerMock());

        auto quotaServiceActor = bootstrap.Runtime->Register(NFq::CreateQuotaServiceActor(
            NConfig::TQuotasManagerConfig{},
            bootstrap.Config.GetStorage(),
            bootstrap.YqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            {
                TQuotaDescription(SUBJECT_TYPE_GROUP, QUOTA_APPLE_SIZE, 100, 0, usageUpdateActor),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_APPLE_SIZE, 100, 0, usageUpdateActor),
                TQuotaDescription(SUBJECT_TYPE_FAMILY, QUOTA_CHERRY_WEIGHT, 100, 0, usageUpdateActor)
            }));
        bootstrap.Runtime->RegisterService(NFq::MakeQuotaServiceActorId(0), quotaServiceActor);

        TActorId sender = bootstrap.Runtime->AllocateEdgeActor();

        {
            // load metric in cache
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id", true)));
            TAutoPtr<IEventHandle> handle;
            bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);
        }

        {
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), sender, new NFq::TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_FAMILY, "some_id")));
            bootstrap.Runtime->Send(new IEventHandle(NFq::MakeQuotaServiceActorId(0), {}, new TEvQuotaService::TQuotaUsageResponse(SUBJECT_TYPE_FAMILY, "some_id", QUOTA_CHERRY_WEIGHT, 1000)));

            TAutoPtr<IEventHandle> handle;
            NFq::TEvQuotaService::TQuotaGetResponse* event = bootstrap.Runtime->GrabEdgeEvent<NFq::TEvQuotaService::TQuotaGetResponse>(handle);

            UNIT_ASSERT_VALUES_EQUAL(event->Quotas.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_APPLE_SIZE)->second.Usage->Value, 7);
            UNIT_ASSERT_VALUES_EQUAL(event->Quotas.find(QUOTA_CHERRY_WEIGHT)->second.Usage->Value, 1000);
        }

    }

}

} // NFq
