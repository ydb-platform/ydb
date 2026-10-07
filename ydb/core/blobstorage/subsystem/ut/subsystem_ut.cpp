#include <ydb/core/blobstorage/subsystem/mock/mock.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TBlobStorageSubsystemTest) {
    Y_UNIT_TEST(MockRegistersGroupsWithoutNodeWarden) {
        NActors::TActorSystemSetup setup;
        setup.NodeId = 42;
        TVector<TIntrusivePtr<NFake::TProxyDS>> groups{
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(1)),
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(2)),
        };
        InstallBlobStorageSubsystem(setup, CreateMockBlobStorageSubsystem(groups, 3));
        UNIT_ASSERT(NActors::GetSubSystem<IBlobStorageSubsystem>(setup.SubSystems));
        UNIT_ASSERT_VALUES_EQUAL(setup.LocalServices.size(), 2);
        for (size_t i = 0; i < groups.size(); ++i) {
            UNIT_ASSERT(setup.LocalServices[i].first == MakeBlobStorageProxyID(groups[i]->GetGroupId()));
            UNIT_ASSERT(setup.LocalServices[i].first != MakeBlobStorageNodeWardenID(setup.NodeId));
            UNIT_ASSERT_VALUES_EQUAL(setup.LocalServices[i].second.PoolId, 3);
            UNIT_ASSERT(setup.LocalServices[i].second.Actor);
        }
    }
    Y_UNIT_TEST(MockDataSurvivesActorSystemRecreation) {
        const auto group = MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(1));
        const TLogoBlobID id(100, 1, 1, 0, 4, 0);
        for (ui32 incarnation = 0; incarnation < 2; ++incarnation) {
            NActors::TTestActorRuntime runtime;
            runtime.SetupNodeSubSystems = [group](ui32, NActors::TActorSystemSetup* setup) {
                InstallBlobStorageSubsystem(*setup, CreateMockBlobStorageSubsystem({group}));
            };
            runtime.Initialize(TAppPrepare(TAppPrepare::TLightweightTag{}).Unwrap());
            const auto edge = runtime.AllocateEdgeActor();
            const auto proxy = MakeBlobStorageProxyID(group->GetGroupId());
            UNIT_ASSERT(runtime.FindActor(proxy, ui32{0}));
            UNIT_ASSERT(!runtime.GetActorSystem(0)->LookupLocalService(
                MakeBlobStorageNodeWardenID(runtime.GetNodeId(0))));
            if (incarnation == 0) {
                runtime.Send(proxy, edge, new TEvBlobStorage::TEvPut(id, TString("data"), TInstant::Max()));
                const auto result = runtime.GrabEdgeEvent<TEvBlobStorage::TEvPutResult>();
                UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
            }
            runtime.Send(proxy, edge, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
            const auto result = runtime.GrabEdgeEvent<TEvBlobStorage::TEvGetResult>();
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Responses[0].Buffer.ConvertToString(), "data");
        }
    }

}
