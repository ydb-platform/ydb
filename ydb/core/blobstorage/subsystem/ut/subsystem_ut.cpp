#include <ydb/core/blobstorage/subsystem/mock/mock.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/library/actors/core/actorsystem.h>
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
}
