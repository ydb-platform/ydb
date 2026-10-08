#include <ydb/core/testlib/basics/core/setup.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/subsystem/mock/mock.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;

namespace {

void CheckMockRuntime(bool realThreads) {
    auto model = MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(1));
    // Models stay outside both runtimes; each node receives fresh actors.
    for (ui32 restart = 0; restart != 2; ++restart) {
        TTestTabletRuntime runtime(2, realThreads);
        ui32 previousCalls = 0;
        runtime.SetupNodeSubSystems = [&previousCalls](ui32, TActorSystemSetup*) {
            ++previousCalls;
        };
        TVector<ui32> configuredNodes;
        ConfigureBlobStorage(runtime, [&](ui32 nodeIndex) {
            configuredNodes.push_back(nodeIndex);
            return CreateMockBlobStorageSubsystem({model});
        });
        runtime.Initialize({new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
            nullptr, nullptr, {}, {}});

        UNIT_ASSERT_VALUES_EQUAL(previousCalls, 2);
        UNIT_ASSERT_VALUES_EQUAL(configuredNodes.size(), 2);
        for (ui32 node = 0; node != 2; ++node) {
            UNIT_ASSERT_VALUES_EQUAL(configuredNodes[node], node);
            auto* system = runtime.GetActorSystem(node);
            UNIT_ASSERT(system->GetSubSystem<IBlobStorageSubsystem>());
            UNIT_ASSERT(system->LookupLocalService(MakeBlobStorageProxyID(model->GetGroupId())));
            UNIT_ASSERT(!system->LookupLocalService(MakeBlobStorageNodeWardenID(runtime.GetNodeId(node))));
            UNIT_ASSERT(system->LookupLocalService(GetNameserviceActorId()));
        }
        UNIT_ASSERT(runtime.GetActorSystem(0)->GetSubSystem<IBlobStorageSubsystem>() !=
            runtime.GetActorSystem(1)->GetSubSystem<IBlobStorageSubsystem>());
    }
}

}

Y_UNIT_TEST_SUITE(LightweightTabletRuntime) {
    Y_UNIT_TEST(MockSubsystemSimulated) {
        CheckMockRuntime(false);
    }

    Y_UNIT_TEST(MockSubsystemRealThreads) {
        CheckMockRuntime(true);
    }
}
