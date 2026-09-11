#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(PDiskSubsystem) {
    Y_UNIT_TEST(MetadataSurvivesNodeRestart) {
        ui32 registrations = 0;
        TEnvironmentSetup env({
            .NodeCount = 1,
            .PrepareRuntime = [&registrations](TTestActorSystem& runtime) {
                auto configure = std::move(runtime.SetupNodeSubSystems);
                runtime.SetupNodeSubSystems = [configure = std::move(configure), &registrations](
                        ui32 nodeId, TActorSystemSetup* setup) {
                    ++registrations;
                    configure(nodeId, setup);
                };
            },
        });
        UNIT_ASSERT_VALUES_EQUAL(registrations, 1);
        const ui32 nodeId = *env.Runtime->GetNodes().begin();
        const TString path = "/mock/pdisk-subsystem-metadata";
        const TActorId wardenId = MakeBlobStorageNodeWardenID(nodeId);
        NKikimrBlobStorage::TPDiskMetadataRecord record;
        record.MutableCommittedStorageConfig()->SetGeneration(42);

        auto edge = env.Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->Send(new IEventHandle(wardenId, edge,
            new NStorage::TEvNodeWardenWriteMetadata(path, record)), nodeId);
        auto written = env.Runtime->WaitForEdgeActorEvent<NStorage::TEvNodeWardenWriteMetadataResult>(edge);
        UNIT_ASSERT(written->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);

        const auto states = env.PDiskMockStates;
        UNIT_ASSERT(!states.empty());
        env.RestartNode(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(registrations, 2);

        edge = env.Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->Send(new IEventHandle(wardenId, edge,
            new NStorage::TEvNodeWardenReadMetadata(path)), nodeId);
        auto read = env.Runtime->WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge);
        UNIT_ASSERT(read->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.SerializeAsString(), record.SerializeAsString());
        for (const auto& [key, state] : states) {
            UNIT_ASSERT(env.PDiskMockStates.at(key) == state);
        }
    }
}
