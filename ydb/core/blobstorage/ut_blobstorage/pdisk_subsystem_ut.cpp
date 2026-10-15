#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(PDiskSubsystem) {

    template<bool Write>
    void TestMetadataTimeout() {
        TEnvironmentSetup env({.NodeCount = 1});
        auto& runtime = env.Runtime;
        const ui32 nodeId = *runtime->GetNodes().begin();
        const auto edge = runtime->AllocateEdgeActor(nodeId);
        const auto warden = MakeBlobStorageNodeWardenID(nodeId);
        const TString path = "/mock/pdisk-metadata-timeout";
        constexpr ui64 cookie = 741;
        constexpr ui32 pdiskResultType = Write ? NPDisk::TEvWriteMetadataResult::EventType
                                              : NPDisk::TEvReadMetadataResult::EventType;
        constexpr ui32 resultType = Write ? NStorage::TEvNodeWardenWriteMetadataResult::EventType
                                         : NStorage::TEvNodeWardenReadMetadataResult::EventType;
        std::unique_ptr<IEventHandle> delayed;
        bool delayReply = true;
        ui32 replies = 0;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (delayReply && event->GetTypeRewrite() == pdiskResultType) {
                UNIT_ASSERT(!delayed);
                delayed = std::move(event);
                return false;
            }
            if (event->GetTypeRewrite() == resultType && event->Recipient == edge) {
                ++replies;
            }
            return true;
        };
        NKikimrBlobStorage::TPDiskMetadataRecord record;
        record.MutableCommittedStorageConfig()->SetGeneration(42);
        if constexpr (Write) {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenWriteMetadata(path, record), 0, cookie), nodeId);
        } else {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenReadMetadata(path), 0, cookie), nodeId);
        }
        using TResult = std::conditional_t<Write, NStorage::TEvNodeWardenWriteMetadataResult,
            NStorage::TEvNodeWardenReadMetadataResult>;
        auto response = env.WaitForEdgeActorEvent<TResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(35));
        UNIT_ASSERT_C(response, "Metadata timeout did not return a result");
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::ERROR);
        UNIT_ASSERT(!response->Get()->Guid);
        UNIT_ASSERT(delayed);
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);

        // A reply arriving after timeout must not produce a second completion.
        delayReply = false;
        runtime->Send(delayed.release(), nodeId);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        runtime->FilterFunction = nullptr;

        // NodeWarden remains usable, and the metadata PDisk reference was released.
        const size_t disksCreated = env.PDiskActors.size();
        runtime->Send(new IEventHandle(warden, edge,
            new NStorage::TEvNodeWardenWriteMetadata(path, record)), nodeId);
        auto written = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenWriteMetadataResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(written);
        UNIT_ASSERT(written->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(env.PDiskActors.size(), disksCreated + 1);
        runtime->Send(new IEventHandle(warden, edge, new NStorage::TEvNodeWardenReadMetadata(path)), nodeId);
        auto read = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(read);
        UNIT_ASSERT(read->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.SerializeAsString(), record.SerializeAsString());
    }

    Y_UNIT_TEST(ReadMetadataTimeoutReturnsError) {
        TestMetadataTimeout<false>();
    }

    Y_UNIT_TEST(WriteMetadataTimeoutReturnsError) {
        TestMetadataTimeout<true>();
    }
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
