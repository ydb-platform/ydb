#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

namespace {

    struct TSlowPDiskState {
        ui32 Starts = 0;
        ui32 CompletedStarts = 0;
        ui32 Stops = 0;
    };

    // Model a PDisk that cannot service metadata until its startup scan finishes.
    // Destroying this actor loses its pending startup, just as stopping the worker does.
    class TSlowMetadataPDisk : public TActorBootstrapped<TSlowMetadataPDisk> {
        TSlowPDiskState& State;
        bool Ready = false;
        std::vector<std::unique_ptr<IEventHandle>> Pending;

        void Reply(IEventHandle& ev) {
            if (ev.GetTypeRewrite() == NPDisk::TEvReadMetadata::EventType) {
                Send(ev.Sender, new NPDisk::TEvReadMetadataResult(NPDisk::EPDiskMetadataOutcome::NO_METADATA,
                    42), 0, ev.Cookie);
            } else {
                Send(ev.Sender, new NPDisk::TEvWriteMetadataResult(NPDisk::EPDiskMetadataOutcome::OK, 42),
                    0, ev.Cookie);
            }
        }

        void HandleMetadata(TAutoPtr<IEventHandle>& ev) {
            if (Ready) {
                Reply(*ev);
            } else {
                Pending.emplace_back(ev.Release());
            }
        }

        void FinishStartup() {
            Ready = true;
            ++State.CompletedStarts;
            for (const auto& ev : Pending) {
                Reply(*ev);
            }
            Pending.clear();
        }

        void Stop() {
            ++State.Stops;
            PassAway();
        }

    public:
        explicit TSlowMetadataPDisk(TSlowPDiskState& state)
            : State(state)
        {}

        void Bootstrap() {
            ++State.Starts;
            Become(&TThis::StateFunc);
            Schedule(TDuration::Seconds(90), new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc,
            fFunc(NPDisk::TEvReadMetadata::EventType, HandleMetadata);
            fFunc(NPDisk::TEvWriteMetadata::EventType, HandleMetadata);
            cFunc(TEvents::TSystem::Wakeup, FinishStartup);
            cFunc(TEvents::TSystem::Poison, Stop);
        )
    };

    class TSlowPDiskServiceFactory : public IPDiskServiceFactory {
        TSlowPDiskState& State;

    public:
        explicit TSlowPDiskServiceFactory(TSlowPDiskState& state)
            : State(state)
        {}

        void Create(const TActorContext& ctx, ui32 pdiskId, const TIntrusivePtr<TPDiskConfig>&,
                const NPDisk::TMainKey&, ui32 poolId, ui32 nodeId) override {
            const auto actor = ctx.Register(new TSlowMetadataPDisk(State), TMailboxType::HTSwap, poolId);
            ctx.ActorSystem()->RegisterLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId), actor);
        }
    };

    template<bool Write>
    void CheckSlowMetadataStartup() {
        TSlowPDiskState state;
        TEnvironmentSetup env({.NodeCount = 1, .ConfigPreprocessor = [&](ui32, TNodeWardenConfig& config) {
            config.PDiskServiceFactory.Reset(new TSlowPDiskServiceFactory(state));
        }});
        auto& runtime = env.Runtime;
        const ui32 nodeId = *runtime->GetNodes().begin();
        const auto edge = runtime->AllocateEdgeActor(nodeId);
        const auto warden = MakeBlobStorageNodeWardenID(nodeId);
        const TString path = "/mock/slow-starting-pdisk";
        constexpr ui64 cookie = 817;
        using TResult = std::conditional_t<Write, NStorage::TEvNodeWardenWriteMetadataResult,
            NStorage::TEvNodeWardenReadMetadataResult>;
        ui32 replies = 0;
        bool deliverReplies = false;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Recipient == edge && ev->GetTypeRewrite() == TResult::EventType) {
                ++replies;
                return deliverReplies;
            }
            return true;
        };
        if constexpr (Write) {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenWriteMetadata(path, {}), 0, cookie), nodeId);
        } else {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenReadMetadata(path), 0, cookie), nodeId);
        }
        env.Sim(TDuration::Seconds(65));
        UNIT_ASSERT_VALUES_EQUAL(state.Starts, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.CompletedStarts, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(state.Stops, 0, "Metadata timeout destroyed a PDisk during startup");
        UNIT_ASSERT_VALUES_EQUAL(replies, 0);
        deliverReplies = true;
        auto response = env.WaitForEdgeActorEvent<TResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "The original startup did not finish");
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Get()->Outcome == (Write ? NPDisk::EPDiskMetadataOutcome::OK
            : NPDisk::EPDiskMetadataOutcome::NO_METADATA));
        UNIT_ASSERT(response->Get()->Guid == 42);
        UNIT_ASSERT_VALUES_EQUAL(state.Starts, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.CompletedStarts, 1);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.Stops, 1);
        runtime->FilterFunction = nullptr;
    }

} // namespace

Y_UNIT_TEST_SUITE(PDiskSubsystem) {

    Y_UNIT_TEST(ReadMetadataPreservesSlowPDiskStartup) {
        CheckSlowMetadataStartup<false>();
    }

    Y_UNIT_TEST(WriteMetadataPreservesSlowPDiskStartup) {
        CheckSlowMetadataStartup<true>();
    }


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
        bool deliverReplies = false;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (delayReply && event->GetTypeRewrite() == pdiskResultType) {
                UNIT_ASSERT(!delayed);
                delayed = std::move(event);
                return false;
            }
            if (event->GetTypeRewrite() == resultType && event->Recipient == edge) {
                ++replies;
                return deliverReplies;
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
        env.Sim(TDuration::Seconds(65));
        UNIT_ASSERT(delayed);
        UNIT_ASSERT_VALUES_EQUAL_C(replies, 0, "A slow metadata operation must remain pending");

        using TPDiskResult = std::conditional_t<Write, NPDisk::TEvWriteMetadataResult,
            NPDisk::TEvReadMetadataResult>;
        const auto expectedGuid = delayed->Get<TPDiskResult>()->PDiskGuid;
        delayReply = false;
        runtime->Send(delayed.release(), nodeId);
        deliverReplies = true;
        auto response = env.WaitForEdgeActorEvent<TResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Get()->Outcome == (Write ? NPDisk::EPDiskMetadataOutcome::OK
            : NPDisk::EPDiskMetadataOutcome::NO_METADATA));
        UNIT_ASSERT(response->Get()->Guid == expectedGuid);
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

    Y_UNIT_TEST(ReadMetadataTimeoutWaitsForCompletion) {
        TestMetadataTimeout<false>();
    }

    Y_UNIT_TEST(WriteMetadataTimeoutWaitsForCompletion) {
        TestMetadataTimeout<true>();
    }
}
