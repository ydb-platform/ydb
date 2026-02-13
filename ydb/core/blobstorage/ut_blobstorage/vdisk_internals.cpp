#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/core/blobstorage/vdisk/chunk_keeper/chunk_keeper_events.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

#include <util/random/random.h>
#include <util/stream/null.h>

#define Ctest Cnull

namespace NKikimr {

Y_UNIT_TEST_SUITE(ChunkKeeper) {

struct TTestCtx : public TTestCtxBase {
    using TSubsystem = NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem;

    TTestCtx()
        : TTestCtxBase(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        })
        , Seed(RandomNumber<ui32>())
        , Rng(Seed)
    {
        Ctest << "Seed# " << Seed << Endl;
    }

    ui32 Random(ui32 min, ui32 max) {
        return std::uniform_int_distribution<ui32>(min, max - 1)(Rng);
    }

#define ADD_TO_HISTORY(msg) {               \
    TString cmd = TStringBuilder() << msg;  \
    Ctest << " --- " << cmd << Endl;        \
    History << cmd << ", "; }

    void Run() {
        UpdateChunkKeeperId();

        constexpr ui32 steps = 1000;
        for (ui32 step = 0; step < steps; ++step) {
            ui32 action = Random(0, 4);

            switch (action) {
            case 0:
                AllocateChunk();
                break;
            case 1:
                if (TotalAllocated > 0) {
                    FreeChunk();
                    break;
                }
                [[fallthrough]];
            case 2:
                Discover();
                break;
            case 3:
                RestartVDisk();
                break;
            }
        }
    }

    void AllocateChunk() {
        TSubsystem subsystem = RandomSubsystem();
        Env->Runtime->WrapInActorContext(Edge, [&]{
            auto ev = std::make_unique<TEvChunkKeeperAllocate>(subsystem);
            TActivationContext::Send(new IEventHandle(ChunkKeeperId, Edge, ev.release()));
        });
        ADD_TO_HISTORY("TEvChunkKeeperAllocate(" << NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem_Name(subsystem) << ")");
        auto resHandle = Env->WaitForEdgeActorEvent<TEvChunkKeeperAllocateResult>(Edge, false);
        UNIT_ASSERT_C(resHandle, History.Str());
        auto* res = resHandle->Get();
        switch (res->Status) {
        case NKikimrProto::OK:
            UNIT_ASSERT_C(res->ChunkIdx, History.Str());
            UNIT_ASSERT_VALUES_EQUAL_C(Chunks[subsystem].count(*res->ChunkIdx), 0, History.Str());
            Chunks[subsystem].insert(*res->ChunkIdx);
            ADD_TO_HISTORY("TEvChunkKeeperAllocateResult(" << *res->ChunkIdx << ")");
            ++TotalAllocated;
            break;
        case NKikimrProto::ERROR:
            // should always be able to allocate at least one chunk
            UNIT_ASSERT_C(TotalAllocated > 0, res->ErrorReason << " History# " << History.Str());
            break;
        default:
            // unexpected status
            UNIT_FAIL(res->ErrorReason);
            break;
        }
    }

    void FreeChunk() {
        Y_ABORT_UNLESS(TotalAllocated);
        TSubsystem subsystem = RandomSubsystem();
        while (Chunks[subsystem].empty()) {
            subsystem = RandomSubsystem();
        }

        ui32 pos = Random(0, Chunks[subsystem].size());
        auto it = Chunks[subsystem].begin();
        std::advance(it, pos);
        ui32 chunkIdx = *it;

        Env->Runtime->WrapInActorContext(Edge, [&]{
            auto ev = std::make_unique<TEvChunkKeeperFree>(chunkIdx, subsystem);
            TActivationContext::Send(new IEventHandle(ChunkKeeperId, Edge, ev.release()));
        });
        ADD_TO_HISTORY("TEvChunkKeeperFree(" << chunkIdx << ","
                << NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem_Name(subsystem) << ")");

        auto resHandle = Env->WaitForEdgeActorEvent<TEvChunkKeeperFreeResult>(Edge, false);
        UNIT_ASSERT_C(resHandle, History.Str());
        auto* res = resHandle->Get();
        switch (res->Status) {
        case NKikimrProto::OK:
            UNIT_ASSERT_VALUES_EQUAL_C(res->ChunkIdx, chunkIdx, History.Str());
            Chunks[subsystem].erase(it);
            ADD_TO_HISTORY("TEvChunkKeeperFreeResult(" << chunkIdx << ")");
            --TotalAllocated;
            break;
        default:
            UNIT_FAIL(res->ErrorReason << " History# " << History.Str());
            break;
        }
    }

    void Discover() {
        TSubsystem subsystem = RandomSubsystem();
        Env->Runtime->WrapInActorContext(Edge, [&]{
            auto ev = std::make_unique<TEvChunkKeeperDiscover>(subsystem);
            TActivationContext::Send(new IEventHandle(ChunkKeeperId, Edge, ev.release()));
        });
        ADD_TO_HISTORY("TEvChunkKeeperDiscover(" << NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem_Name(subsystem) << ")");

        auto resHandle = Env->WaitForEdgeActorEvent<TEvChunkKeeperDiscoverResult>(Edge, false);
        UNIT_ASSERT_C(resHandle, History.Str());
        auto* res = resHandle->Get();
        
        ADD_TO_HISTORY("TEvChunkKeeperDiscoverResult(" << PrintSorted(res->Chunks) << ") Actual# " << PrintSorted(Chunks[subsystem]));
        UNIT_ASSERT_C(Compare(res->Chunks, Chunks[subsystem]), History.Str());
    }

    void UpdateChunkKeeperId() {
        TIntrusivePtr<TBlobStorageGroupInfo> groupInfo = Env->GetGroupInfo(GroupId);
        TActorId skeletonFront = groupInfo->GetActorId(0);
        AllocateEdgeActorOnSpecificNode(skeletonFront.NodeId());
        Env->Runtime->WrapInActorContext(Edge, [&]{
            auto ev = std::make_unique<IEventHandle>(skeletonFront, Edge, new TEvGetSkeletonState);
            ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, skeletonFront);
            TActivationContext::Send(ev.release());
        });
        auto res = Env->WaitForEdgeActorEvent<TEvGetSkeletonStateResult>(Edge, false);
        UNIT_ASSERT(res);
        ChunkKeeperId = res->Get()->ChunkKeeperActorId;
    }

    void RestartVDisk() {
        ADD_TO_HISTORY("Restarting VDisk");
        Env->StopNode(ChunkKeeperId.NodeId());
        Env->StartNode(ChunkKeeperId.NodeId());
        Env->Sim(TDuration::Seconds(5));
        AllocateEdgeActorOnSpecificNode(ChunkKeeperId.NodeId());
        GetGroupStatus(GroupId);
        UpdateChunkKeeperId();
    }

    template<class T>
    TString PrintSorted(const T& container) {
        auto set = std::set<typename T::value_type>(container.begin(), container.end());
        TStringStream str;
        str << "{";
        for (auto it = set.begin(); it != set.end(); ++it) {
            str << *it << ",";
        }
        str << "}";
        return str.Str();
    }

    template<class T1, class T2>
    bool Compare(const T1& set1, const T2& set2) {
        return PrintSorted(set1) == PrintSorted(set2);
    }

    TSubsystem RandomSubsystem() {
        return static_cast<TSubsystem>(Random(1, 3));
    }

    std::unordered_map<TSubsystem, std::unordered_set<ui32>> Chunks;
    TActorId ChunkKeeperId;

    ui32 TotalAllocated = 0;

    TStringStream History;

    ui32 Seed;
    std::mt19937 Rng;

#undef ADD_TO_HISTORY
};

Y_UNIT_TEST(Random) {
    TTestCtx ctx;
    ctx.Initialize();
    ctx.Run();
}

}

}
