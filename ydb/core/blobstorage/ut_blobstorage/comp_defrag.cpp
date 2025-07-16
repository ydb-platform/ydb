#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_search.h>

#include <library/cpp/protobuf/util/pb_io.h>

constexpr ui64 CHUNK_SIZE = 128 * 1024 * 1024;
constexpr ui64 MIN_HUGE_BLOB_SIZE = 512 * 1024; // 512 KiB

struct TLsmMetrics {
    ui64 CompactionBytesRead = 0;
    ui64 CompactionBytesWritten = 0;

    ui64 LevelFresh = 0;
    ui64 Level1 = 0;
    ui64 Level2 = 0;
    ui64 Level3 = 0;

    ui64 HugeUsedChunks = 0;
    ui64 HugeChunksCanBeFreed = 0;
    ui64 DskSpaceCurInplacedData = 0;
};

struct TTetsEnv {
    TTetsEnv()
    : Env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .MinHugeBlobInBytes = MIN_HUGE_BLOB_SIZE,
    })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        DataSmall = FastGenDataForLZ4(128 * 1024, 0);
        DataLarge = FastGenDataForLZ4(3 * 1024 * 1024, 0);

        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Env.Invoke(request);
        const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->GroupID.GetRawId(), baseConfig.GetGroup(0).GetGroupId());
        PdiskLayout = MakePDiskLayout(baseConfig, GroupInfo->GetTopology(), baseConfig.GetGroup(0).GetGroupId());
    }

    NMonitoring::TDynamicCounterPtr GetCounters(ui32 nodeId) {
        return Env.Runtime->GetNode(nodeId)->AppData.get()->Counters;
    }

    std::unique_ptr<TEvBlobStorage::TEvPut> GetData(ui32 index) const {
        ui32 tabletId = index % 2 + 1;
        auto& data = index % 100 < 10 ? DataLarge : DataSmall;
        auto id = TLogoBlobID(tabletId, 1, index, 0, data.size(), 0);
        return std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
    }

    template<class TEvent>
    void SendToDsProxy(TEvent* event) {
        Env.Runtime->WrapInActorContext(Sender, [&] {
            SendToBSProxy(Sender, GroupInfo->GroupID, event);
        });
    }

    ui64 AggregateVDiskCounters(const TString& subsystem, const TString& counter, std::unordered_map<TString, TString> labels = {}) {
        return Env.AggregateVDiskCounters(
            "test", Env.Settings.NodeCount,
            GroupInfo->GetTotalVDisksNum(), GroupInfo->GroupID.GetRawId(),
            PdiskLayout, subsystem, counter, labels
        );
    }

    TLsmMetrics GetLsmMetrics() {
        TLsmMetrics metrics;

        metrics.CompactionBytesRead = AggregateVDiskCounters("lsmhull", "LsmCompactionBytesRead");
        metrics.CompactionBytesWritten = AggregateVDiskCounters("lsmhull", "LsmCompactionBytesWritten");

        metrics.LevelFresh = AggregateVDiskCounters("levels", "NumItems", {{"level", "0"}});
        metrics.Level1 = AggregateVDiskCounters("levels", "NumItems", {{"level", "1..8"}})
                        + AggregateVDiskCounters("levels", "NumItems", {{"level", "9..16"}});
        metrics.Level2 = AggregateVDiskCounters("levels", "NumItems", {{"level", "17"}});
        metrics.Level3 = AggregateVDiskCounters("levels", "NumItems", {{"level", "18"}});

        metrics.HugeUsedChunks = AggregateVDiskCounters("outofspace", "HugeUsedChunks");
        metrics.HugeChunksCanBeFreed = AggregateVDiskCounters("outofspace", "HugeChunksCanBeFreed");
        metrics.DskSpaceCurInplacedData = AggregateVDiskCounters("outofspace", "DskSpaceCurInplacedData");

        return metrics;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    TString DataSmall, DataLarge;
    ui32 CollectGeneration = 0;
    TActorId Sender;
    std::vector<ui32> PdiskLayout;
};

#define UNIT_ASSERT_VALUE_IN(min, value, max) \
    UNIT_ASSERT_C((min) <= (value) && (value) <= (max), \
        "Value " << value << " is not in range [" << min << ", " << max << "]")

struct TEvDefragStartQuantum : TEventLocal<TEvDefragStartQuantum, TEvBlobStorage::EvDefragStartQuantum> {
    NKikimr::TChunksToDefrag ChunksToDefrag;
};

Y_UNIT_TEST_SUITE(CompDefrag) {

    Y_UNIT_TEST(Write) {
        TTetsEnv env;
        ui32 N = 50000;
        ui32 batchSize = 1000;

        ui64 bytesWrittenSmall = 0, bytesWrittenLarge = 0;
        std::unordered_set<std::pair<ui32, ui32>> seenParts;
        env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVPut: {
                    auto put = ev->Get<TEvBlobStorage::TEvVPut>();
                    auto id = LogoBlobIDFromLogoBlobID(put->Record.GetBlobID());
                    if (!seenParts.insert({id.Step(), id.PartId()}).second) {
                        break; // skip duplicates
                    }
                    ui64 bytes = put->GetBufferBytes();
                    if (bytes >= MIN_HUGE_BLOB_SIZE) {
                        bytesWrittenLarge += bytes;
                    } else {
                        bytesWrittenSmall += bytes;
                    }
                    break;
                }
                case TEvBlobStorage::EvDefragStartQuantum: {
                    auto defragStart = ev->Get<TEvDefragStartQuantum>();
                    if (!defragStart->ChunksToDefrag.Chunks.empty()) {
                        Cerr << "Defrag start quantum: " << defragStart->ChunksToDefrag.ToString() << Endl;
                    }
                    break;
                }
                case TEvBlobStorage::EvCompactVDisk: {
                    auto compact = ev->Get<TEvCompactVDisk>();
                    Cerr << "Compact VDisk: " << compact->ModeToString(compact->Mode) << Endl;
                    break;
                }
                case TEvBlobStorage::EvCompactVDiskResult: {
                    Cerr << "EvCompactVDiskResult" << Endl;
                    break;
                }
            }
            return true;
        };

        auto write = [&](ui32 index) {
            env.SendToDsProxy(env.GetData(index).release());
        };
        auto waitForWriteResult = [&]() {
            auto res = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(env.Sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        };

        // warm up ds proxy and bs queue
        write(0);
        waitForWriteResult();

        // write data in batches
        for (ui32 i = 0; i < N / batchSize; ++i) {
            for (ui32 j = 0; j < batchSize; ++j) {
                write(i * batchSize + j);
            }
            for (ui32 j = 0; j < batchSize; ++j) {
                waitForWriteResult();
            }
        }

        // run full compaction
        env.Env.Sim(TDuration::Seconds(20));
        for (ui32 i = 0; i < env.GroupInfo->GetTotalVDisksNum(); ++i) {
            const TActorId& vdiskId = env.GroupInfo->GetActorId(i);
            env.Env.CompactVDisk(vdiskId);
        }
        env.Env.Sim(TDuration::Seconds(20));

        // check metrics
        auto metrics = env.GetLsmMetrics();
        Cerr << "Bytes written (small): " << bytesWrittenSmall << Endl
            << "Bytes written (large): " << bytesWrittenLarge << Endl
            << "Compaction bytes read: " << metrics.CompactionBytesRead << Endl
            << "Compaction bytes written: " << metrics.CompactionBytesWritten << Endl
            << "Level Fresh: " << metrics.LevelFresh << Endl
            << "Level 1: " << metrics.Level1 << Endl
            << "Level 2: " << metrics.Level2 << Endl
            << "Level 3: " << metrics.Level3 << Endl
            << "Huge Used Chunks: " << metrics.HugeUsedChunks << Endl
            << "Huge Chunks Can Be Freed: " << metrics.HugeChunksCanBeFreed << Endl
            << "Dsk Space Cur Inplaced Data: " << metrics.DskSpaceCurInplacedData << Endl
            << "=========================================================" << Endl;

        UNIT_ASSERT_VALUE_IN(N * 6, metrics.LevelFresh + metrics.Level1 + metrics.Level2 + metrics.Level3, N * 8);
        UNIT_ASSERT_VALUE_IN(N * 6, metrics.Level2, N * 8); // everything should be on level 2
        UNIT_ASSERT_VALUE_IN(bytesWrittenLarge / CHUNK_SIZE, metrics.HugeUsedChunks, std::ceil(bytesWrittenLarge * 1.125 / CHUNK_SIZE));


        // remove huge blobs from one of the tablets
        ui32 tabletIdToRemoveHuge = 1; // remove huge blobs from tablet 1
        auto keep = std::make_unique<TVector<TLogoBlobID>>();
        for (ui32 i = 0; i < N; ++i) {
            auto ev = env.GetData(i);
            if (ev->Id.TabletID() == tabletIdToRemoveHuge && ev->Buffer.size() < MIN_HUGE_BLOB_SIZE * 4) {
                keep->push_back(ev->Id);
            }
        }
        env.Env.Runtime->WrapInActorContext(env.Sender, [&] {
            SendToBSProxy(
                env.Sender, env.GroupInfo->GroupID,
                new TEvBlobStorage::TEvCollectGarbage(
                    tabletIdToRemoveHuge, 1, 1, 
                    0, true, 1, Max<ui32>(),
                    keep.release(), nullptr, TInstant::Max(), true
                )
            );
        });
        const auto& res = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        env.Env.Sim(TDuration::Minutes(100)); // defrag scheduler runs every 5-5.5 minutes


        // check metrics
        metrics = env.GetLsmMetrics();
        Cerr << "Bytes written (small): " << bytesWrittenSmall << Endl
            << "Bytes written (large): " << bytesWrittenLarge << Endl
            << "Compaction bytes read: " << metrics.CompactionBytesRead << Endl
            << "Compaction bytes written: " << metrics.CompactionBytesWritten << Endl
            << "Level Fresh: " << metrics.LevelFresh << Endl
            << "Level 1: " << metrics.Level1 << Endl
            << "Level 2: " << metrics.Level2 << Endl
            << "Level 3: " << metrics.Level3 << Endl
            << "Huge Used Chunks: " << metrics.HugeUsedChunks << Endl
            << "Huge Chunks Can Be Freed: " << metrics.HugeChunksCanBeFreed << Endl
            << "Dsk Space Cur Inplaced Data: " << metrics.DskSpaceCurInplacedData << Endl
            << "=========================================================" << Endl;
    }

}
