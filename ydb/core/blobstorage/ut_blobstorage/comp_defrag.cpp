#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_search.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_quantum.h>

#include <library/cpp/protobuf/util/pb_io.h>

constexpr ui64 CHUNK_SIZE = 32_MB;
constexpr ui64 MIN_HUGE_BLOB_SIZE = 128_KB;
constexpr ui64 MAX_DEFRAG_INFLIGHT = 2;

struct TLsmMetrics {
    ui64 CompactionBytesRead = 0;
    ui64 CompactionBytesWritten = 0;

    ui64 Level0 = 0;
    ui64 Level1 = 0;
    ui64 Level2 = 0;
    ui64 Level3 = 0;

    ui64 HugeUsedChunks = 0;
    ui64 HugeChunksCanBeFreed = 0;
    ui64 DskSpaceCurInplacedData = 0;

    ui64 DskUsedBytes = 0;
    ui64 DskTotalBytes = 0;
};

struct TTestEnv {

    TTestEnv(double defragThresholdToRunCompaction)
    : Env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .DiskType = NPDisk::EDeviceType::DEVICE_TYPE_ROT,
        .MinHugeBlobInBytes = MIN_HUGE_BLOB_SIZE,
        // .UseFakeConfigDispatcher = true,
        .PDiskSize = 20_GB,
        .PDiskChunkSize = CHUNK_SIZE,
    })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        DataSmall = FastGenDataForLZ4(32_KB, 0);
        DataLarge = FastGenDataForLZ4(1_MB, 0);

        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Env.Invoke(request);
        const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->GroupID.GetRawId(), baseConfig.GetGroup(0).GetGroupId());
        PdiskLayout = MakePDiskLayout(baseConfig, GroupInfo->GetTopology(), baseConfig.GetGroup(0).GetGroupId());

        for (ui32 i = 1; i <= Env.Settings.NodeCount; ++i) {
            Env.SetIcbControl(i, "VDiskControls.MaxChunksToDefragInflight", MAX_DEFRAG_INFLIGHT);
            Env.SetIcbControl(i, "VDiskControls.DefaultHugeGarbagePerMille", 50);
            Env.SetIcbControl(i, "VDiskControls.DefragThresholdToRunCompactionPerMille", defragThresholdToRunCompaction * 1000);
        }
        Env.Sim(TDuration::Minutes(1));

        Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            auto eventType = ev->GetTypeRewrite();
            auto it = Filters.find(eventType);
            if (it != Filters.end()) {
                return it->second(nodeId, ev);
            }
            return true;
        };

        SetFilterFunction(TEvBlobStorage::EvVPut, [this](ui32, std::unique_ptr<IEventHandle>& ev) {
            auto put = ev->Get<TEvBlobStorage::TEvVPut>();
            auto id = LogoBlobIDFromLogoBlobID(put->Record.GetBlobID());
            if (!SeenParts.insert({id.Step(), id.PartId()}).second) {
                return true; // skip duplicates;
            }
            ui64 bytes = put->GetBufferBytes();
            if (bytes >= MIN_HUGE_BLOB_SIZE) {
                BytesWrittenLarge += bytes;
            } else {
                BytesWrittenSmall += bytes;
            }
            return true;
        });
        SetFilterFunction(TEvBlobStorage::EvCompactVDisk, [this](ui32 nodeId, std::unique_ptr<IEventHandle>&) {
            CompactionsPerNode[nodeId]++;
            return true;
        });
        SetFilterFunction(NKikimr::TEvDefragQuantumResult::EventType, [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            auto res = ev->Get<NKikimr::TEvDefragQuantumResult>();
            ChunksFreedByDefragPerNode[nodeId] += res->Stat.FreedChunks.size();
            return true;
        });
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

        metrics.Level0 = AggregateVDiskCounters("levels", "NumItems", {{"level", "0"}});
        metrics.Level1 = AggregateVDiskCounters("levels", "NumItems", {{"level", "1..8"}})
                        + AggregateVDiskCounters("levels", "NumItems", {{"level", "9..16"}});
        metrics.Level2 = AggregateVDiskCounters("levels", "NumItems", {{"level", "17"}});
        metrics.Level3 = AggregateVDiskCounters("levels", "NumItems", {{"level", "18"}});

        metrics.HugeUsedChunks = AggregateVDiskCounters("outofspace", "HugeUsedChunks");
        metrics.HugeChunksCanBeFreed = AggregateVDiskCounters("outofspace", "HugeChunksCanBeFreed");
        metrics.DskSpaceCurInplacedData = AggregateVDiskCounters("outofspace", "DskSpaceCurInplacedData");

        metrics.DskUsedBytes = AggregateVDiskCounters("outofspace", "DskUsedBytes");
        metrics.DskTotalBytes = AggregateVDiskCounters("outofspace", "DskTotalBytes");

        return metrics;
    }

    std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> SetFilterFunction(ui32 eventType, std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> func) {
        std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> oldFunc = Filters[eventType];
        Filters[eventType] = std::move(func);
        return oldFunc;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    TString DataSmall, DataLarge;
    ui32 CollectGeneration = 0;
    TActorId Sender;
    std::vector<ui32> PdiskLayout;
    std::unordered_map<ui32, std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>> Filters;

    ui64 BytesWrittenSmall = 0, BytesWrittenLarge = 0;
    std::unordered_map<ui32, ui32> CompactionsPerNode;
    std::unordered_map<ui32, ui32> ChunksFreedByDefragPerNode;
    std::unordered_set<std::pair<ui32, ui32>> SeenParts;
};

TLsmMetrics PrintMetrics(TTestEnv& env) {
    auto metrics = env.GetLsmMetrics();
    Cerr << "Compaction bytes read: " << metrics.CompactionBytesRead << Endl
         << "Compaction bytes written: " << metrics.CompactionBytesWritten << Endl
         << "Level 0: " << metrics.Level0 << Endl
         << "Level 1: " << metrics.Level1 << Endl
         << "Level 2: " << metrics.Level2 << Endl
         << "Level 3: " << metrics.Level3 << Endl
         << "Huge Used Chunks: " << metrics.HugeUsedChunks << Endl
         << "Huge Chunks Can Be Freed: " << metrics.HugeChunksCanBeFreed << Endl
         << "Dsk Space Cur Inplaced Data: " << metrics.DskSpaceCurInplacedData << Endl
         << "Dsk Used Bytes: " << metrics.DskUsedBytes << Endl
         << "Dsk Total Bytes: " << metrics.DskTotalBytes << Endl
         << "=========================================================" << Endl;
    return metrics;
}

void WriteData(TTestEnv& env, ui32 N, ui32 batchSize) {
    // warm up ds proxy and bs queue
    env.SendToDsProxy(env.GetData(0).release());
    auto res = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(env.Sender, false);
    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

    // write data in batches
    for (ui32 i = 0; i < N / batchSize; ++i) {
        for (ui32 j = 0; j < batchSize; ++j) {
            env.SendToDsProxy(env.GetData(i * batchSize + j).release());
        }
        for (ui32 j = 0; j < batchSize; ++j) {
            auto res = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(env.Sender, false);
            if (res->Get()->Status != NKikimrProto::OK) {
                PrintMetrics(env);
            }
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
    }
}

void RunFullCompaction(TTestEnv& env) {
    Cerr << "Running full compaction" << Endl;
    env.Env.Sim(TDuration::Seconds(20));
    for (ui32 i = 0; i < env.GroupInfo->GetTotalVDisksNum(); ++i) {
        const TActorId& vdiskId = env.GroupInfo->GetActorId(i);
        env.Env.CompactVDisk(vdiskId);
    }
    env.Env.Sim(TDuration::Seconds(20));
}

void DeleteHugeBlobsOfTablet(TTestEnv& env, ui32 N, ui32 tabletId) {
    auto keep = std::make_unique<TVector<TLogoBlobID>>();
    for (ui32 i = 0; i < N; ++i) {
        auto ev = env.GetData(i);
        if (ev->Id.TabletID() == tabletId && ev->Buffer.size() < MIN_HUGE_BLOB_SIZE * 4) {
            keep->push_back(ev->Id);
        }
    }
    env.Env.Runtime->WrapInActorContext(env.Sender, [&] {
        SendToBSProxy(
            env.Sender, env.GroupInfo->GroupID,
            new TEvBlobStorage::TEvCollectGarbage(
                tabletId, 1, 1,
                0, true, 1, Max<ui32>(),
                keep.release(), nullptr, TInstant::Max(), true
            )
        );
    });
    const auto& res = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(env.Sender);
    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
}

#define UNIT_ASSERT_VALUE_IN(min, value, max) \
    UNIT_ASSERT_C((min) <= (value) && (value) <= (max), \
        "Value " << value << " is not in range [" << min << ", " << max << "]")

struct TEvDefragStartQuantum : TEventLocal<TEvDefragStartQuantum, TEvBlobStorage::EvDefragStartQuantum> {
    NKikimr::TChunksToDefrag ChunksToDefrag;
};

Y_UNIT_TEST_SUITE(CompDefrag) {

    Y_UNIT_TEST(DoesItWork) {
        TTestEnv env(0.01);
        ui32 N = 50000;
        ui32 batchSize = 1000;

        WriteData(env, N, batchSize);
        RunFullCompaction(env);
        ui64 compactionBytesWritten = 0;
        ui32 totalHugeChunks = env.GetLsmMetrics().HugeUsedChunks;

        { // check metrics
            Cerr << "Bytes written (small): " << env.BytesWrittenSmall << Endl
                << "Bytes written (large): " << env.BytesWrittenLarge << Endl;
            auto metrics = PrintMetrics(env);
            UNIT_ASSERT_VALUE_IN(N * 6, metrics.Level0 + metrics.Level1 + metrics.Level2 + metrics.Level3, N * 8);
            UNIT_ASSERT_VALUE_IN(N * 6, metrics.Level2, N * 8); // everything should be on level 2
            UNIT_ASSERT_VALUE_IN(env.BytesWrittenLarge / CHUNK_SIZE, metrics.HugeUsedChunks, std::ceil(env.BytesWrittenLarge * 1.125 / CHUNK_SIZE));

            compactionBytesWritten = metrics.CompactionBytesWritten;

            for (auto& [nodeId, count] : env.CompactionsPerNode) {
                Cerr << "Node " << nodeId << " had " << count << " compactions" << Endl;
                UNIT_ASSERT_GE(count, 1);
                count = 0; // reset
            }
        }


        DeleteHugeBlobsOfTablet(env, N, 1);
        env.Env.Sim(TDuration::Minutes(30)); // defrag scheduler runs every 5-5.5 minutes


        { // check metrics
            auto metrics = PrintMetrics(env);

            for (const auto& [nodeId, count] : env.CompactionsPerNode) {
                ui32 chunksFreed = env.ChunksFreedByDefragPerNode[nodeId];
                Cerr << "Node " << nodeId << " had " << count << " compactions and " << chunksFreed << " chunks freed" << Endl;

                UNIT_ASSERT_VALUE_IN(1, count, chunksFreed / MAX_DEFRAG_INFLIGHT - 1);
            }
            Cerr << "Total compaction bytes written during deletion: " << metrics.CompactionBytesWritten - compactionBytesWritten << Endl
                << "Amplification to lsm size: " << (metrics.CompactionBytesWritten - compactionBytesWritten) / float(metrics.DskSpaceCurInplacedData) << Endl;

            UNIT_ASSERT_VALUES_EQUAL(metrics.HugeChunksCanBeFreed, 0);
            UNIT_ASSERT_LT(env.GetLsmMetrics().HugeUsedChunks, totalHugeChunks);
        }
    }

    Y_UNIT_TEST(DelayedCompaction) {
        TTestEnv env(0.01);
        ui32 N = 50000;
        ui32 batchSize = 1000;

        WriteData(env, N, batchSize);
        RunFullCompaction(env);
        ui32 totalHugeChunks = env.GetLsmMetrics().HugeUsedChunks;

        // disable compaction to test defrag without compaction
        auto oldCompFilter = env.SetFilterFunction(TEvBlobStorage::EvCompactVDisk, [](ui32, std::unique_ptr<IEventHandle>&) {
            return false; // skip compaction events
        });

        DeleteHugeBlobsOfTablet(env, N, 1);
        // wait for defrag to free all chunks it could free
        env.Env.Sim(TDuration::Minutes(30)); // defrag scheduler runs every 5-5.5 minutes
        UNIT_ASSERT_VALUES_EQUAL(env.GetLsmMetrics().HugeUsedChunks, totalHugeChunks);

        // check that defrag terminating without compaction
        env.ChunksFreedByDefragPerNode.clear();
        env.Env.Sim(TDuration::Minutes(10));
        UNIT_ASSERT_VALUES_EQUAL(env.GetLsmMetrics().HugeUsedChunks, totalHugeChunks);
        UNIT_ASSERT_VALUES_EQUAL(env.ChunksFreedByDefragPerNode.size(), 0);

        env.CompactionsPerNode.clear();
        env.SetFilterFunction(TEvBlobStorage::EvCompactVDisk, std::move(oldCompFilter));
        env.Env.Sim(TDuration::Minutes(10));

        for (const auto& [nodeId, count] : env.CompactionsPerNode) {
            Cerr << "Node " << nodeId << " had " << count << " compactions" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }

        auto metrics = PrintMetrics(env);
        UNIT_ASSERT_VALUES_EQUAL(metrics.HugeChunksCanBeFreed, 0);
        UNIT_ASSERT_LT(env.GetLsmMetrics().HugeUsedChunks, totalHugeChunks);

    }

}
