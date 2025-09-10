#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_quantum.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_search.h>

#include <library/cpp/protobuf/util/pb_io.h>


struct TMetrics {
    ui64 CompactionBytesRead = 0;
    ui64 CompactionBytesWritten = 0;
    ui64 DefragBytesRewritten = 0;

    ui64 Level0 = 0;
    ui64 Level1 = 0;
    ui64 Level2 = 0;
    ui64 Level3 = 0;

    ui64 HugeUsedChunks = 0;
    ui64 HugeChunksCanBeFreed = 0;
    ui64 HugeLockedChunks = 0;

    ui64 DskSpaceCurInplacedData = 0;
    ui64 DskUsedBytes = 0;
    ui64 DskTotalBytes = 0;

    ui64 SpaceInHugeChunksCouldBeFreedViaCompaction = 0;
};

struct TTetsEnvBase {
    TTetsEnvBase(TEnvironmentSetup::TSettings&& settings)
    : Env(std::move(settings))
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Env.Invoke(request);
        const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->GroupID.GetRawId(), baseConfig.GetGroup(0).GetGroupId());
        PdiskLayout = MakePDiskLayout(baseConfig, GroupInfo->GetTopology(), baseConfig.GetGroup(0).GetGroupId());

        Env.Sim(TDuration::Minutes(1));
    }

    void SetIcbControl(const TString& control, ui64 value) {
        for (ui32 i = 1; i <= Env.Settings.NodeCount; ++i) {
            Env.SetIcbControl(i, control, value);
        }
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

    TMetrics GetMetrics() {
        TMetrics metrics;

        metrics.CompactionBytesRead = AggregateVDiskCounters("lsmhull", "LsmCompactionBytesRead");
        metrics.CompactionBytesWritten = AggregateVDiskCounters("lsmhull", "LsmCompactionBytesWritten");
        metrics.DefragBytesRewritten = AggregateVDiskCounters("defrag", "DefragBytesRewritten");

        metrics.Level0 = AggregateVDiskCounters("levels", "NumItems", {{"level", "0"}});
        metrics.Level1 = AggregateVDiskCounters("levels", "NumItems", {{"level", "1..8"}})
                        + AggregateVDiskCounters("levels", "NumItems", {{"level", "9..16"}});
        metrics.Level2 = AggregateVDiskCounters("levels", "NumItems", {{"level", "17"}});
        metrics.Level3 = AggregateVDiskCounters("levels", "NumItems", {{"level", "18"}});

        metrics.HugeUsedChunks = AggregateVDiskCounters("outofspace", "HugeUsedChunks");
        metrics.HugeChunksCanBeFreed = AggregateVDiskCounters("outofspace", "HugeChunksCanBeFreed");
        metrics.HugeLockedChunks = AggregateVDiskCounters("outofspace", "HugeLockedChunks");

        metrics.DskSpaceCurInplacedData = AggregateVDiskCounters("outofspace", "DskSpaceCurInplacedData");
        metrics.DskUsedBytes = AggregateVDiskCounters("outofspace", "DskUsedBytes");
        metrics.DskTotalBytes = AggregateVDiskCounters("outofspace", "DskTotalBytes");

        metrics.SpaceInHugeChunksCouldBeFreedViaCompaction = AggregateVDiskCounters("defrag", "SpaceInHugeChunksCouldBeFreedViaCompaction");

        return metrics;
    }

    TMetrics PrintMetrics() {
        auto metrics = GetMetrics();
        Cerr << "Compaction bytes read: " << metrics.CompactionBytesRead << Endl
             << "Compaction bytes written: " << metrics.CompactionBytesWritten << Endl
             << "Defrag bytes rewritten: " << metrics.DefragBytesRewritten << Endl
             << "Level 0: " << metrics.Level0 << Endl
             << "Level 1: " << metrics.Level1 << Endl
             << "Level 2: " << metrics.Level2 << Endl
             << "Level 3: " << metrics.Level3 << Endl
             << "Huge Used Chunks: " << metrics.HugeUsedChunks << Endl
             << "Huge Chunks Can Be Freed: " << metrics.HugeChunksCanBeFreed << Endl
             << "Huge Locked Chunks: " << metrics.HugeLockedChunks << Endl
             << "Dsk Space Cur Inplaced Data: " << metrics.DskSpaceCurInplacedData << Endl
             << "Dsk Used Bytes: " << metrics.DskUsedBytes << Endl
             << "Dsk Total Bytes: " << metrics.DskTotalBytes << Endl
             << "Space In Huge Chunks Could Be Freed Via Compaction: " << metrics.SpaceInHugeChunksCouldBeFreedViaCompaction << Endl
             << "=========================================================" << Endl;
        return metrics;
    }

    std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> SetFilterFunction(ui32 eventType, std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> func) {
        std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> oldFunc = Filters[eventType];
        if (func == nullptr) {
            Filters.erase(eventType);
        } else {
            Filters[eventType] = std::move(func);
        }
        return oldFunc;
    }

    virtual std::unique_ptr<TEvBlobStorage::TEvPut> GetData(ui32 index) const = 0;

    void WriteData(ui32 N, ui32 batchSize) {
        // warm up ds proxy and bs queue
        SendToDsProxy(GetData(0).release());
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        // write data in batches
        for (ui32 i = 0; i < N / batchSize; ++i) {
            for (ui32 j = 0; j < batchSize; ++j) {
                SendToDsProxy(GetData(i * batchSize + j).release());
            }
            for (ui32 j = 0; j < batchSize; ++j) {
                auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Sender, false);
                if (res->Get()->Status != NKikimrProto::OK) {
                    PrintMetrics();
                }
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }
        }
    }

    void RunFullCompaction() {
        Cerr << "Running full compaction" << Endl;
        Env.Sim(TDuration::Seconds(20));
        for (ui32 i = 0; i < GroupInfo->GetTotalVDisksNum(); ++i) {
            const TActorId& vdiskId = GroupInfo->GetActorId(i);
            Env.CompactVDisk(vdiskId);
        }
        Env.Sim(TDuration::Seconds(20));
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    ui32 CollectGeneration = 0;
    TActorId Sender;
    std::vector<ui32> PdiskLayout;
    std::unordered_map<ui32, std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>> Filters;
};


#define UNIT_ASSERT_VALUE_IN(min, value, max) \
    UNIT_ASSERT_C((min) <= (value) && (value) <= (max), \
        "Value " << value << " is not in range [" << min << ", " << max << "]")


struct TTetsEnvCompThrottler : TTetsEnvBase {
    using TTetsEnvBase::TTetsEnvBase;
    TTetsEnvCompThrottler() : TTetsEnvBase({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }) {
        Data = FastGenDataForLZ4(128 * 1024, 0);
    }

    std::unique_ptr<TEvBlobStorage::TEvPut> GetData(ui32 index) const {
        ui32 tabletId = 1;
        auto& data = Data;
        auto id = TLogoBlobID(tabletId, 1, index, 0, data.size(), 0);
        return std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
    }

    TString Data;
};

ui32 GetMsgSize(std::unique_ptr<IEventBase>& msg) {
    if (msg->Type() == TEvBlobStorage::EvChunkWrite) {
        auto *write = static_cast<NPDisk::TEvChunkWrite*>(msg.get());
        return write->PartsPtr ? write->PartsPtr->ByteSize() : 0;
    } else {
        auto *read = static_cast<NPDisk::TEvChunkRead*>(msg.get());
        return read->Size;
    }
    return 0;
}

struct TCompStatPerNode {
    ui64 BytesWritten = 0;
    ui64 BytesRead = 0;

    void AddWrittenBytes(ui64 bytes) {
        BytesWritten += bytes;
    }

    void AddReadBytes(ui64 bytes) {
        BytesRead += bytes;
    }

    void Clear() {
        BytesWritten = BytesRead = 0;
    }

    ui64 GetSumBytes() {
        return BytesWritten + BytesRead;
    }
};

struct TCompStat {
    TCompStat() {
        for (ui32 i = 1; i <= 8; i++) {
            NodeCompStats[i] = TCompStatPerNode();
        }
    }

    void AddBytes(ui32 nodeId, NPDisk::TEvChunkWrite* msg) {
        NodeCompStats[nodeId].AddWrittenBytes(msg->PartsPtr ? msg->PartsPtr->ByteSize() : 0);
    }

    void AddBytes(ui32 nodeId, NPDisk::TEvChunkRead* msg) {
        NodeCompStats[nodeId].AddReadBytes(msg->Size);
    }

    ui64 GetSumWrittenBytes() {
        ui64 result = 0;
        for (ui32 i = 1; i <= 8; i++) {
            result += NodeCompStats[i].BytesWritten;
        }
        return result;
    }

    ui64 GetSumReadBytes() {
        ui64 result = 0;
        for (ui32 i = 1; i <= 8; i++) {
            result += NodeCompStats[i].BytesRead;
        }
        return result;
    }

    ui64 GetSumBytes() {
        return GetSumWrittenBytes() + GetSumReadBytes();
    }

    void Clear() {
        for (ui32 i = 1; i <= 8; i++) {
            NodeCompStats[i].Clear();
        }
    }

    THashMap<ui32, TCompStatPerNode> NodeCompStats;
};

void CheckCompStat(TDuration duration, TCompStatPerNode& nodeStat, ui32 rate) {
    if (!rate) {
        return;
    }
    UNIT_ASSERT(nodeStat.BytesWritten + nodeStat.BytesRead <= (duration.Seconds() + 1) * rate);
}

Y_UNIT_TEST_SUITE(CompactionThrottler) {

    Y_UNIT_TEST(CompactionLoad) {
        TTetsEnvCompThrottler env;
        ui32 N = 7000;
        ui32 batchSize = 1000;

        TCompStat compStats;
        ui64 compactionBytesWritten = 0, compactionBytesRead = 0;
        ui64 expectedCompactionBytesWritten = 0;

        std::unordered_map<ui32, ui64> firstRequests;

        env.Env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvChunkWrite: {
                    auto *write = ev->Get<NPDisk::TEvChunkWrite>();
                    compStats.AddBytes(nodeId, write);
                    break;
                }
                case TEvBlobStorage::EvChunkRead: {
                    auto *read = ev->Get<NPDisk::TEvChunkRead>();
                    compStats.AddBytes(nodeId, read);
                    break;
                }
            }
            return true;
        };

        env.WriteData(N, batchSize);

        // run first full compaction for stabilize lsm
        env.RunFullCompaction();

        compactionBytesWritten = env.GetMetrics().CompactionBytesWritten;
        compactionBytesRead = env.GetMetrics().CompactionBytesRead;
        compStats.Clear();

        // run 2 full compaction for calculate read and write rate without throttler
        env.RunFullCompaction();

        expectedCompactionBytesWritten = compactionBytesWritten = env.GetMetrics().CompactionBytesWritten - compactionBytesWritten;
        compactionBytesRead = env.GetMetrics().CompactionBytesRead - compactionBytesRead;
        UNIT_ASSERT_EQUAL(compactionBytesWritten, compStats.GetSumWrittenBytes());
        UNIT_ASSERT_EQUAL(compactionBytesRead, compStats.GetSumReadBytes());

        std::unordered_map<ui32, ui32> nodeCompRate;
        std::unordered_map<ui32, ui32> expectedWorkingSeconds;
        { // set up throttler rate
            nodeCompRate[1] = 0; // without throttler
            nodeCompRate[2] = compStats.NodeCompStats[2].GetSumBytes() + 1; // more then sum value (like without throttler)
            for (ui32 i = 3; i <= env.Env.Settings.NodeCount; ++i) {
                expectedWorkingSeconds[i] = 50 * (i - 2); // +50 sec for compaction for each next nodes
                nodeCompRate[i] = compStats.NodeCompStats[i].GetSumBytes() / (expectedWorkingSeconds[i]);
            }
            for (ui32 i = 1; i <= env.Env.Settings.NodeCount; ++i) {
                env.Env.SetIcbControl(i, "VDiskControls.HullCompThrottlerBytesRate", nodeCompRate[i]);
            }
            env.Env.Sim(TDuration::Minutes(1));
        }

        std::unordered_map<TActorId, std::unique_ptr<IEventHandle>> waitingCompactions;
        ui32 compactionsFinishedCount = 0;
        { // start 3 full compaction to check compaction throttler
            for (ui32 i = 0; i < env.GroupInfo->GetTotalVDisksNum(); ++i) {
                const TActorId& vdiskId = env.GroupInfo->GetActorId(i);
                auto waitingCompactionActorId = env.Env.Runtime->AllocateEdgeActor(vdiskId.NodeId());
                env.Env.Runtime->Send(new IEventHandle(vdiskId, waitingCompactionActorId, TEvCompactVDisk::Create(EHullDbType::LogoBlobs, TEvCompactVDisk::EMode::FULL)), vdiskId.NodeId());
                TTestActorSystem::TEdgeActor *waitingCompactionActor = dynamic_cast<TTestActorSystem::TEdgeActor*>(env.Env.Runtime->GetActor(waitingCompactionActorId));
                waitingCompactionActor->WaitForEvent(&waitingCompactions[waitingCompactionActorId]);
            }

        }

        TInstant startCompactions = env.Env.Runtime->GetClock();
        compStats.Clear();

        ui32 i = 0;
        while (compactionsFinishedCount < env.GroupInfo->GetTotalVDisksNum()) {
            for (auto it = waitingCompactions.begin(); it != waitingCompactions.end(); ) {
                TInstant now = env.Env.Runtime->GetClock();
                auto nodeId = it->first.NodeId();
                auto nodeStat = compStats.NodeCompStats[nodeId];
                CheckCompStat(now - startCompactions, nodeStat, nodeCompRate[nodeId]);
                if (it->second && it->second->GetTypeRewrite() == TEvBlobStorage::EvCompactVDiskResult) {
                    env.Env.Runtime->DestroyActor(it->first);
                    it = waitingCompactions.erase(it);
                    compactionsFinishedCount++;

                    // check compaction wotking time
                    if (nodeId <= 2) {
                        // nodes without rate limit
                        UNIT_ASSERT_EQUAL(i, 1); // <= 10 sec
                    } else {
                        // node with rate limit
                        UNIT_ASSERT_VALUE_IN((i - 1) * 10, expectedWorkingSeconds[nodeId], i * 10);
                    }
                } else {
                    ++it;
                }
            }
            i++;
            env.Env.Sim(TDuration::Seconds(10));
        }

        UNIT_ASSERT_EQUAL(compStats.GetSumWrittenBytes(), expectedCompactionBytesWritten);

    }

}

struct TTestEnvCompDefragIndependent : TTetsEnvBase {
    static constexpr ui64 CHUNK_SIZE = 32_MB;
    static constexpr ui64 MIN_HUGE_BLOB_SIZE = 128_KB;
    static constexpr ui64 MAX_DEFRAG_INFLIGHT = 2;

    using TTetsEnvBase::TTetsEnvBase;
    TTestEnvCompDefragIndependent(double garbageThresholdToRunCompaction = 0.0)
        : TTetsEnvBase({
            .NodeCount = 8,
            .VDiskReplPausedAtStart = false,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .DiskType = NPDisk::EDeviceType::DEVICE_TYPE_ROT,
            .MinHugeBlobInBytes = MIN_HUGE_BLOB_SIZE,
            .PDiskSize = 20_GB,
            .PDiskChunkSize = CHUNK_SIZE,
        })
    {
        DataSmall = FastGenDataForLZ4(32_KB, 0);
        DataLarge = FastGenDataForLZ4(1_MB, 0);

        SetIcbControl("VDiskControls.MaxChunksToDefragInflight", MAX_DEFRAG_INFLIGHT);
        SetIcbControl("VDiskControls.DefaultHugeGarbagePerMille", 50);
        SetIcbControl("VDiskControls.GarbageThresholdToRunFullCompactionPerMille", garbageThresholdToRunCompaction * 1000);
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
            DefragQuantumsPerNode[nodeId]++;
            return true;
        });
    }

    std::unique_ptr<TEvBlobStorage::TEvPut> GetData(ui32 index) const {
        ui32 tabletId = index % 2 + 1;
        auto& data = index % 100 < 10 ? DataLarge : DataSmall;
        auto id = TLogoBlobID(tabletId, 1, index, 0, data.size(), 0);
        return std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
    }

    TString DataSmall, DataLarge;

    ui64 BytesWrittenSmall = 0, BytesWrittenLarge = 0;
    std::unordered_map<ui32, ui32> CompactionsPerNode;
    std::unordered_map<ui32, ui32> ChunksFreedByDefragPerNode;
    std::unordered_map<ui32, ui32> DefragQuantumsPerNode;
    std::unordered_set<std::pair<ui32, ui32>> SeenParts;
};


void DeleteHugeBlobsOfTablet(TTetsEnvBase& env, ui32 N, ui32 tabletId) {
    auto keep = std::make_unique<TVector<TLogoBlobID>>();
    for (ui32 i = 0; i < N; ++i) {
        auto ev = env.GetData(i);
        if (ev->Id.TabletID() == tabletId && ev->Buffer.size() < TTestEnvCompDefragIndependent::MIN_HUGE_BLOB_SIZE * 4) {
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

struct TEvDefragStartQuantum : TEventLocal<TEvDefragStartQuantum, TEvBlobStorage::EvDefragStartQuantum> {
    NKikimr::TChunksToDefrag ChunksToDefrag;
};

Y_UNIT_TEST_SUITE(CompDefrag) {

    Y_UNIT_TEST(DoesItWork) {
        TTestEnvCompDefragIndependent env(0.01);
        ui32 N = 50000;
        ui32 batchSize = 1000;

        env.WriteData(N, batchSize);
        env.RunFullCompaction();
        ui64 compactionBytesWritten = 0;
        ui32 totalHugeChunks = env.GetMetrics().HugeUsedChunks;

        { // check metrics
            Cerr << "Bytes written (small): " << env.BytesWrittenSmall << Endl
                << "Bytes written (large): " << env.BytesWrittenLarge << Endl;
            auto metrics = env.PrintMetrics();
            UNIT_ASSERT_VALUE_IN(N * 6, metrics.Level0 + metrics.Level1 + metrics.Level2 + metrics.Level3, N * 8);
            UNIT_ASSERT_VALUE_IN(N * 6, metrics.Level2, N * 8); // everything should be on level 2
            UNIT_ASSERT_VALUE_IN(env.BytesWrittenLarge / TTestEnvCompDefragIndependent::CHUNK_SIZE, metrics.HugeUsedChunks, std::ceil(env.BytesWrittenLarge * 1.125 / TTestEnvCompDefragIndependent::CHUNK_SIZE));

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
            auto metrics = env.PrintMetrics();

            for (const auto& [nodeId, count] : env.CompactionsPerNode) {
                ui32 chunksFreed = env.ChunksFreedByDefragPerNode[nodeId];
                Cerr << "Node " << nodeId << " had " << count << " compactions and " << chunksFreed << " chunks freed" << Endl;

                UNIT_ASSERT_VALUE_IN(1, count, chunksFreed / TTestEnvCompDefragIndependent::MAX_DEFRAG_INFLIGHT - 1);
            }
            Cerr << "Total compaction bytes written during deletion: " << metrics.CompactionBytesWritten - compactionBytesWritten << Endl
                << "Amplification to lsm size: " << (metrics.CompactionBytesWritten - compactionBytesWritten) / float(metrics.DskSpaceCurInplacedData) << Endl;

            UNIT_ASSERT_VALUES_EQUAL(metrics.HugeChunksCanBeFreed, 0);
            UNIT_ASSERT_LT(env.GetMetrics().HugeUsedChunks, totalHugeChunks);
        }
    }

    Y_UNIT_TEST(DelayedCompaction) {
        TTestEnvCompDefragIndependent env(0.01);
        ui32 N = 50000;
        ui32 batchSize = 1000;

        env.WriteData(N, batchSize);
        env.RunFullCompaction();
        // env.Env.Sim(TDuration::Minutes(10));
        ui32 totalHugeChunks = env.GetMetrics().HugeUsedChunks;

        // disable compaction to test defrag without compaction
        TVector<std::unique_ptr<IEventHandle>> compactions;
        auto oldCompFilter = env.SetFilterFunction(TEvBlobStorage::EvCompactVDisk, [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            compactions.push_back(std::move(ev));
            return false; // skip compaction events
        });

        DeleteHugeBlobsOfTablet(env, N, 1);
        // wait for defrag to free all chunks it could free
        env.Env.Sim(TDuration::Minutes(30)); // defrag scheduler runs every 5-5.5 minutes
        UNIT_ASSERT_VALUES_EQUAL(env.GetMetrics().HugeUsedChunks, totalHugeChunks);

        // check that defrag terminating without compaction
        env.ChunksFreedByDefragPerNode.clear();
        env.Env.Sim(TDuration::Minutes(10));
        UNIT_ASSERT_VALUES_EQUAL(env.GetMetrics().HugeUsedChunks, totalHugeChunks);
        UNIT_ASSERT_VALUES_EQUAL(env.ChunksFreedByDefragPerNode.size(), 0);

        env.CompactionsPerNode.clear();
        env.SetFilterFunction(TEvBlobStorage::EvCompactVDisk, std::move(oldCompFilter));
        for (auto& ev : compactions) {
            env.Env.Runtime->WrapInActorContext(env.Sender, [&] {
                TlsActivationContext->Send(ev.release());
            });
        }
        env.Env.Sim(TDuration::Minutes(10));

        for (const auto& [nodeId, count] : env.CompactionsPerNode) {
            Cerr << "Node " << nodeId << " had " << count << " compactions" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }

        auto metrics = env.PrintMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.HugeChunksCanBeFreed, 0);
        UNIT_ASSERT_LT(env.GetMetrics().HugeUsedChunks, totalHugeChunks);

    }

    Y_UNIT_TEST(ZeroThresholdDefragWithCompaction) {
        TTestEnvCompDefragIndependent env(0.0); // Zero threshold - compaction should run immediately
        ui32 N = 50000;
        ui32 batchSize = 1000;

        env.WriteData(N, batchSize);
        env.RunFullCompaction();
        ui32 totalHugeChunks = env.GetMetrics().HugeUsedChunks;

        // Clear counters before deletion
        env.CompactionsPerNode.clear();
        env.DefragQuantumsPerNode.clear();
        env.ChunksFreedByDefragPerNode.clear();

        DeleteHugeBlobsOfTablet(env, N, 1);
        env.Env.Sim(TDuration::Minutes(30)); // defrag scheduler runs every 5-5.5 minutes

        // Check that defragmentation is working
        auto metrics = env.PrintMetrics();
        UNIT_ASSERT_LT(env.GetMetrics().HugeUsedChunks, totalHugeChunks);
        UNIT_ASSERT_VALUES_EQUAL(metrics.HugeChunksCanBeFreed, 0);

        // Verify that number of compactions equals number of defragmentation quantums
        ui32 totalCompactions = 0;
        ui32 totalDefragQuanta = 0;

        for (const auto& [nodeId, compactions] : env.CompactionsPerNode) {
            ui32 defragQuantums = env.DefragQuantumsPerNode[nodeId];
            Cerr << "Node " << nodeId << " had " << compactions << " compactions and " << defragQuantums << " defrag quantums" << Endl;

            totalCompactions += compactions;
            totalDefragQuanta += defragQuantums;

            // Each defrag quantum should trigger exactly one compaction with zero threshold
            UNIT_ASSERT_VALUES_EQUAL(compactions, defragQuantums);
        }

        UNIT_ASSERT_GT(totalCompactions, 0);
        UNIT_ASSERT_GT(totalDefragQuanta, 0);
        UNIT_ASSERT_VALUES_EQUAL(totalCompactions, totalDefragQuanta);

        Cerr << "Total compactions: " << totalCompactions << ", Total defrag quantums: " << totalDefragQuanta << Endl;
    }

    Y_UNIT_TEST(DynamicThresholdChange) {
        TTestEnvCompDefragIndependent env(0.0); // Start with zero threshold
        ui32 N = 75000; // Increased from 50000 to create more data
        ui32 batchSize = 1000;

        env.WriteData(N, batchSize);
        env.RunFullCompaction();
        ui32 totalHugeChunks = env.GetMetrics().HugeUsedChunks;

        // Clear counters before deletion
        env.CompactionsPerNode.clear();
        env.DefragQuantumsPerNode.clear();
        env.ChunksFreedByDefragPerNode.clear();

        // Track quantums per node and threshold change status
        std::unordered_map<ui32, ui32> quantumsBeforeThresholdChange;
        std::unordered_set<ui32> nodesWithThresholdChanged;
        ui32 targetQuantums = 2;

        // Add filter to track defrag quantums and change threshold dynamically
        env.SetFilterFunction(NKikimr::TEvDefragQuantumResult::EventType, [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            auto res = ev->Get<NKikimr::TEvDefragQuantumResult>();
            env.ChunksFreedByDefragPerNode[nodeId] += res->Stat.FreedChunks.size();
            env.DefragQuantumsPerNode[nodeId]++;

            // Check if this node has reached target quantums and threshold hasn't been changed yet
            if (env.DefragQuantumsPerNode[nodeId] == targetQuantums && !nodesWithThresholdChanged.count(nodeId)) {
                Cerr << "Node " << nodeId << " reached " << targetQuantums << " quantums, changing threshold to 0.01" << Endl;

                // Change threshold for this specific node
                env.Env.SetIcbControl(nodeId, "VDiskControls.GarbageThresholdToRunFullCompactionPerMille", 10); // 0.01 * 1000 = 10
                nodesWithThresholdChanged.insert(nodeId);

                // Record quantums before threshold change for this node
                quantumsBeforeThresholdChange[nodeId] = env.DefragQuantumsPerNode[nodeId];
            }

            return true;
        });

        DeleteHugeBlobsOfTablet(env, N, 1);
        env.Env.Sim(TDuration::Minutes(30)); // Wait for defrag to complete

        // Verify results
        ui32 totalCompactions = 0;
        ui32 totalQuantums = 0;

        for (ui32 nodeId = 1; nodeId <= env.Env.Settings.NodeCount; ++nodeId) {
            ui32 compactions = env.CompactionsPerNode[nodeId];
            ui32 quantums = env.DefragQuantumsPerNode[nodeId];
            ui32 quantumsBeforeChange = quantumsBeforeThresholdChange[nodeId];
            ui32 quantumsAfterChange = quantums - quantumsBeforeChange;

            Cerr << "Node " << nodeId << " had " << compactions << " compactions and " << quantums << " defrag quantums" << Endl;
            Cerr << "  - Before threshold change: " << quantumsBeforeChange << " quantums" << Endl;
            Cerr << "  - After threshold change: " << quantumsAfterChange << " quantums" << Endl;

            totalCompactions += compactions;
            totalQuantums += quantums;

            // Verify that with zero threshold, we have 1:1 ratio for the first 2 quantums
            UNIT_ASSERT_GE(quantumsBeforeChange, targetQuantums);

            // For quantums after threshold change, should have fewer compactions than quantums
            if (quantumsAfterChange > 0) {
                // With 0.01 threshold, we should have fewer compactions than total quantums
                // but more than just the quantums before threshold change
                UNIT_ASSERT_GT(compactions, quantumsBeforeChange); // Should have more compactions than just the first 2
                UNIT_ASSERT_LT(compactions, quantums); // Should have fewer compactions than total quantums
            }
        }

        UNIT_ASSERT_GT(totalCompactions, 0);
        UNIT_ASSERT_GT(totalQuantums, 0);

        Cerr << "Total compactions: " << totalCompactions << ", Total defrag quantums: " << totalQuantums << Endl;

        // Verify defragmentation completed successfully
        auto metrics = env.PrintMetrics();
        UNIT_ASSERT_LT(env.GetMetrics().HugeUsedChunks, totalHugeChunks);
        UNIT_ASSERT_VALUES_EQUAL(metrics.HugeChunksCanBeFreed, 0);
    }

    Y_UNIT_TEST(ChunksSoftLocking) {
        TTestEnvCompDefragIndependent env(0.01);
        ui32 N = 50000;
        ui32 batchSize = 1000;

        env.SetIcbControl("VDiskControls.MaxChunksToDefragInflight", 10);

        env.WriteData(N, batchSize);
        env.RunFullCompaction();

        env.PrintMetrics();

        // freeze defragmentation with locked chunks
        TVector<IEventHandle*> lockChunksResponses;
        auto oldFilter = env.SetFilterFunction(NKikimr::TEvHugeLockChunksResult::EventType, [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            auto msg = ev->Get<NKikimr::TEvHugeLockChunksResult>();
            UNIT_ASSERT_GT(msg->LockedChunks.size(), 5);
            lockChunksResponses.push_back(ev.release());
            return false;
        });

        DeleteHugeBlobsOfTablet(env, N, 1);
        env.RunFullCompaction();
        env.Env.Sim(TDuration::Minutes(10));
        UNIT_ASSERT_VALUES_EQUAL(lockChunksResponses.size(), env.Env.Settings.NodeCount);

        ui64 hugeUsedChunksBeforeWrites = env.PrintMetrics().HugeUsedChunks;

        // write more data
        env.Sender = env.Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        ui32 M = N / 2; // half of the data were deleted, so we write half of the data again and expect no new chunks to be created
        for (ui32 i = 0; i < M / batchSize; ++i) {
            for (ui32 j = 0; j < batchSize; ++j) {
                auto ev = env.GetData(i * batchSize + j);
                auto nextGenId = TLogoBlobID::Make(ev->Id.TabletID(), ev->Id.Generation() + 1, ev->Id.Step(), ev->Id.Channel(), ev->Id.BlobSize(), ev->Id.Cookie(), ev->Id.CrcMode());
                auto nextGenEv = std::make_unique<TEvBlobStorage::TEvPut>(nextGenId, TRope(ev->Buffer).ConvertToString(), TInstant::Max());
                env.SendToDsProxy(nextGenEv.release());
            }
            for (ui32 j = 0; j < batchSize; ++j) {
                auto res = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(env.Sender, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }
        }
        env.Env.Sim(TDuration::Minutes(10));

        // check that no new chunks were created
        UNIT_ASSERT_VALUES_EQUAL(env.PrintMetrics().HugeUsedChunks, hugeUsedChunksBeforeWrites);


        // unfreeze defragmentation with locked chunks
        env.SetFilterFunction(NKikimr::TEvHugeLockChunksResult::EventType, [&](ui32, std::unique_ptr<IEventHandle>&) {
            return true;
        });
        for (auto& ev : lockChunksResponses) {
            env.Env.Runtime->WrapInActorContext(ev->Sender, [&] {
                TlsActivationContext->Send(ev);
            });
        }
        env.Env.Sim(TDuration::Minutes(10));
    }

    Y_UNIT_TEST(DefragThrottling) {
        ui64 totalBytesToDefrag = 0;
        { // without throttling
            TTestEnvCompDefragIndependent env(0.01);
            ui32 N = 50000;
            ui32 batchSize = 1000;
            
            env.WriteData(N, batchSize);
            env.RunFullCompaction();

            DeleteHugeBlobsOfTablet(env, N, 1);

            env.Env.Sim(TDuration::Minutes(30));

            totalBytesToDefrag = env.PrintMetrics().DefragBytesRewritten;
            Cerr << "Total bytes to defrag: " << totalBytesToDefrag << Endl;
        }

        UNIT_ASSERT_GT(totalBytesToDefrag, 512_MB);

        { // with throttling
            TTestEnvCompDefragIndependent env(0.01);
            ui32 N = 50000;
            ui32 batchSize = 1000;
            
            env.WriteData(N, batchSize);
            env.RunFullCompaction();

            env.PrintMetrics();

            DeleteHugeBlobsOfTablet(env, N, 1);

            env.SetIcbControl("VDiskControls.DefragThrottlerBytesRate", 1_MB);
            TDuration maxThrottlingDuration = TDuration::Minutes(6) + totalBytesToDefrag / 1_MB / 8 * TDuration::Seconds(1) * 2; // 2 is a factor of safety
            Cerr << "Max throttling duration: " << maxThrottlingDuration.ToString() << Endl;

            ui64 defragBytesRewrittenBefore = env.PrintMetrics().DefragBytesRewritten;
            for (ui32 i = 0; i < maxThrottlingDuration.Seconds(); ++i) {
                env.Env.Sim(TDuration::Seconds(1));
                ui64 cur = env.GetMetrics().DefragBytesRewritten;
                ui64 defragBytesRewritten = cur - defragBytesRewrittenBefore;
                UNIT_ASSERT_LE(defragBytesRewritten, 1_MB * 8);
                defragBytesRewrittenBefore = cur;
            }

            env.PrintMetrics();
            UNIT_ASSERT_VALUE_IN(totalBytesToDefrag - totalBytesToDefrag / 100, defragBytesRewrittenBefore, totalBytesToDefrag + totalBytesToDefrag / 100);
        }
    }

}
