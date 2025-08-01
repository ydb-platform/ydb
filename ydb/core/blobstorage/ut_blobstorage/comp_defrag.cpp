#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/defrag/defrag_search.h>

#include <library/cpp/protobuf/util/pb_io.h>

struct TLsmMetrics {
    ui64 CompactionBytesRead = 0;
    ui64 CompactionBytesWritten = 0;
};

struct TTetsEnv {
    TTetsEnv()
    : Env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        DataSmall = FastGenDataForLZ4(128 * 1024, 0);

        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Env.Invoke(request);
        const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->GroupID.GetRawId(), baseConfig.GetGroup(0).GetGroupId());
        PdiskLayout = MakePDiskLayout(baseConfig, GroupInfo->GetTopology(), baseConfig.GetGroup(0).GetGroupId());

        Env.Sim(TDuration::Minutes(1));
    }

    std::unique_ptr<TEvBlobStorage::TEvPut> GetData(ui32 index) const {
        ui32 tabletId = 1;
        auto& data = DataSmall;
        auto id = TLogoBlobID(tabletId, 1, index, 0, data.size(), 0);
        return std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
    }

    template<class TEvent>
    void SendToDsProxy(TEvent* event) {
        Env.Runtime->WrapInActorContext(Sender, [&] {
            SendToBSProxy(Sender, GroupInfo->GroupID, event);
        });
    }

    ui64 AggregateVDiskCounters(const TString& subsystem, const TString& counter) {
        return Env.AggregateVDiskCounters(
            "test", Env.Settings.NodeCount,
            GroupInfo->GetTotalVDisksNum(), GroupInfo->GroupID.GetRawId(),
            PdiskLayout, subsystem, counter
        );
    }

    TLsmMetrics GetLsmMetrics() {
        TLsmMetrics metrics;

        metrics.CompactionBytesRead = AggregateVDiskCounters("lsmhull", "LsmCompactionBytesRead");
        metrics.CompactionBytesWritten = AggregateVDiskCounters("lsmhull", "LsmCompactionBytesWritten");

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

Y_UNIT_TEST_SUITE(CompDefrag) {

    Y_UNIT_TEST(CompactionLoad) {
        TTetsEnv env;
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

        { // write data
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
                    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                }
            }
        }

        { // run first full compaction for stabilize lsm
            env.Env.Sim(TDuration::Seconds(20));
            for (ui32 i = 0; i < env.GroupInfo->GetTotalVDisksNum(); ++i) {
                const TActorId& vdiskId = env.GroupInfo->GetActorId(i);
                env.Env.CompactVDisk(vdiskId);
            }
            env.Env.Sim(TDuration::Seconds(20));
        }

        compactionBytesWritten = env.GetLsmMetrics().CompactionBytesWritten;
        compactionBytesRead = env.GetLsmMetrics().CompactionBytesRead;
        compStats.Clear();

        { // run 2 full compaction for calculate read and write rate without throttler
            env.Env.Sim(TDuration::Seconds(20));
            for (ui32 i = 0; i < env.GroupInfo->GetTotalVDisksNum(); ++i) {
                const TActorId& vdiskId = env.GroupInfo->GetActorId(i);
                env.Env.CompactVDisk(vdiskId);
            }
            env.Env.Sim(TDuration::Seconds(20));
        }

        expectedCompactionBytesWritten = compactionBytesWritten = env.GetLsmMetrics().CompactionBytesWritten - compactionBytesWritten;
        compactionBytesRead = env.GetLsmMetrics().CompactionBytesRead - compactionBytesRead;
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
