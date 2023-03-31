#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

static TIntrusivePtr<TBlobStorageGroupInfo> PrepareEnv(TEnvironmentSetup& env, TVector<TLogoBlobID> *keep) {
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
    env.Sim(TDuration::Minutes(5));

    const ui32 dataLen = 512 * 1024;
    const TString data(dataLen, 'x');
    ui32 index = 0;

    const ui32 orderNum = 0;
    const TVDiskID& vdiskId = info->GetVDiskId(orderNum);
    const TActorId& actorId = info->GetActorId(orderNum);

    const ui32 targetNumChunks = 20;
    std::map<ui32, std::vector<TLogoBlobID>> chunkToBlob;

    for (;;) {
        const TLogoBlobID id(1, 1, index /*step*/, 0, data.size(), 0);
        const ui32 hash = id.FullID().Hash();
        if (!info->GetTopology().BelongsToSubgroup(vdiskId, hash)) {
            continue;
        }
        const ui32 idxInSubgroup = info->GetTopology().GetIdxInSubgroup(vdiskId, hash);
        const ui32 partIdx = idxInSubgroup & 1; // 0 1 0 1 01 01 01 01 possible layouts; this fits them
        env.PutBlob(vdiskId, TLogoBlobID(id, partIdx + 1), data);
        ++index;

        env.Sim();

        auto res = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
        chunkToBlob.clear();
        for (const auto& item : res->Layout) {
            using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
            if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob) {
                chunkToBlob[item.Location.ChunkIdx].push_back(item.BlobId);
            }
        }
        if (chunkToBlob.size() == targetNumChunks) {
            break;
        }
    }

    std::set<TLogoBlobID> blobsToDelete;
    for (auto& [_, blobsOfChunk] : chunkToBlob) {
        blobsToDelete.insert(std::next(blobsOfChunk.begin()), blobsOfChunk.end());
        keep->push_back(blobsOfChunk.front());
    }

    // issue gc command
    {
        const TActorId& sender = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(1, 1, 1, 0, true, 1, Max<ui32>(),
                new TVector<TLogoBlobID>(*keep), nullptr, TInstant::Max(), true));
        });
        const auto& res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    // wait for sync
    env.Sim(TDuration::Seconds(3));

    // partition
    for (const ui32 node : env.Runtime->GetNodes()) {
        env.StopNode(node);
    }
    env.StartNode(actorId.NodeId());
    env.Sim(TDuration::Seconds(20));

    for (;;) {
        // trigger compaction
        {
            const auto& sender = env.Runtime->AllocateEdgeActor(actorId.NodeId());
            auto ev = std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
            ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
            env.Runtime->Send(ev.release(), sender.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(sender);
        }

        // check layout
        ui32 numUndeleted = 0;
        std::map<ui32, ui32> chunkToBlobs;
        auto res = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
        for (const auto& item : res->Layout) {
            using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
            if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob) {
                ++chunkToBlobs[item.Location.ChunkIdx];
                numUndeleted += blobsToDelete.count(item.BlobId);
            }
        }
        if (numUndeleted) {
            env.Sim(TDuration::Seconds(10));
            continue;
        }
        for (const auto& [_, numBlobs] : chunkToBlobs) {
            if (numBlobs == targetNumChunks && chunkToBlobs.size() == 1) {
                return nullptr; // all done
            }
            UNIT_ASSERT_VALUES_EQUAL(numBlobs, 1);
        }
        break;
    }

    return info;
}

Y_UNIT_TEST_SUITE(Defragmentation) {
    Y_UNIT_TEST(DoesItWork) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
        });

        TVector<TLogoBlobID> keep;
        TIntrusivePtr<TBlobStorageGroupInfo> info = PrepareEnv(env, &keep);
        if (!info) {
            return;
        }
        const ui32 orderNum = 0;
        const TActorId& actorId = info->GetActorId(orderNum);

        // defrag
        {
            env.Sim(TDuration::Minutes(6)); // time enough to start defragmentation automatically
            auto factory = []{ return std::unique_ptr<IEventBase>(TEvCompactVDisk::Create(EHullDbType::LogoBlobs)); };
            env.SyncQueryFactory<TEvCompactVDiskResult>(actorId, factory, true);
        }

        // check layout again
        {
            std::map<ui32, ui32> chunkToBlobs;
            auto res = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            for (const auto& item : res->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob) {
                    ++chunkToBlobs[item.Location.ChunkIdx];
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(chunkToBlobs.size(), 1);
        }
    }

    Y_UNIT_TEST(DefragCompactionRace) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
        });

        TVector<TLogoBlobID> keep;
        TIntrusivePtr<TBlobStorageGroupInfo> info = PrepareEnv(env, &keep);
        if (!info) {
            return;
        }
        const ui32 orderNum = 0;
        const TActorId& actorId = info->GetActorId(orderNum);

        // set up filtering function to catch TEvPut events from defrag rewriter
        ui32 caughtPutNodeId;
        std::unique_ptr<IEventHandle> caughtPut;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Type == TEvBlobStorage::EvVPut) {
                UNIT_ASSERT(!caughtPut);
                caughtPutNodeId = nodeId;
                caughtPut = std::move(ev);
                return false;
            }
            return true;
        };

        while (!caughtPut) {
            env.Sim(TDuration::Minutes(1));
        }

        // unpartition
        for (const ui32 node : env.Runtime->GetNodes()) {
            if (node != actorId.NodeId()) {
                env.StartNode(node);
            }
        }

        // issue collect garbage command
        {
            const TActorId& sender = env.Runtime->AllocateEdgeActor(1);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(1, 1, 2, 0, false, 0, 0,
                    nullptr, new TVector<TLogoBlobID>(keep), TInstant::Max(), true));
            });
            const auto& res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        // trigger compaction
        auto factory = []{ return std::unique_ptr<IEventBase>(TEvCompactVDisk::Create(EHullDbType::LogoBlobs)); };
        env.SyncQueryFactory<TEvCompactVDiskResult>(actorId, factory, true);

        // ensure empty layout
        {
            auto res = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            for (const auto& item : res->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::HugeBlob) {
                    UNIT_ASSERT(false);
                }
            }
        }

        // issue some blob metadata
        {
            const TActorId& sender = env.Runtime->AllocateEdgeActor(1);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(1, 1, 3, 0, false, 0, 0,
                    new TVector<TLogoBlobID>(keep), nullptr, TInstant::Max(), true));
            });
            const auto& res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        // resume defrag
        env.Runtime->Send(caughtPut.release(), caughtPutNodeId);
        ui32 putCounter = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Type == TEvBlobStorage::EvVPut) {
                ++putCounter;
            }
            return true;
        };
        env.Sim(TDuration::Minutes(5));
        UNIT_ASSERT_VALUES_EQUAL(putCounter, 1);
    }
}
