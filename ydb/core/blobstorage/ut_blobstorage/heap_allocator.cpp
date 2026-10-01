#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullactor.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullcompact.h>

namespace {

    struct TBlobInfo {
        TLogoBlobID Id;
        TString Data;
        bool Alive = true;
    };

    class TWorkload {
        static constexpr ui32 NumRounds = 5;
        static constexpr ui32 BlobsPerRound = 40;
        static constexpr ui64 BaseTabletId = 1000;

        const ui32 RestartsPerCheckpoint;
        TEnvironmentSetup Env;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TReallyFastRng32 Rng;
        std::vector<TBlobInfo> Blobs;
        std::array<ui32, NumRounds> PerGenerationCounter;

    public:
        TWorkload(bool enableHeapAllocator, ui64 seed, ui32 restartsPerCheckpoint = 1,
                bool freezeKeeperEntryPoint = false, ui64 pdiskChunkSize = 0)
            : RestartsPerCheckpoint(restartsPerCheckpoint)
            , Env{{
                    .NodeCount = 1,
                    .Erasure = TBlobStorageGroupType::ErasureNone,
                    .FeatureFlags = MakeFeatureFlags(enableHeapAllocator),
                    .MinHugeBlobInBytes = 4096,
                    .PDiskChunkSize = pdiskChunkSize,
                }}
            , Rng(seed)
        {
            PerGenerationCounter.fill(1);

            if (freezeKeeperEntryPoint) {
                // The huge keeper writes a new entry point only when PDisk asks for the log to be cut; with those
                // requests gone its persisted state stays frozen at the boot-time one, while the hull goes on
                // committing SSTs that point into chunks the keeper started striping long afterwards.
                Env.Runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
                    return ev->GetTypeRewrite() != TEvBlobStorage::EvCutLog;
                };
            }

            Env.CreateBoxAndPool(1, 1);
            Env.Sim(TDuration::Minutes(1));
            auto groups = Env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            Info = Env.GetGroupInfo(groups.front());
        }

        void Run() {
            for (ui32 round = 0; round < NumRounds; ++round) {
                Write(round);
                // restart before compacting, so that local recovery has to replay huge blob allocations that are
                // still only present in the recovery log
                Restart();
                Verify();

                Delete(round);
                Compact();
                // ... and here it replays slot deletions along with the freshly committed entry point
                Restart();
                Verify();
            }

            // drop everything -- this empties out the chunks holding the blobs and returns them to the allocator
            for (ui32 round = 0; round < NumRounds; ++round) {
                DeleteAll(round);
            }
            Compact();
            Restart();
            Verify();
        }

    private:
        static TFeatureFlags MakeFeatureFlags(bool enableHeapAllocator) {
            TFeatureFlags ff;
            ff.SetEnableVDiskHeapAllocator(enableHeapAllocator);
            return ff;
        }

        // sizes span the huge blob threshold as well as several append blocks, so that stripes of many distinct
        // lengths get allocated
        ui32 RandomBlobSize() {
            switch (Rng() % 4) {
                case 0: return 1 + Rng() % 4096;
                case 1: return 4096 + Rng() % (64 << 10);
                case 2: return (128 << 10) + Rng() % (256 << 10);
                default: return (512 << 10) + Rng() % (512 << 10);
            }
        }

        void Write(ui32 round) {
            for (ui32 step = 1; step <= BlobsPerRound; ++step) {
                const ui32 size = RandomBlobSize();
                TString data = FastGenDataForLZ4(size, Rng());
                const TLogoBlobID id(BaseTabletId + round, 1, step, 0, size, 0);
                Env.PutBlob(Info->GroupID.GetRawId(), id, data);
                Blobs.push_back({id, std::move(data), true});
            }

            // protect just written blobs with keep flags and put a barrier below them
            auto keep = std::make_unique<TVector<TLogoBlobID>>();
            for (const TBlobInfo& blob : Blobs) {
                if (blob.Id.TabletID() == BaseTabletId + round) {
                    keep->push_back(blob.Id);
                }
            }
            CollectGarbage(round, true, keep.release(), nullptr);
        }

        void Delete(ui32 round) {
            for (ui32 victimRound = 0; victimRound <= round; ++victimRound) {
                auto doNotKeep = std::make_unique<TVector<TLogoBlobID>>();
                for (TBlobInfo& blob : Blobs) {
                    if (blob.Alive && blob.Id.TabletID() == BaseTabletId + victimRound && Rng() % 3 == 0) {
                        doNotKeep->push_back(blob.Id);
                        blob.Alive = false;
                    }
                }
                if (!doNotKeep->empty()) {
                    std::sort(doNotKeep->begin(), doNotKeep->end());
                    CollectGarbage(victimRound, false, nullptr, doNotKeep.release());
                }
            }
        }

        void DeleteAll(ui32 round) {
            auto doNotKeep = std::make_unique<TVector<TLogoBlobID>>();
            for (TBlobInfo& blob : Blobs) {
                if (blob.Alive && blob.Id.TabletID() == BaseTabletId + round) {
                    doNotKeep->push_back(blob.Id);
                    blob.Alive = false;
                }
            }
            if (!doNotKeep->empty()) {
                std::sort(doNotKeep->begin(), doNotKeep->end());
                CollectGarbage(round, false, nullptr, doNotKeep.release());
            }
        }

        void CollectGarbage(ui32 round, bool collect, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep) {
            const TActorId sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            const ui32 perGenerationCounter = PerGenerationCounter[round]++;
            Env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, Info->GroupID, new TEvBlobStorage::TEvCollectGarbage(BaseTabletId + round, 1,
                    perGenerationCounter, 0, collect, collect ? 1 : 0, collect ? Max<ui32>() : 0, keep, doNotKeep,
                    TInstant::Max(), true));
            });
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        void Compact() {
            Env.Sim(TDuration::Seconds(5));
            Env.CompactVDisk(Info->GetActorId(0));
            Env.Sim(TDuration::Seconds(5));
        }

        // Restarting more than once in a row feeds a recovered state back through recovery: the stripe extents are
        // rebuilt from the hull's references, then written into the next entry point, then rebuilt again. Anything
        // dropped or double-counted on the way through shows up on the second pass rather than staying latent.
        void Restart() {
            for (ui32 i = 0; i < RestartsPerCheckpoint; ++i) {
                Env.RestartNode(Info->GetActorId(0).NodeId());
                Env.Sim(TDuration::Seconds(30));
            }
        }

        void Verify() {
            for (const TBlobInfo& blob : Blobs) {
                const TActorId sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
                Env.Runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, Info->GroupID, new TEvBlobStorage::TEvGet(blob.Id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead));
                });
                auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
                const auto& response = res->Get()->Responses[0];
                UNIT_ASSERT_VALUES_EQUAL(response.Id, blob.Id);
                if (blob.Alive) {
                    UNIT_ASSERT_VALUES_EQUAL_C(response.Status, NKikimrProto::OK, blob.Id.ToString());
                    UNIT_ASSERT_VALUES_EQUAL(response.Buffer.ConvertToString(), blob.Data);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(response.Status, NKikimrProto::NODATA, blob.Id.ToString());
                }
            }
        }
    };

}

Y_UNIT_TEST_SUITE(VDiskHeapAllocator) {

    Y_UNIT_TEST(MetadataFreshCompaction) {
        for (bool enableProjection : {false, true}) {
            TFeatureFlags ff;
            ff.SetEnableVDiskHeapAllocator(true);
            ff.SetEnableVDiskFreshSpaceProjection(enableProjection);
            TEnvironmentSetup env({
                .NodeCount = 1,
                .Erasure = TBlobStorageGroupType::ErasureNone,
                .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
                    config.HeapAllocatorMaxSstInBytes = 1_MB;
                },
                .FeatureFlags = ff,
            });
            env.CreateBoxAndPool(1, 1);
            env.Sim(TDuration::Seconds(30));
            const auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            const auto info = env.GetGroupInfo(groups.front());
            const TActorId vdiskActorId = info->GetActorId(0);
            const ui64 blockedTabletId = 1000;
            const ui32 blockedGeneration = 7;
            const ui64 collectedTabletId = 1001;
            auto deadline = [&] { return env.Runtime->GetClock() + TDuration::Minutes(1); };

            // With projection, the block and the garbage collection are admitted against chunks reserved for their
            // Fresh segments. The metadata SSTs go into heap stripes instead, so those chunks are never written, and
            // they have to go back to PDisk once the compactions are over.
            bool admitting = true;
            std::set<ui32> admitted;
            std::set<ui32> forgotten;
            ui32 metadataChunkCommits = 0;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvBlobStorage::EvChunkReserveResult:
                        if (const auto* msg = ev->Get<NPDisk::TEvChunkReserveResult>();
                                admitting && msg->Status == NKikimrProto::OK) {
                            admitted.insert(msg->ChunkIds.begin(), msg->ChunkIds.end());
                        }
                        break;
                    case TEvBlobStorage::EvChunkForget: {
                        const auto& chunks = ev->Get<NPDisk::TEvChunkForget>()->ForgetChunks;
                        forgotten.insert(chunks.begin(), chunks.end());
                        break;
                    }
                    case TEvBlobStorage::EvHullChange:
                        if (const auto* msg = dynamic_cast<THullChange<TKeyBlock, TMemRecBlock>*>(ev->GetBase())) {
                            metadataChunkCommits += msg->CommitChunks.size();
                        } else if (const auto* msg = dynamic_cast<THullChange<TKeyBarrier, TMemRecBarrier>*>(
                                ev->GetBase())) {
                            metadataChunkCommits += msg->CommitChunks.size();
                        }
                        break;
                }
                return true;
            };

            TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvBlock(blockedTabletId,
                    blockedGeneration, deadline()));
            });
            auto block = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false, deadline());
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->Get()->Status, NKikimrProto::OK);

            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(collectedTabletId, 1,
                    1, 0, true, 1, 2, nullptr, nullptr, deadline(), true));
            });
            auto gc = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false, deadline());
            UNIT_ASSERT(gc);
            UNIT_ASSERT_VALUES_EQUAL(gc->Get()->Status, NKikimrProto::OK);
            admitting = false;
            // One chunk for the Blocks segment, one for the Barriers segment.
            UNIT_ASSERT_VALUES_EQUAL(admitted.size(), enableProjection ? 2 : 0);

            // CompactVDisk() covers LogoBlobs only. Both metadata databases need a HugeKeeper destination
            // to allocate their SST stripes, even when Fresh space projection is disabled.
            for (EHullDbType db : {EHullDbType::Blocks, EHullDbType::Barriers}) {
                env.Runtime->Send(new IEventHandle(vdiskActorId, edge,
                    TEvCompactVDisk::Create(db, TEvCompactVDisk::EMode::FRESH_ONLY)), vdiskActorId.NodeId());
                auto compact = env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge, false, deadline());
                UNIT_ASSERT_C(compact, "metadata Fresh compaction timed out; db# "
                    << (db == EHullDbType::Blocks ? "Blocks" : "Barriers")
                    << " projection# " << enableProjection);
            }
            UNIT_ASSERT_VALUES_EQUAL(metadataChunkCommits, 0);
            for (const ui32 chunk : admitted) {
                UNIT_ASSERT_C(forgotten.contains(chunk), "chunk# " << chunk << " reserved for Fresh was never returned");
            }
            env.Runtime->FilterFunction = {};
            env.Runtime->DestroyActor(edge);

            env.RestartNode(vdiskActorId.NodeId());
            env.Sim(TDuration::Seconds(30));
            edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGetBlock(blockedTabletId, deadline()));
            });
            auto getBlock = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetBlockResult>(edge, false, deadline());
            UNIT_ASSERT(getBlock);
            UNIT_ASSERT_VALUES_EQUAL(getBlock->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(getBlock->Get()->BlockedGeneration, blockedGeneration);

            env.WithQueueId(info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
                env.Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVGetBarrier(
                    info->GetVDiskId(0), TKeyBarrier::First(), TKeyBarrier::Inf(), nullptr, true)), queueId.NodeId());
                auto getBarrier = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetBarrierResult>(edge, true, deadline());
                UNIT_ASSERT(getBarrier);
                const auto& record = getBarrier->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.KeysSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.ValuesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetKeys(0).GetTabletId(), collectedTabletId);
                UNIT_ASSERT_VALUES_EQUAL(record.GetKeys(0).GetChannel(), 0);
                UNIT_ASSERT_VALUES_EQUAL(record.GetValues(0).GetCollectGen(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetValues(0).GetCollectStep(), 2);
            });
        }
    }

    Y_UNIT_TEST(MetadataCompactionFromStripeToChunks) {
        for (EHullDbType db : {EHullDbType::Blocks, EHullDbType::Barriers}) {
            ui32 stripeSstBytes = 1_MB;
            TFeatureFlags ff;
            ff.SetEnableVDiskHeapAllocator(true);
            ff.SetEnableVDiskFreshSpaceProjection(true);
            ff.SetEnableTightPDiskSpaceColors(true);
            TEnvironmentSetup env({
                .NodeCount = 1,
                .Erasure = TBlobStorageGroupType::ErasureNone,
                .VDiskConfigPreprocessor = [&](TVDiskConfig& config) {
                    config.HeapAllocatorMaxSstInBytes = stripeSstBytes;
                },
                .FeatureFlags = ff,
            });
            env.CreateBoxAndPool(1, 1);
            env.Sim(TDuration::Seconds(30));
            const auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            const auto info = env.GetGroupInfo(groups.front());
            const TActorId vdiskActorId = info->GetActorId(0);
            constexpr ui64 tabletId = 1000;
            constexpr ui32 generation = 7;
            auto deadline = [&] { return env.Now() + TDuration::Minutes(1); };

            TActorId levelIndexActor;
            TDiskPart inputStripe;
            ui32 levelResults = 0;
            ui32 preCompacts = 0;
            ui32 stripeDeletions = 0;
            auto observeChange = [&](const auto* msg, const TActorId& recipient) {
                UNIT_ASSERT(!msg->Aborted);
                levelIndexActor = recipient;
                if (msg->FreshCompaction) {
                    UNIT_ASSERT(inputStripe.Empty());
                    UNIT_ASSERT_VALUES_EQUAL(msg->AllocatedStripeBlobs.Size(), 1);
                    UNIT_ASSERT(msg->CommitChunks.empty());
                    inputStripe = msg->AllocatedStripeBlobs.Vec.front();
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(stripeSstBytes, 0);
                    UNIT_ASSERT(!msg->CommitChunks.empty());
                    // None of the worker's lists would trigger PreCompact before the fix: only the input
                    // SST stripe, which the level-index actor adds later, needs a huge-heap write ID.
                    UNIT_ASSERT(msg->FreedHugeBlobs.Empty());
                    UNIT_ASSERT(msg->AllocatedHugeBlobs.Empty());
                    UNIT_ASSERT(msg->AllocatedStripeBlobs.Empty());
                    ++levelResults;
                }
            };
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvBlobStorage::EvHullChange:
                        if (db == EHullDbType::Blocks) {
                            if (const auto* msg = dynamic_cast<THullChange<TKeyBlock, TMemRecBlock>*>(ev->GetBase())) {
                                observeChange(msg, ev->Recipient);
                            }
                        } else if (const auto* msg = dynamic_cast<THullChange<TKeyBarrier, TMemRecBarrier>*>(
                                ev->GetBase())) {
                            observeChange(msg, ev->Recipient);
                        }
                        break;
                    case TEvBlobStorage::EvHugePreCompact:
                        if (levelResults && ev->Sender == levelIndexActor) {
                            ++preCompacts;
                        }
                        break;
                    case TEvBlobStorage::EvHullFreeHugeSlots: {
                        const auto* msg = ev->Get<TEvHullFreeHugeSlots>();
                        for (const auto& part : msg->HugeBlobs) {
                            if (!inputStripe.Empty() && part == inputStripe) {
                                UNIT_ASSERT(msg->WId);
                                ++stripeDeletions;
                            }
                        }
                        break;
                    }
                }
                return true;
            };

            TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                if (db == EHullDbType::Blocks) {
                    SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvBlock(tabletId, generation, deadline()));
                } else {
                    SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(tabletId, generation,
                        1, 0, true, generation, 2, nullptr, nullptr, deadline(), true));
                }
            });
            if (db == EHullDbType::Blocks) {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false, deadline());
                UNIT_ASSERT(res);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            } else {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false, deadline());
                UNIT_ASSERT(res);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }

            auto compact = [&](TEvCompactVDisk::EMode mode) {
                env.Runtime->Send(new IEventHandle(vdiskActorId, edge, TEvCompactVDisk::Create(db, mode)),
                    vdiskActorId.NodeId());
                UNIT_ASSERT_C(env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge, false, deadline()),
                    "metadata compaction timed out; db# " << static_cast<ui32>(db));
            };
            compact(TEvCompactVDisk::EMode::FRESH_ONLY);
            UNIT_ASSERT(!inputStripe.Empty());

            // Recover the striped input with stripe SST output disabled. This reproduces the same transition
            // as planned compaction without depending on its feature flag or PDisk arbitration protocol.
            stripeSstBytes = 0;
            env.Runtime->DestroyActor(edge);
            env.RestartNode(vdiskActorId.NodeId());
            env.Sim(TDuration::Seconds(30));
            edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            compact(TEvCompactVDisk::EMode::FULL);
            const TInstant until = deadline();
            while (!stripeDeletions) {
                UNIT_ASSERT_C(env.Now() < until, "input SST stripe was not freed");
                env.Sim(TDuration::Seconds(1));
            }
            UNIT_ASSERT(levelResults);
            UNIT_ASSERT(preCompacts);
            UNIT_ASSERT_VALUES_EQUAL(stripeDeletions, 1);
            env.Runtime->FilterFunction = {};
            env.Runtime->DestroyActor(edge);

            env.RestartNode(vdiskActorId.NodeId());
            env.Sim(TDuration::Seconds(30));
            edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            if (db == EHullDbType::Blocks) {
                env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGetBlock(tabletId, deadline()));
                });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetBlockResult>(edge, true, deadline());
                UNIT_ASSERT(res);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->BlockedGeneration, generation);
            } else {
                env.WithQueueId(info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
                    env.Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVGetBarrier(
                        info->GetVDiskId(0), TKeyBarrier::First(), TKeyBarrier::Inf(), nullptr, true)), queueId.NodeId());
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetBarrierResult>(edge, true, deadline());
                    UNIT_ASSERT(res);
                    const auto& record = res->Get()->Record;
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(record.KeysSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(record.ValuesSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetKeys(0).GetTabletId(), tabletId);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetKeys(0).GetChannel(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetValues(0).GetCollectGen(), generation);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetValues(0).GetCollectStep(), 2);
                });
            }
        }
    }

    Y_UNIT_TEST(RandomWorkloadHeapOff) {
        for (ui64 seed = 1; seed <= 3; ++seed) {
            TWorkload(false, seed).Run();
        }
    }

    Y_UNIT_TEST(RandomWorkloadHeapOn) {
        for (ui64 seed = 1; seed <= 3; ++seed) {
            TWorkload(true, seed).Run();
        }
    }

    Y_UNIT_TEST(RandomWorkloadHeapOnRepeatedRestarts) {
        for (ui64 seed = 1; seed <= 2; ++seed) {
            TWorkload(true, seed, 3).Run();
        }
    }

    // Whether a disk address is a slot or a stripe is decided by which heap owns its chunk, and chunks move between
    // the heaps as they fill up and empty out. With the keeper's entry point frozen, every one of those moves has to
    // be reconstructed from the log, so this is where replay's picture of chunk ownership gets tested. The chunks are
    // kept small on purpose: the workload then recycles them many times over rather than living inside one or two.
    Y_UNIT_TEST(RandomWorkloadHeapOnStaleKeeperEntryPoint) {
        for (ui64 seed = 1; seed <= 2; ++seed) {
            TWorkload(true, seed, 1, true, 32_MB).Run();
        }
    }

}
