#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>

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
