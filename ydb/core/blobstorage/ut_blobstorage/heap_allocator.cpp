#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullactor.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullcompact.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

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
                    .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
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

    TFeatureFlags HeapAllocatorFlag() {
        TFeatureFlags ff;
        ff.SetEnableVDiskHeapAllocator(true);
        return ff;
    }

    // The "state" counters of the VDisk, under whichever pool name it started with.
    ::NMonitoring::TDynamicCounterPtr VDiskStateCounters(TEnvironmentSetup& env, ui32 groupId, ui32 orderNumber,
            const TActorId& vdiskActorId) {
        ui32 nodeId, pdiskId;
        std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);
        const std::pair<TString, TString> chain[] = {
            {"group", Sprintf("%09" PRIu32, groupId)},
            {"orderNumber", Sprintf("%02" PRIu32, orderNumber)},
            {"pdisk", Sprintf("%09" PRIu32, pdiskId)},
            {"media", "rot"},
            {"subsystem", "state"},
        };
        const auto vdisks = GetServiceCounters(env.Runtime->GetNode(nodeId)->AppData->Counters, "vdisks");
        std::vector<::NMonitoring::TDynamicCounterPtr> found;
        vdisks->EnumerateSubgroups([&](const TString& poolLabel, const TString& poolName) {
            auto counters = vdisks->FindSubgroup(poolLabel, poolName);
            for (const auto& [label, value] : chain) {
                if (counters) {
                    counters = counters->FindSubgroup(label, value);
                }
            }
            if (counters) {
                found.push_back(counters);
            }
        });
        UNIT_ASSERT_VALUES_EQUAL_C(found.size(), 1, "VDisk# " << vdiskActorId << " orderNumber# " << orderNumber);
        return found.front();
    }

    // One character per VDisk of the group in order-number order, as its gauges report the heap it latched at start:
    // '1' for the stripe heap, '0' for the size-class one.
    TString HeapModes(TEnvironmentSetup& env, ui32 groupId) {
        const auto info = env.GetGroupInfo(groupId);
        TString modes;
        for (ui32 orderNumber = 0; orderNumber < info->GetTotalVDisksNum(); ++orderNumber) {
            const auto state = VDiskStateCounters(env, groupId, orderNumber, info->GetActorId(orderNumber));
            const i64 sizeClass = state->GetCounter("HeapAllocatorSizeClass")->Val();
            const i64 stripe = state->GetCounter("HeapAllocatorStripe")->Val();
            UNIT_ASSERT_VALUES_EQUAL_C(sizeClass + stripe, 1, "orderNumber# " << orderNumber);
            modes.push_back(stripe ? '1' : '0');
        }
        return modes;
    }

    // Restarts just these VDisks, so that each one latches the record its NodeWarden holds now.
    void RestartVDisks(TEnvironmentSetup& env, ui32 groupId, std::initializer_list<ui32> orderNumbers) {
        const auto info = env.GetGroupInfo(groupId);
        for (ui32 orderNumber : orderNumbers) {
            ui32 nodeId, pdiskId;
            std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(info->GetActorId(orderNumber));
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {},
                new TEvBlobStorage::TEvAskRestartVDisk(pdiskId, info->GetVDiskId(orderNumber))), nodeId);
        }
        env.Sim(TDuration::Seconds(30));
    }

    NKikimrBlobStorage::TDefineStoragePool ReadStoragePool(TEnvironmentSetup& env, ui64 storagePoolId = 1) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableReadStoragePool();
        cmd->SetBoxId(1);
        cmd->AddStoragePoolId(storagePoolId);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 1);
        return response.GetStatus(0).GetStoragePool(0);
    }

    void DefineStoragePool(TEnvironmentSetup& env, const NKikimrBlobStorage::TDefineStoragePool& pool) {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // nullopt resets the setting, so that the pool inherits the global value again
    void FillUpdateStoragePoolSettings(NKikimrBlobStorage::TUpdateStoragePoolSettings *cmd,
            std::optional<ui32> numLeadingDisks, ui64 storagePoolId = 1) {
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(storagePoolId);
        if (numLeadingDisks) {
            cmd->MutableSettings()->SetVDiskHeapAllocatorNumLeadingDisks(*numLeadingDisks);
        } else {
            cmd->AddReset("VDiskHeapAllocatorNumLeadingDisks");
        }
    }

    void UpdateStoragePoolSettings(TEnvironmentSetup& env, std::optional<ui32> numLeadingDisks, ui64 storagePoolId = 1) {
        NKikimrBlobStorage::TConfigRequest request;
        FillUpdateStoragePoolSettings(request.AddCommand()->MutableUpdateStoragePoolSettings(), numLeadingDisks,
            storagePoolId);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    ui64 VDiskStripeHeapUsedBytes(TEnvironmentSetup& env, const TActorId& vdiskActorId) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(vdiskActorId.NodeId(), __FILE__, __LINE__);
        auto request = std::make_unique<TEvGetVDiskSpaceReportRequest>();
        request->Record.SetForceRecalculation(true);
        env.Runtime->Send(new IEventHandle(vdiskActorId, edge, request.release()), edge.NodeId());
        const auto res = env.WaitForEdgeActorEvent<TEvGetVDiskSpaceReportResponse>(edge);
        const auto& record = res->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::OK));
        return record.GetReport().GetStripeHeap().GetUsedBytes();
    }

    std::vector<ui64> StripeHeapUsedBytes(TEnvironmentSetup& env, ui32 groupId) {
        const auto info = env.GetGroupInfo(groupId);
        std::vector<ui64> used;
        for (ui32 orderNumber = 0; orderNumber < info->GetTotalVDisksNum(); ++orderNumber) {
            used.push_back(VDiskStripeHeapUsedBytes(env, info->GetActorId(orderNumber)));
        }
        return used;
    }

    // Reads every part the VDisk keeps of these blobs straight from it and compares them with the erasure split.
    // Returns the number of parts found.
    ui32 CheckLocalParts(TEnvironmentSetup& env, ui32 groupId, ui32 orderNumber,
            const std::vector<std::pair<TLogoBlobID, TString>>& blobs) {
        const auto info = env.GetGroupInfo(groupId);
        const TVDiskID vdiskId = info->GetVDiskId(orderNumber);
        ui32 numParts = 0;
        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
            for (const auto& [id, data] : blobs) {
                TDataPartSet parts;
                info->Type.SplitData(static_cast<TBlobStorageGroupType::ECrcMode>(id.CrcMode()), data, parts);
                const TActorId edge = env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                env.Runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                    vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::None, Nothing(), {{id, 0, 0}}).release()), queueId.NodeId());
                const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                const auto& record = res->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                for (const auto& result : record.GetResult()) {
                    if (result.GetStatus() == NKikimrProto::NODATA) {
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrProto::OK, id);
                    const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                    UNIT_ASSERT_C(partId.PartId(), partId);
                    const TString expected = parts.Parts[partId.PartId() - 1].OwnedString.ConvertToString();
                    UNIT_ASSERT_C(res->Get()->GetBlobData(result).ConvertToString() ==
                        expected.substr(0, info->Type.PartSize(partId)), partId);
                    ++numParts;
                }
            }
        });
        return numParts;
    }

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
                .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
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
                .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
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

    Y_UNIT_TEST(FlagWithoutKnobStaysSizeClass) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .FeatureFlags = HeapAllocatorFlag(),
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, env.GetGroups().front()), "0");
        UNIT_ASSERT(!ReadStoragePool(env).HasSettings());
    }

    // Neither the global value nor a pool setting enables the stripe heap without the feature flag.
    Y_UNIT_TEST(KnobWithoutFlagStaysSizeClass) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "0");

        UpdateStoragePoolSettings(env, Max<ui32>());
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "0");
    }

    // A pool setting replaces the global value, including 0, and only the first N VDisks of each group take the
    // stripe heap. A running VDisk keeps the mode it latched. A VDisk-only restart reads the record that
    // BS_CONTROLLER pushed to the live node, so this covers the push itself. A reset brings the global value back.
    Y_UNIT_TEST(PoolSettingReplacesGlobalValue) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .FeatureFlags = HeapAllocatorFlag(),
            .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 generation = env.GetGroupInfo(groupId)->GroupGeneration;
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "11111111");

        UpdateStoragePoolSettings(env, 1);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env).GetSettings().GetVDiskHeapAllocatorNumLeadingDisks(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "11111111");

        RestartVDisks(env, groupId, {1});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "10111111");
        RestartVDisks(env, groupId, {0, 2, 3, 4, 5, 6, 7});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "10000000");

        UpdateStoragePoolSettings(env, 0);
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "00000000");

        UpdateStoragePoolSettings(env, std::nullopt);
        UNIT_ASSERT(!ReadStoragePool(env).HasSettings());
        RestartVDisks(env, groupId, {0, 1, 2, 3, 4, 5, 6, 7});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "11111111");
        UNIT_ASSERT_VALUES_EQUAL(env.GetGroupInfo(groupId)->GroupGeneration, generation);
    }

    // DefineStoragePool, as Console issues it from its own template, neither carries nor resets the settings.
    Y_UNIT_TEST(DefineStoragePoolKeepsSettings) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        UpdateStoragePoolSettings(env, 2);

        auto pool = ReadStoragePool(env);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetSettings().GetVDiskHeapAllocatorNumLeadingDisks(), 2u);
        pool.ClearSettings();
        DefineStoragePool(env, pool);
        pool = ReadStoragePool(env);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetSettings().GetVDiskHeapAllocatorNumLeadingDisks(), 2u);

        pool.MutableSettings()->SetVDiskHeapAllocatorNumLeadingDisks(5);
        DefineStoragePool(env, pool);
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env).GetSettings().GetVDiskHeapAllocatorNumLeadingDisks(), 2u);

        auto expectError = [&](std::optional<ui32> numLeadingDisks, ui64 storagePoolId, const TString& reset,
                const TString& error) {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableUpdateStoragePoolSettings();
            FillUpdateStoragePoolSettings(cmd, numLeadingDisks, storagePoolId);
            if (reset) {
                cmd->AddReset(reset);
            }
            const auto response = env.Invoke(request);
            UNIT_ASSERT(!response.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), error);
        };
        expectError(3, 1, "VDiskHeapAllocatorNumLeadingDisks", "both set and reset");
        expectError(3, 1, "NoSuchSetting", "unknown setting");
        expectError(3, 42, {}, "not found");
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env).GetSettings().GetVDiskHeapAllocatorNumLeadingDisks(), 2u);
    }

    // RestartPDisk in the same request touches the VSlots of the PDisk without sending their records. The new
    // setting still has to reach those VDisks before they start again.
    Y_UNIT_TEST(PoolSettingWithPDiskRestartInOneRequest) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .FeatureFlags = HeapAllocatorFlag(),
            .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "1");

        ui32 nodeId, pdiskId;
        std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(env.GetGroupInfo(groupId)->GetActorId(0));
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreDegradedGroupsChecks(true);
        request.SetIgnoreDisintegratedGroupsChecks(true);
        request.SetIgnoreGroupFailModelChecks(true);
        FillUpdateStoragePoolSettings(request.AddCommand()->MutableUpdateStoragePoolSettings(), 0);
        auto *target = request.AddCommand()->MutableRestartPDisk()->MutableTargetPDiskId();
        target->SetNodeId(nodeId);
        target->SetPDiskId(pdiskId);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "0");
    }

    // MoveGroups changes the pool of a group without a new group generation. Its VDisk records are pushed again, so
    // that a VDisk-only restart latches the target pool's value.
    Y_UNIT_TEST(PoolSettingFollowsMovedGroup) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .FeatureFlags = HeapAllocatorFlag(),
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        UpdateStoragePoolSettings(env, 1);
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "1");

        auto target = ReadStoragePool(env);
        target.SetStoragePoolId(2);
        target.SetName("target");
        target.SetNumGroups(0);
        target.SetItemConfigGeneration(0);
        target.ClearSettings();
        DefineStoragePool(env, target);

        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableMoveGroups();
        cmd->SetBoxId(1);
        cmd->SetOriginStoragePoolId(1);
        cmd->SetOriginStoragePoolGeneration(ReadStoragePool(env, 1).GetItemConfigGeneration());
        cmd->SetTargetStoragePoolId(2);
        cmd->SetTargetStoragePoolGeneration(ReadStoragePool(env, 2).GetItemConfigGeneration());
        cmd->AddExplicitGroupId(groupId);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(10));

        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "0");
    }

    // BS_CONTROLLER does not push settings to a slot that is being wiped, and the record that asked for the wipe
    // carries the old value. The VDisk that starts after the wipe still gets a setting changed in between: NodeWarden
    // starts it from the record BS_CONTROLLER sends in reply to the WIPED report.
    Y_UNIT_TEST(PoolSettingChangedDuringWipe) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .FeatureFlags = HeapAllocatorFlag(),
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        UpdateStoragePoolSettings(env, 1);
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "1");

        // hold the wipe back at PDisk, so that the setting changes while it is in flight
        std::unique_ptr<IEventHandle> slayResult;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NPDisk::TEvSlayResult::EventType) {
                slayResult = std::move(ev); // a retried slay supersedes the previous round
                return false;
            }
            return true;
        };
        const auto info = env.GetGroupInfo(groupId);
        ui32 nodeId, pdiskId, vslotId;
        std::tie(nodeId, pdiskId, vslotId) = DecomposeVDiskServiceId(info->GetActorId(0));
        env.Wipe(nodeId, pdiskId, vslotId, info->GetVDiskId(0));
        const TInstant deadline = env.Now() + TDuration::Minutes(1);
        while (!slayResult) {
            UNIT_ASSERT_C(env.Now() < deadline, "PDisk did not answer the slay");
            env.Sim(TDuration::Seconds(1));
        }

        UpdateStoragePoolSettings(env, 0);
        env.Runtime->FilterFunction = {};
        // if this round went stale meanwhile, the next retry completes the wipe
        env.Runtime->Send(slayResult.release(), nodeId);
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "0");
    }

    // A donor gets the pool setting like any other VDisk of the group, while its mode gauges stay at zero.
    Y_UNIT_TEST(PoolSettingReachesDonor) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .FeatureFlags = HeapAllocatorFlag(),
            .VDiskHeapAllocatorNumLeadingDisks = Max<ui32>(),
        });
        env.EnableDonorMode();
        env.CreateBoxAndPool(2, 1);
        env.CommenceReplication();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const TActorId donorActorId = env.GetGroupInfo(groupId)->GetActorId(0);
        env.SettlePDisk(donorActorId);
        UNIT_ASSERT_VALUES_UNEQUAL(env.GetGroupInfo(groupId)->GetActorId(0), donorActorId);

        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "11111111");
        const auto donorState = VDiskStateCounters(env, groupId, 0, donorActorId);
        UNIT_ASSERT_VALUES_EQUAL(donorState->GetCounter("HeapAllocatorSizeClass")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(donorState->GetCounter("HeapAllocatorStripe")->Val(), 0);

        ui32 nodeId, pdiskId, vslotId;
        std::tie(nodeId, pdiskId, vslotId) = DecomposeVDiskServiceId(donorActorId);
        std::optional<NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk> donorRecord;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (const auto& vdisk : record.GetServiceSet().GetVDisks()) {
                    const auto& location = vdisk.GetVDiskLocation();
                    if (location.GetNodeID() == nodeId && location.GetPDiskID() == pdiskId &&
                            location.GetVDiskSlotID() == vslotId) {
                        donorRecord = vdisk;
                    }
                }
            }
            return true;
        };
        UpdateStoragePoolSettings(env, 1);
        env.Sim(TDuration::Seconds(5));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(donorRecord);
        UNIT_ASSERT(donorRecord->HasDonorMode());
        UNIT_ASSERT(donorRecord->HasVDiskHeapAllocatorNumLeadingDisks());
        UNIT_ASSERT_VALUES_EQUAL(donorRecord->GetVDiskHeapAllocatorNumLeadingDisks(), 1u);
    }

    // With N = 1 only the first VDisk of the group allocates stripes. Switching it to the size-class heap and back
    // with VDisk-only restarts keeps its data readable: a size-class disk reads its old stripes and puts nothing new
    // there.
    Y_UNIT_TEST(MixedGroupAllocation) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .FeatureFlags = HeapAllocatorFlag(),
            // MinHugeBlobInBytes applies to this disk type only, and CreateBoxAndPool makes ROT drives
            .DiskType = NPDisk::EDeviceType::DEVICE_TYPE_ROT,
            .MinHugeBlobInBytes = 4096,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        UpdateStoragePoolSettings(env, 1);
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "10000000");

        std::vector<std::pair<TLogoBlobID, TString>> blobs;
        auto write = [&](ui32 count) {
            for (ui32 i = 0; i < count; ++i) {
                const ui32 step = static_cast<ui32>(blobs.size()) + 1;
                const ui32 size = (256 << 10) + step * 4096;
                const TLogoBlobID id(1000, 1, step, 0, size, 0);
                TString data = FastGenDataForLZ4(size, step);
                env.PutBlob(groupId, id, data);
                blobs.emplace_back(id, std::move(data));
            }
            env.Sim(TDuration::Seconds(5));
        };

        write(16);
        const auto used = StripeHeapUsedBytes(env, groupId);
        UNIT_ASSERT(used[0]);
        for (ui32 orderNumber = 1; orderNumber < used.size(); ++orderNumber) {
            UNIT_ASSERT_VALUES_EQUAL_C(used[orderNumber], 0u, orderNumber);
        }
        const ui32 numParts = CheckLocalParts(env, groupId, 0, blobs);
        UNIT_ASSERT(numParts);

        UpdateStoragePoolSettings(env, 0);
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "00000000");
        UNIT_ASSERT_VALUES_EQUAL(CheckLocalParts(env, groupId, 0, blobs), numParts);
        write(16);
        UNIT_ASSERT_VALUES_EQUAL(StripeHeapUsedBytes(env, groupId)[0], used[0]);
        const ui32 numPartsDisabled = CheckLocalParts(env, groupId, 0, blobs);
        UNIT_ASSERT(numPartsDisabled > numParts);

        UpdateStoragePoolSettings(env, 1);
        RestartVDisks(env, groupId, {0});
        UNIT_ASSERT_VALUES_EQUAL(HeapModes(env, groupId), "10000000");
        UNIT_ASSERT_VALUES_EQUAL(CheckLocalParts(env, groupId, 0, blobs), numPartsDisabled);
        write(8);
        UNIT_ASSERT(StripeHeapUsedBytes(env, groupId)[0] > used[0]);
        UNIT_ASSERT(CheckLocalParts(env, groupId, 0, blobs) > numPartsDisabled);
    }
}
