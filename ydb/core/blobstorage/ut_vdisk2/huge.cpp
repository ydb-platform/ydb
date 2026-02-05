#include "env.h"
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hullop/hullop_entryserialize.h>
#include <contrib/libs/xxhash/xxhash.h>

using namespace NKikimr;

ui64 Hash(const TString& data) {
    return XXH64(data.data(), data.size(), 1);
}

Y_UNIT_TEST_SUITE(VDiskTest) {

    Y_UNIT_TEST(HugeBlobWrite) {
        const TInstant started = TInstant::Now();
        const TInstant end = started + TDuration::Seconds(FromString<int>(GetEnv("TIMEOUT", "540")));
        const bool doValidate = FromString<int>(GetEnv("VALIDATE", "1"));
        const ui64 seed = FromString<ui64>(GetEnv("SEED", ToString(RandomNumber<ui64>())));
        SetRandomSeed(seed);
        Cerr << "RandomSeed# " << seed << Endl;
        std::optional<TTestEnv> env(std::in_place);

        std::vector<ui32> minHugeBlobValues = {4_KB, 8_KB, 12_KB, 16_KB, 32_KB, 64_KB, 96_KB, 128_KB, 192_KB, 256_KB,
            384_KB, 512_KB};

        std::set<TLogoBlobID> content;

        auto validateBlob = [&](const TLogoBlobID& id) {
            auto res = env->Get(id);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res.ResultSize(), 1);
            const auto& value = res.GetResult(0);
            UNIT_ASSERT_VALUES_EQUAL(value.GetStatus(), NKikimrProto::OK);
            const TString expected = FastGenDataForLZ4(id.BlobSize(), id.Hash());
            UNIT_ASSERT_EQUAL_C(value.GetBufferData(), expected, "id# " << id
                << " gotHash# " << Hash(value.GetBufferData())
                << " expectedHash# " << Hash(expected));
        };

        auto validate = [&] {
            for (const TLogoBlobID& id : content) {
                validateBlob(id);
            }
        };

        std::vector<ui64> tabletIds;
        for (ui32 i = 0; i < 100; ++i) {
            tabletIds.push_back(i + 1);
        }

        struct TTabletContext {
            std::pair<ui32, ui32> Barrier;
            std::pair<ui32, ui32> IssuedBarrier;
            TVector<TLogoBlobID> Keep;
            TVector<TLogoBlobID> DoNotKeep;
            ui32 GarbageCounter = 0;
            ui32 Gen = 1, Step = 1;
        };
        std::unordered_map<ui64, TTabletContext> tablets;

        ui64 maxTotalSize = 32_GB;
        ui64 minTotalSize = 24_GB;
        ui64 totalSize = 0;
        ui8 channel = 0;
        ui32 lastMinHugeBlobValue = 0;

        while (TInstant::Now() < end) {
            const ui64 tabletId = tabletIds[RandomNumber(tabletIds.size())];
            TTabletContext& tablet = tablets[tabletId];

            const size_t blobSize = 1 + RandomNumber<size_t>(640_KB);
            TLogoBlobID id(tabletId, tablet.Gen, tablet.Step++, channel, blobSize, 0, 1);
            TString data = FastGenDataForLZ4(id.BlobSize(), id.Hash());

            auto res = env->Put(id, data);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);

            const auto [it, inserted] = content.emplace(id);
            UNIT_ASSERT(inserted);
            totalSize += data.size();

            Cerr << "Put id# " << id << " totalSize# " << totalSize << " blobs# " << content.size()
                << " hash# " << Hash(data)
                << Endl;

            if (RandomNumber(1000u) < 33) {
                ui32 minHugeBlobValue;
                do {
                    minHugeBlobValue = minHugeBlobValues[RandomNumber(minHugeBlobValues.size())];
                } while (minHugeBlobValue == lastMinHugeBlobValue);
                lastMinHugeBlobValue = minHugeBlobValue;
                env->ChangeMinHugeBlobSize(minHugeBlobValue);
                Cerr << "Change MinHugeBlobSize# " << minHugeBlobValue << Endl; 
            }

            if (totalSize > maxTotalSize || (totalSize >= minTotalSize && RandomNumber(1000u) < 1)) {
                std::vector<TLogoBlobID> options;
                options.reserve(content.size());
                for (const TLogoBlobID& id : content) {
                    options.push_back(id);
                }

                const ui64 aim = RandomNumber(totalSize);
                while (totalSize > aim && !options.empty()) {
                    size_t index = RandomNumber(options.size());
                    TLogoBlobID& id = options[index];
                    const auto& genstep = std::make_pair(id.Generation(), id.Step());

                    validateBlob(id);
                    content.erase(id);
                    totalSize -= id.BlobSize();
                    Cerr << "Erase id# " << id << " totalSize# " << totalSize << " blobs# " << content.size() << Endl;

                    TTabletContext& tablet = tablets[id.TabletID()];
                    tablet.Barrier = std::max(tablet.Barrier, genstep);
                    if (genstep <= tablet.IssuedBarrier) {
                        tablet.DoNotKeep.push_back(id.FullID());
                    }

                    std::swap(id, options.back());
                    options.pop_back();
                }
                for (const TLogoBlobID& id : content) {
                    const auto& genstep = std::make_pair(id.Generation(), id.Step());
                    TTabletContext& tablet = tablets[id.TabletID()];
                    if (tablet.IssuedBarrier < genstep && genstep <= tablet.Barrier) {
                        tablet.Keep.push_back(id.FullID());
                    }
                }

                for (auto& [tabletId, tablet] : tablets) {
                    if (tablet.Barrier != tablet.IssuedBarrier || !tablet.DoNotKeep.empty()) {
                        auto res = env->Collect(tabletId, tablet.Gen, ++tablet.GarbageCounter, channel, tablet.Barrier,
                            false, std::exchange(tablet.Keep, {}), std::exchange(tablet.DoNotKeep, {}));
                        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
                        tablet.IssuedBarrier = tablet.Barrier;
                    } else {
                        UNIT_ASSERT(tablet.Keep.empty());
                    }
                }
                Cerr << "CollectGarbage content.size# " << content.size() << " totalSize# " << totalSize << Endl;
            }

            if (RandomNumber(1000u) < 150) {
                Cerr << "Trim" << Endl;
                const TActorId& edge = env->GetRuntime()->AllocateEdgeActor(1);
                env->GetRuntime()->WrapInActorContext(edge, [&] {
                    env->GetPDiskMockState()->TrimQuery();
                });
                env->GetRuntime()->DestroyActor(edge);
            }

            if (RandomNumber(1000u) < 1) {
                Cerr << "Restart" << Endl;
                ui32 numEventsToSim = RandomNumber(50u);
                env->GetRuntime()->Sim([&] { return numEventsToSim--; });
                env.emplace(env->GetPDiskMockState());
            }

            if (RandomNumber(1000u) < 10) {
                Cerr << "Compact" << Endl;
                env->Compact(RandomNumber(2u));
            }

            if (RandomNumber(10000u) < 1 && doValidate) {
                Cerr << "Validate" << Endl;
                validate();
            }
        }

        if (doValidate) {
            Cerr << "Validate before exit" << Endl;
            validate();
        }
    }

    Y_UNIT_TEST(HugeBlobRecompaction) {
        SetRandomSeed(FromString<int>(GetEnv("SEED", "1")));
        std::optional<TTestEnv> env(std::in_place);

        TString blobValue = TString::Uninitialized(69541);

        auto changeMinHugeBlobSize = [&env](ui32 size) {
            env->ChangeMinHugeBlobSize(size);
            Cerr << "Change MinHugeBlobSize# " << size << Endl;
        };

        ui32 minHugeBlobSizeBefore = 100_KB;
        ui32 minHugeBlobSizeAfter = 32513;

        {
            char* data = blobValue.Detach();
            static const char pattern[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            const size_t patternLen = sizeof(pattern) - 1;

            size_t offset = 0;
            size_t remaining = blobValue.size();

            while (remaining >= patternLen) {
                memcpy(data + offset, pattern, patternLen);
                offset += patternLen;
                remaining -= patternLen;
            }

            if (remaining) {
                memcpy(data + offset, pattern, remaining);
            }
        }

        changeMinHugeBlobSize(minHugeBlobSizeBefore);

        {
            ui32 step = 1;

            for (size_t i = 0; i < 100; i++) {
                TLogoBlobID id(1, 1, step++, 0, blobValue.size(), 0, 1);

                auto res = env->Put(id, blobValue);
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
            }
        }

        auto getCounters = [&env](ui8 level) {
            auto vdiskCounters = env->GetCounters()->GetSubgroup("subsystem", "vdisk");
            auto vdiskSub = vdiskCounters->GetSubgroup("counters", "vdisks")
                ->GetSubgroup("storagePool", "static")
                ->GetSubgroup("group", "000000000")
                ->GetSubgroup("orderNumber", "00")
                ->GetSubgroup("pdisk", "000000001")
                ->GetSubgroup("media", "ssd");
            auto levels = vdiskSub->GetSubgroup("subsystem", "levels");
            auto levelSub = levels->GetSubgroup("level", ToString(level));
            ui64 numItemsHuge = levelSub->FindCounter("NumItemsHuge")->GetAtomic();
            ui64 numItemsInplaced = levelSub->FindCounter("NumItemsInplaced")->GetAtomic();
            return std::make_tuple(numItemsHuge, numItemsInplaced);
        };

        auto checkBlobs = [&env, &blobValue]() {
            ui32 step = 1;

            for (size_t i = 0; i < 100; i++) {
                TLogoBlobID id(1, 1, step++, 0, blobValue.size(), 0, 1);

                NKikimrBlobStorage::TEvVGetResult res = env->Get(id);
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res.ResultSize(), 1);
                const auto& value = res.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL(value.GetStatus(), NKikimrProto::OK);
                TString bufferData = value.GetBufferData();

                if (bufferData != blobValue) {
                    UNIT_ASSERT_C(false, "Mismatch for id# " << id);
                }
            }
        };

        checkBlobs();

        env->Compact(true); // Fresh only

        {
            auto [numItemsHuge, numItemsInplaced] = getCounters(0);
            UNIT_ASSERT_VALUES_EQUAL(numItemsHuge, 0);
            UNIT_ASSERT_VALUES_EQUAL(numItemsInplaced, 100);
        }

        checkBlobs();

        env->Compact();

        {
            auto [numItemsHuge, numItemsInplaced] = getCounters(17);
            UNIT_ASSERT_VALUES_EQUAL(numItemsHuge, 0);
            UNIT_ASSERT_VALUES_EQUAL(numItemsInplaced, 100);
        }

        changeMinHugeBlobSize(minHugeBlobSizeAfter);

        {
            auto [numItemsHuge, numItemsInplaced] = getCounters(17);
            UNIT_ASSERT_VALUES_EQUAL(numItemsHuge, 0);
            UNIT_ASSERT_VALUES_EQUAL(numItemsInplaced, 100);
        }

        checkBlobs();

        env->Compact();

        {
            auto [numItemsHuge, numItemsInplaced] = getCounters(17);
            UNIT_ASSERT_VALUES_EQUAL(numItemsHuge, 100);
            UNIT_ASSERT_VALUES_EQUAL(numItemsInplaced, 0);
        }

        checkBlobs();
    }

    void TestHugeBlobAllocationRace(bool eraseOld, bool writeBeforeRestart) {
        SetRandomSeed(FromString<int>(GetEnv("SEED", "1")));

        constexpr ui32 chunkSize = 64_MB;
        TIntrusivePtr<TPDiskMockState> pdisk = new TPDiskMockState(1, 1, 1, (ui64)10 << 40, chunkSize);
        std::optional<TTestEnv> env(std::in_place, pdisk);

        auto changeMinHugeBlobSize = [&env](ui32 size) {
            env->ChangeMinHugeBlobSize(size);
        };

        const ui32 minHugeBefore = 10_MB;
        const ui32 minHugeAfter = 2_MB;
        const ui32 blobSize = 4_MB;

        const ui64 tabletId = 1;
        const ui32 gen = 1;
        const ui8 channel = 0;
        ui32 step = 1;

        auto makeData = [](const TLogoBlobID& id) {
            return FastGenDataForLZ4(id.BlobSize(), id.Hash());
        };

        auto simEvents = [&env](ui32 n) {
            ui32 numEventsToSim = n;
            env->GetRuntime()->Sim([&] {
                return numEventsToSim--;
            });
        };

        changeMinHugeBlobSize(minHugeBefore);

        std::array<TLogoBlobID, 2> oldIds;
        std::array<TString, 2> oldData;
        for (size_t i = 0; i < oldIds.size(); ++i) {
            TLogoBlobID id(tabletId, gen, step++, channel, blobSize, 0, 1);
            TString data = makeData(id);
            auto res = env->Put(id, data);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
            oldIds[i] = id;
            oldData[i] = std::move(data);
        }

        env->Compact(true);

        bool sawAllocatedOnly = false;
        bool droppedAllocatedOnly = false;
        bool allocatedOnlyWIdSet = false;
        ui64 allocatedOnlyWId = 0;
        auto& runtime = *env->GetRuntime();
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvHullFreeHugeSlots) {
                if (auto* msg = ev->CastAsLocal<TEvHullFreeHugeSlots>()) {
                    if (msg->HugeBlobs.Empty() && !msg->AllocatedBlobs.Empty()) {
                        sawAllocatedOnly = true;
                        if (!allocatedOnlyWIdSet) {
                            allocatedOnlyWIdSet = true;
                            allocatedOnlyWId = msg->WId;
                            if (!allocatedOnlyWId && !droppedAllocatedOnly) {
                                droppedAllocatedOnly = true;
                                return false; // simulate crash before huge keeper sees allocated slots
                            }
                        }
                    }
                }
            }
            return true;
        };

        changeMinHugeBlobSize(minHugeAfter);
        env->Compact();

        for (ui32 i = 0; i < 200 && !sawAllocatedOnly; ++i) {
            simEvents(20);
        }
        UNIT_ASSERT_C(sawAllocatedOnly, "no AllocatedHugeBlobs-only FreeHugeSlots");

        const TVDiskID vdiskId(0, 1, 0, 0, 0);
        const auto ownerRound = env->GetPDiskMockState()->GetOwnerRound(vdiskId);
        UNIT_ASSERT_C(ownerRound, "no PDisk owner round for VDisk");
        const ui8 ownerId = 1;
        const TActorId pdiskActorId = MakeBlobStoragePDiskID(1, 1);

        auto readLog = [&]() {
            TVector<NPDisk::TLogRecord> records;
            NPDisk::TLogPosition pos{0, 0};
            while (true) {
                const TActorId& edge = env->GetRuntime()->AllocateEdgeActor(1);
                env->GetRuntime()->Send(new IEventHandle(pdiskActorId, edge,
                    new NPDisk::TEvReadLog(ownerId, *ownerRound, pos)), 1);
                auto ev = env->GetRuntime()->WaitForEdgeActorEvent({edge});
                env->GetRuntime()->DestroyActor(edge);
                auto *msg = ev->CastAsLocal<NPDisk::TEvReadLogResult>();
                UNIT_ASSERT(msg);
                UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
                records.insert(records.end(), msg->Results.begin(), msg->Results.end());
                if (msg->IsEndOfLog) {
                    break;
                }
                pos = msg->NextPosition;
            }
            return records;
        };

        auto hasAllocatedEntryPoint = [&]() {
            for (const NPDisk::TLogRecord& rec : readLog()) {
                if (rec.Signature != TLogSignature::SignatureHullLogoBlobsDB) {
                    continue;
                }
                NKikimrVDiskData::THullDbEntryPoint pb;
                TString explanation;
                if (!THullDbSignatureRoutines::ParseArray(pb, rec.Data.GetData(), rec.Data.GetSize(), explanation)) {
                    continue;
                }
                if (pb.GetAllocatedHugeBlobs().DiskPartsSize() > 0) {
                    return true;
                }
            }
            return false;
        };

        if (!allocatedOnlyWId) {
            UNIT_ASSERT_C(droppedAllocatedOnly, "expected to drop AllocatedHugeBlobs-only FreeHugeSlots");
            bool haveAllocatedEntryPoint = false;
            for (ui32 i = 0; i < 50 && !haveAllocatedEntryPoint; ++i) {
                haveAllocatedEntryPoint = hasAllocatedEntryPoint();
                if (!haveAllocatedEntryPoint) {
                    simEvents(200);
                }
            }
            UNIT_ASSERT_C(haveAllocatedEntryPoint, "no entrypoint with AllocatedHugeBlobs after compaction");

            bool trimmed = false;
            for (ui32 i = 0; i < 50; ++i) {
                if (!hasAllocatedEntryPoint()) {
                    trimmed = true;
                    break;
                }
                const TActorId& edge = env->GetRuntime()->AllocateEdgeActor(1);
                env->GetRuntime()->WrapInActorContext(edge, [&] {
                    env->GetPDiskMockState()->TrimQuery();
                });
                env->GetRuntime()->DestroyActor(edge);
                simEvents(200);
            }
            UNIT_ASSERT_C(trimmed, "failed to trim entrypoint with AllocatedHugeBlobs from log");
        }

        if (writeBeforeRestart) {
            std::array<TLogoBlobID, 2> newIds;
            std::array<TString, 2> newData;
            for (size_t i = 0; i < newIds.size(); ++i) {
                TLogoBlobID id(tabletId, gen, step++, channel, blobSize, 0, 1);
                TString data = makeData(id);
                auto res = env->Put(id, data);
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
                newIds[i] = id;
                newData[i] = std::move(data);
            }
        }

        if (eraseOld) {
            ui32 garbageCounter = 0;
            for (size_t i = 0; i < oldIds.size(); ++i) {
                TLogoBlobID id = oldIds[i];
                auto res = env->Collect(tabletId, gen, ++garbageCounter, channel,
                    std::make_pair(id.Generation(), id.Step()), true, {}, {id.FullID()});
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
            }

            env->Compact(false);

            simEvents(100);
        }

        env.emplace(env->GetPDiskMockState());
        changeMinHugeBlobSize(minHugeAfter);

        const size_t newWrites = 32;
        for (size_t i = 0; i < newWrites; ++i) {
            TLogoBlobID id(tabletId, gen, step++, channel, blobSize, 0, 1);
            TString data = makeData(id);
            auto res = env->Put(id, data);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
        }

        if (!eraseOld) {
            for (size_t i = 0; i < oldIds.size(); ++i) {
                NKikimrBlobStorage::TEvVGetResult res = env->Get(oldIds[i]);
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res.ResultSize(), 1);
                const auto& value = res.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL(value.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_EQUAL_C(value.GetBufferData(), oldData[i], "Mismatch for id# " << oldIds[i]);
            }
        }
    }

    Y_UNIT_TEST(HugeBlobAllocationRace) {
        TestHugeBlobAllocationRace(false, false);
        TestHugeBlobAllocationRace(false, true);
        TestHugeBlobAllocationRace(true, false);
        TestHugeBlobAllocationRace(true, true);
    }

}
