#include "env.h"
#include <ydb/core/util/lz4_data_generator.h>
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

}
