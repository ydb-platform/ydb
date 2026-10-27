#include "hullds_sst_packed.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

namespace NKikimr {

    namespace {

        using TRec = TPackedLogoBlobSstIndex::TRec;
        using TIterator = TPackedLogoBlobSstIndex::TIterator;

        const TBlobStorageGroupType GType(TErasureType::Erasure4Plus2Block);

        // An SST as compaction writes it: groups of about 5 steps per (tablet, channel, generation); inline blobs
        // placed one after another in data chunks, huge blobs in slots, records without data, and a few oddities.
        TTrackableVector<TRec> MakeIndex(TTestContexts& ctxs, ui32 records, ui32 seed) {
            TReallyFastRng32 rng(seed);
            TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            ui64 tablet = 72075186224037888ull;
            ui32 channel = 0, gen = 1, step = 1;
            ui32 dataChunk = 10, dataOffset = 0;
            ui32 manyHugeIndex = 0;
            while (index.size() < records) {
                switch (rng() % 16) {
                    case 0: tablet += 1 + rng() % 1000; channel = rng() % 3; gen = 1 + rng() % 10; step = rng() % 100; break;
                    case 1: ++channel; gen = 1 + rng() % 10; step = rng() % 100; break;
                    case 2: gen += 1 + rng() % 3; step = rng() % 100; break;
                    default: step += rng() % 4; break;
                }
                const ui32 blobSize = rng() % 4 ? 100 + rng() % 4000 : 1 + rng() % (8 << 20);
                const ui32 cookie = rng() % 8 ? 0 : rng() % 1000;
                const ui32 partId = 1 + rng() % 6;
                const TLogoBlobID id(tablet, gen, step, channel, blobSize, cookie, partId);
                if (!index.empty() && !(index.back().GetKey() < TKeyLogoBlob(id))) {
                    continue;
                }

                const ui64 ingress = (ui64(rng()) << 32 | rng()) & (rng() % 2 ? 0xFFFF000000000000ull : ~0ull);
                TMemRecLogoBlob memRec{TIngress(ingress)};
                const NMatrix::TVectorType parts = TIngress(ingress).LocalParts(GType);
                const ui32 size = TDiskBlob::CalculateBlobSize(GType, id.FullID(), parts,
                    rng() % 8 ? EBlobHeaderMode::XXH3_64BIT_HEADER : EBlobHeaderMode::OLD_HEADER)
                    + (rng() % 50 ? 0 : 1 + rng() % 100);
                switch (rng() % 10) {
                    case 0: case 1: case 2:
                        break; // no data
                    case 3: case 4: case 5: case 6:
                        if (dataOffset + size > (128 << 20) || rng() % 200 == 0) {
                            ++dataChunk;
                            dataOffset = rng() % 2 ? 0 : 4096;
                        }
                        memRec.SetDiskBlob(TDiskPart(dataChunk, dataOffset, size));
                        dataOffset += AlignUp<ui32>(size, 4);
                        break;
                    case 7: case 8:
                        memRec.SetHugeBlob(TDiskPart(1000 + rng() % 50, (rng() % 300) * (512 << 10), size));
                        break;
                    default:
                        if (rng() % 2) {
                            const ui32 num = 2 + rng() % 3;
                            memRec.SetManyHugeBlobs(manyHugeIndex, num, size);
                            manyHugeIndex += num;
                        } else {
                            memRec.SetMemBlob(ui64(rng()) << 20 | rng(), size);
                        }
                        break;
                }
                index.emplace_back(TKeyLogoBlob(id), memRec);
            }
            return index;
        }

        void ExpectAt(const TIterator& it, const TTrackableVector<TRec>& index, size_t i) {
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_C(it.GetCurKey() == index[i].GetKey(),
                it.GetCurKey().ToString() << " != " << index[i].GetKey().ToString());
            const TMemRecLogoBlob memRec = it.GetMemRec();
            const TMemRecLogoBlob expected = index[i].GetMemRec();
            UNIT_ASSERT(memcmp(&memRec, &expected, sizeof(memRec)) == 0);
        }

        // the least key of the (tablet, channel, generation) group after the record's, which the index may not hold
        std::optional<TLogoBlobID> NextGroupKey(const TLogoBlobID& id) {
            ui64 tablet = id.TabletID();
            ui64 channelGeneration = (ui64(id.Channel()) << 32 | id.Generation()) + 1;
            if (channelGeneration >> 40) {
                if (tablet == Max<ui64>()) {
                    return std::nullopt;
                }
                ++tablet;
                channelGeneration = 0;
            }
            return TLogoBlobID(tablet, ui32(channelGeneration), 0, channelGeneration >> 32, 0, 0);
        }

        void CheckIndex(TTestContexts& ctxs, const TTrackableVector<TRec>& index) {
            const TPackedLogoBlobSstIndex packed(ctxs.GetVCtx()->SstIndex, index);
            UNIT_ASSERT_VALUES_EQUAL(packed.Elements(), index.size());
            UNIT_ASSERT(packed.FirstKey() == index.front().GetKey());
            UNIT_ASSERT(packed.LastKey() == index.back().GetKey());

            // decodes to the very same bytes
            TTrackableVector<TRec> saved(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            packed.Save(&saved);
            UNIT_ASSERT_VALUES_EQUAL(saved.size(), index.size());
            UNIT_ASSERT(memcmp(saved.data(), index.data(), index.size() * sizeof(TRec)) == 0);

            // forward and backward walks
            TIterator it(&packed);
            it.SeekToFirst();
            for (size_t i = 0; i < index.size(); ++i, it.Next()) {
                ExpectAt(it, index, i);
            }
            UNIT_ASSERT(!it.Valid());
            it.Prev();
            for (size_t i = index.size(); i-- > 0; it.Prev()) {
                ExpectAt(it, index, i);
            }
            UNIT_ASSERT(!it.Valid());
            it.SeekToLast();
            ExpectAt(it, index, index.size() - 1);

            // seeks: every key, a key right before it (lands on it), a key right after it (lands on the next), and the
            // group after the record's, which lands on the next group's first record
            for (size_t i = 0; i < index.size(); ++i) {
                const TLogoBlobID id = index[i].GetKey().LogoBlobID();
                it.Seek(index[i].GetKey());
                ExpectAt(it, index, i);

                if (id.PartId() > 1 && (!i || index[i - 1].GetKey().LogoBlobID() < TLogoBlobID(id, id.PartId() - 1))) {
                    it.Seek(TLogoBlobID(id, id.PartId() - 1));
                    ExpectAt(it, index, i);
                }
                const bool lastInGroup = i + 1 == index.size()
                    || !TLogoBlobSstRecHigh(id).SameKey(TLogoBlobSstRecHigh(index[i + 1].GetKey().LogoBlobID()));
                if (const auto next = NextGroupKey(id); lastInGroup && next) {
                    it.Seek(*next);
                    if (i + 1 < index.size()) {
                        ExpectAt(it, index, i + 1);
                    } else {
                        UNIT_ASSERT(!it.Valid());
                    }
                }
                if (id.PartId() < TLogoBlobID::MaxPartId) {
                    it.Seek(TLogoBlobID(id, id.PartId() + 1));
                    if (i + 1 < index.size()) {
                        ExpectAt(it, index, i + 1);
                    } else {
                        UNIT_ASSERT(!it.Valid());
                    }
                }
            }
            const TKeyLogoBlob maxKey(TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));
            it.Seek(maxKey);
            if (index.back().GetKey() == maxKey) {
                ExpectAt(it, index, index.size() - 1);
            } else {
                UNIT_ASSERT(!it.Valid());
                it.Prev();
                ExpectAt(it, index, index.size() - 1);
            }
            it.Seek(TLogoBlobID(0, 0, 0, 0, 0, 0));
            ExpectAt(it, index, 0);
        }

        TVector<TLogoBlobID> MakeExtremeIds() {
            TVector<TLogoBlobID> ids;
            for (ui64 tablet : {ui64(0), Max<ui64>()}) {
                for (ui32 channel : {0u, TLogoBlobID::MaxChannel}) {
                    for (ui32 gen : {0u, Max<ui32>()}) {
                        // 255 / 256: the Step bits split between Raw1 and Raw2
                        for (ui32 step : {0u, 255u, 256u, Max<ui32>()}) {
                            for (ui32 blobSize : {0u, TLogoBlobID::MaxBlobSize}) {
                                for (ui32 cookie : {0u, TLogoBlobID::MaxCookie}) {
                                    for (ui32 crcMode : {0u, TLogoBlobID::MaxCrcMode}) {
                                        for (ui32 partId : {0u, TLogoBlobID::MaxPartId}) {
                                            ids.emplace_back(tablet, gen, step, channel, blobSize, cookie, partId,
                                                crcMode);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Sort(ids);
            return ids;
        }

        TMemRecLogoBlob MakeExtremeMemRec(ui32 i) {
            const ui32 maxId = (ui32(1) << TMemRecLogoBlob::IdBits) - 1;
            return TMemRecLogoBlob(TIngress(i % 2 ? Max<ui64>() : 0), TBlobType::EType(i % 4),
                i / 2 % 2 ? maxId : 0, i / 3 % 2 ? Max<ui32>() : 0, i / 5 % 2 ? Max<ui32>() : 0);
        }

    } // anonymous

    Y_UNIT_TEST_SUITE(TPackedLogoBlobSstIndexTest) {

        Y_UNIT_TEST(RoundTrip) {
            TTestContexts ctxs;
            // frame boundaries: one record, a frame without the last record, exact frames, a frame and a record
            for (ui32 records : {1u, 31u, 32u, 33u, 64u, 3001u}) {
                CheckIndex(ctxs, MakeIndex(ctxs, records, records));
            }
        }

        Y_UNIT_TEST(FieldExtremes) {
            TTestContexts ctxs;
            const TVector<TLogoBlobID> ids = MakeExtremeIds();
            TTrackableVector<TRec> all(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            for (ui32 i = 0; i < ids.size(); ++i) {
                all.emplace_back(TKeyLogoBlob(ids[i]), MakeExtremeMemRec(i));
            }
            // The prefix length changes the Dict widths, and with them the bit positions of the columns after them,
            // including 32-bit wide frames. The full index ends with the greatest key.
            for (ui32 records = 1; records <= 70; ++records) {
                TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
                index.assign(all.begin(), all.begin() + records);
                CheckIndex(ctxs, index);
            }
            CheckIndex(ctxs, all);
        }

        Y_UNIT_TEST(FrameWidthBoundaries) {
            TTestContexts ctxs;
            constexpr ui32 frameSize = TPackedLogoBlobSstIndex::FrameSize;
            for (ui32 width = 0; width <= 32; ++width) {
                const ui32 range = (ui64(1) << width) - 1;
                const ui32 cookieRange = Min(range, TLogoBlobID::MaxCookie);
                // at the top of the range the largest value is Max<ui32>
                for (bool highBase : {false, true}) {
                    const ui32 base = highBase ? Max<ui32>() - range : 0;
                    const ui32 cookieBase = highBase ? TLogoBlobID::MaxCookie - cookieRange : 0;
                    // Change column start alignment and exercise a short final frame.
                    for (ui32 tail = 1; tail <= 8; ++tail) {
                        TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
                        for (ui32 i = 0; i < 3 * frameSize + tail; ++i) {
                            // Constant maximum -> variable width -> constant zero -> partial variable frame.
                            // The varying frames have their minimum after their maximum, so packing cannot take
                            // the first value for the minimum.
                            const ui32 frame = i / frameSize;
                            const ui32 delta = i % 3 == 0 ? range : i % 3 == 1 ? 0 : range / 2;
                            const ui32 value = frame == 0 ? Max<ui32>() : frame == 2 ? 0 : base + delta;
                            const ui32 cookie = frame == 0 ? TLogoBlobID::MaxCookie : frame == 2 ? 0
                                : cookieBase + Min(delta, cookieRange);
                            const TLogoBlobID id(i, 0, value, 0, 1 + i % 5, cookie, i % 3, 0);
                            const TMemRecLogoBlob memRec(TIngress(i % 7), TBlobType::DiskBlob, 1, value, i % 9);
                            index.emplace_back(TKeyLogoBlob(id), memRec);
                        }
                        CheckIndex(ctxs, index);
                    }
                }
            }
        }

        Y_UNIT_TEST(RandomBitsRoundTripAndSeek) {
            TTestContexts ctxs;
            TReallyFastRng32 rng(20261026);
            auto random64 = [&] { return ui64(rng()) << 32 | rng(); };
            // sameStep: Raw1 and Raw2's Step byte are fixed, so all records share one group and one Step and the seek
            // orders them by the rest of Raw2 alone. Otherwise every bit of both words is random, Step included.
            for (bool sameStep : {false, true}) {
                auto randomRaw2 = [&] {
                    const ui64 value = random64();
                    return sameStep ? value | (ui64(0xFF) << TLogoBlobID::StepLowShift) : value;
                };
                TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
                TVector<TKeyLogoBlob> queries;
                // 1025 = 32 * 32 + 1: the random Dict columns need 11-bit indexes, where 1024 values would need 10,
                // and the last frame holds a single record
                for (ui32 i = 0; i < 1025; ++i) {
                    const ui64 raw1 = sameStep ? Max<ui64>() : random64();
                    const ui64 raw2 = randomRaw2();
                    const TKeyLogoBlob key(TLogoBlobID(7, raw1, raw2));
                    const ui64 ingress = random64();
                    const ui32 typeId = rng();
                    const ui32 offset = rng();
                    const ui32 size = rng();
                    index.emplace_back(key, TMemRecLogoBlob(TIngress(ingress),
                        TBlobType::EType(typeId & TMemRecLogoBlob::TypeMask),
                        typeId >> TMemRecLogoBlob::TypeBits, offset, size));
                    queries.emplace_back(TLogoBlobID(7, raw1, randomRaw2()));
                }
                Sort(index, [](const TRec& a, const TRec& b) { return a.GetKey() < b.GetKey(); });
                CheckIndex(ctxs, index);

                const TPackedLogoBlobSstIndex packed(ctxs.GetVCtx()->SstIndex, index);
                TIterator it(&packed);
                for (const auto& key : queries) {
                    const auto expected = std::lower_bound(index.begin(), index.end(), key);
                    it.Seek(key);
                    UNIT_ASSERT_VALUES_EQUAL(it.Valid(), expected != index.end());
                    if (expected != index.end()) {
                        ExpectAt(it, index, expected - index.begin());
                    }
                }
            }
        }

        Y_UNIT_TEST(SeekComparesWholeStep) {
            TTestContexts ctxs;
            // steps with equal low bytes (Raw2 alone would misorder them) and steps across the low byte's wrap
            const TVector<ui32> steps = {1, 255, 256, 257, 511, 512, 65537, Max<ui32>() - 255, Max<ui32>()};
            TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            for (ui32 step : steps) {
                index.emplace_back(TKeyLogoBlob(TLogoBlobID(7, 1, step, 0, 100, 0, 1)), TMemRecLogoBlob());
            }
            CheckIndex(ctxs, index);

            const TPackedLogoBlobSstIndex packed(ctxs.GetVCtx()->SstIndex, index);
            TIterator it(&packed);
            for (ui32 i = 1; i < steps.size(); ++i) {
                if (steps[i] - steps[i - 1] > 1) {
                    it.Seek(TLogoBlobID(7, 1, steps[i] - 1, 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie,
                        TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));
                    ExpectAt(it, index, i);
                }
                it.Seek(TLogoBlobID(7, 1, steps[i], 0, 0, 0));
                ExpectAt(it, index, i);
            }
        }

        Y_UNIT_TEST(DictWidthBoundaries) {
            TTestContexts ctxs;
            // 2^k and 2^k + 1 distinct values per Dict column; PartCrc has only 64 possible values
            for (ui32 distinct : {1u, 2u, 3u, 4u, 5u, 8u, 9u, 16u, 17u, 32u, 33u, 64u, 65u,
                    128u, 129u, 256u, 257u, 512u, 513u, 1024u, 1025u}) {
                TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
                for (ui32 i = 0; i < 2 * distinct + 1; ++i) {
                    const ui32 v = i % distinct;
                    TMemRecLogoBlob memRec(TIngress(ui64(v) << 40 | v), TBlobType::EType(v % 4), v, i, 1000 + v);
                    index.emplace_back(TKeyLogoBlob(TLogoBlobID(7, 1, i, 0, 1 + v, 0, v % 16, v / 16 % 4)), memRec);
                }
                CheckIndex(ctxs, index);
            }
        }

    }

} // NKikimr
