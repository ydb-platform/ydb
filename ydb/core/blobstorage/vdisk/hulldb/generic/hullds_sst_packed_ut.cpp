#include "hullds_sst_packed.h"
#include "hullds_sst_packed_ut.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    namespace {

        using TRec = TPackedLogoBlobSstIndex::TRec;
        using TIterator = TPackedLogoBlobSstIndex::TIterator;
        using NSstIndexTest::MakeIndex;

        void ExpectAt(const TIterator& it, const TTrackableVector<TRec>& index, size_t i) {
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), index[i].GetKey().ToString());
            const TMemRecLogoBlob memRec = it.GetMemRec();
            const TMemRecLogoBlob expected = index[i].GetMemRec();
            UNIT_ASSERT(memcmp(&memRec, &expected, sizeof(memRec)) == 0);
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

            // seeks: every key, a key right before it (lands on it), a key right after it (lands on the next)
            for (size_t i = 0; i < index.size(); ++i) {
                const TLogoBlobID id = index[i].GetKey().LogoBlobID();
                it.Seek(index[i].GetKey());
                ExpectAt(it, index, i);

                if (id.PartId() > 1 && (!i || index[i - 1].GetKey().LogoBlobID() < TLogoBlobID(id, id.PartId() - 1))) {
                    it.Seek(TLogoBlobID(id, id.PartId() - 1));
                    ExpectAt(it, index, i);
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
            it.Seek(TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), 255, 0, 0));
            UNIT_ASSERT(!it.Valid());
            it.Prev();
            ExpectAt(it, index, index.size() - 1);
            it.Seek(TLogoBlobID(0, 0, 0, 0, 0, 0));
            ExpectAt(it, index, 0);
        }

        // records of one group with the given non-decreasing steps
        TTrackableVector<TRec> MakeGroupIndex(TTestContexts& ctxs, const TVector<ui32>& steps, ui32 tablet = 7) {
            TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            for (ui32 i = 0; i < steps.size(); ++i) {
                TMemRecLogoBlob memRec;
                memRec.SetDiskBlob(TDiskPart(1, i * 4096, 100 + i));
                index.emplace_back(TKeyLogoBlob(TLogoBlobID(tablet, 1, steps[i], 0, 100 + i, i, 1 + i % 5)), memRec);
            }
            return index;
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

        Y_UNIT_TEST(EveryRecordItsOwnGroup) {
            TTestContexts ctxs;
            TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            for (ui32 i = 0; i < 100; ++i) {
                index.emplace_back(TKeyLogoBlob(TLogoBlobID(1000 + i, 1, 1, 0, 10, 0)), TMemRecLogoBlob());
            }
            CheckIndex(ctxs, index);
        }

        Y_UNIT_TEST(StepJumpsAndRepeats) {
            TTestContexts ctxs;
            // a frame boundary between equal steps, a wide step jump at a frame start, steps up to the maximum
            TVector<ui32> steps;
            for (ui32 i = 0; i < 70; ++i) {
                steps.push_back(i < 40 ? 5 : i < 64 ? 6 : 5 + (i - 63) * (Max<ui32>() / 8));
            }
            CheckIndex(ctxs, MakeGroupIndex(ctxs, steps));
        }

        Y_UNIT_TEST(SeekBetweenGroups) {
            TTestContexts ctxs;
            TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            for (ui32 tablet : {10u, 20u, 30u}) {
                for (ui32 step = 1; step <= 50; ++step) {
                    index.emplace_back(TKeyLogoBlob(TLogoBlobID(tablet, 1, step, 0, 10, 0)), TMemRecLogoBlob());
                }
            }
            const TPackedLogoBlobSstIndex packed(ctxs.GetVCtx()->SstIndex, index);
            TIterator it(&packed);
            // a tablet between two groups: the next group's first record
            it.Seek(TLogoBlobID(15, 1, 1, 0, 10, 0));
            ExpectAt(it, index, 50);
            // a step above the last of a group: the next group's first record
            it.Seek(TLogoBlobID(20, 1, 51, 0, 10, 0));
            ExpectAt(it, index, 100);
            // a step above the last group: past the end
            it.Seek(TLogoBlobID(30, 1, 51, 0, 10, 0));
            UNIT_ASSERT(!it.Valid());
            // a step below the first of a group: that group's first record
            it.Seek(TLogoBlobID(30, 1, 0, 0, 10, 0));
            ExpectAt(it, index, 100);
            CheckIndex(ctxs, index);
        }

        Y_UNIT_TEST(OffsetWithoutData) {
            TTestContexts ctxs;
            auto index = MakeIndex(ctxs, 500, 7);
            TMemRecLogoBlob memRec = index[100].GetMemRec();
            memRec.SetDiskBlob(TDiskPart(0, 4096, 0));
            index[100] = TRec(index[100].GetKey(), memRec);
            CheckIndex(ctxs, index);
        }

    }

} // NKikimr
