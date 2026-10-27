#include "hullds_sst_it_all_ut.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageHullSstIt) {

        using namespace NBlobStorageHullSstItHelpers;
        using TMemIterator = TLogoBlobSst::TMemIterator;

        Y_UNIT_TEST(TestSeekToFirst) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());
            it.SeekToFirst();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Next();
            }
            TString result("[0:0:10:0:0:0:0][0:0:11:0:0:0:0]"
                          "[0:0:12:0:0:0:0][0:0:13:0:0:0:0]"
                          "[0:0:14:0:0:0:0][0:0:15:0:0:0:0]"
                          "[0:0:16:0:0:0:0][0:0:17:0:0:0:0]"
                          "[0:0:18:0:0:0:0][0:0:19:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekToLast) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());
            it.SeekToLast();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Prev();
            }
            TString result("[0:0:19:0:0:0:0][0:0:18:0:0:0:0]"
                          "[0:0:17:0:0:0:0][0:0:16:0:0:0:0]"
                          "[0:0:15:0:0:0:0][0:0:14:0:0:0:0]"
                          "[0:0:13:0:0:0:0][0:0:12:0:0:0:0]"
                          "[0:0:11:0:0:0:0][0:0:10:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekExactAndNext) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 15, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == TString("[0:0:15:0:0:0:0]"));

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Next();
            }
            TString result("[0:0:15:0:0:0:0][0:0:16:0:0:0:0]"
                          "[0:0:17:0:0:0:0][0:0:18:0:0:0:0]"
                          "[0:0:19:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekExactAndPrev) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 15, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == TString("[0:0:15:0:0:0:0]"));

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Prev();
            }
            TString result("[0:0:15:0:0:0:0][0:0:14:0:0:0:0]"
                          "[0:0:13:0:0:0:0][0:0:12:0:0:0:0]"
                          "[0:0:11:0:0:0:0][0:0:10:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekBefore) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 5, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:10:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSeekAfterAndPrev) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 1));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 25, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(!it.Valid());
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:19:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSeekNotExactBefore) {
            TLogoBlobSstPtr ptr(GenerateSst(10, 10, 2));
            TMemIterator it(ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 15, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:16:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSstIndexSeekAndIterate) {
            TTestContexts ctxs;
            TTrackableVector<TLogoBlobSst::TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));

            auto addRecord = [&index](ui64 tabletId, ui32 step) {
                TLogoBlobID id(tabletId, 0, step, 0, 0, 0);
                index.emplace_back(TKeyLogoBlob(id), TMemRecLogoBlob());
            };

            addRecord(10, 0);
            addRecord(10, 10);
            addRecord(20, 0);
            addRecord(20, 10);
            addRecord(20, 300);

            TLogoBlobSstPtr ptr(new TLogoBlobSst(ctxs.GetVCtx()));
            ptr->LoadLinearIndex(std::move(index));

            TMemIterator it(ptr.Get());

            it.Seek(TLogoBlobID(5, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 5, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 10, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[10:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(10, 0, 15, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(15, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 0, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:0:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 5, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 10, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:10:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 15, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:300:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 300, 0, 0, 0));
            UNIT_ASSERT(it.GetCurKey().ToString() == "[20:0:300:0:0:0:0]");

            it.Seek(TLogoBlobID(20, 0, 400, 0, 0, 0));
            UNIT_ASSERT(!it.Valid());

            it.Seek(TLogoBlobID(25, 0, 0, 0, 0, 0));
            UNIT_ASSERT(!it.Valid());

            it.SeekToFirst();
            it.Prev();
            UNIT_ASSERT(!it.Valid());

            it.SeekToLast();
            it.Next();
            UNIT_ASSERT(!it.Valid());

            it.SeekToFirst();
            TStringStream str1;
            while (it.Valid()) {
                str1 << it.GetCurKey().ToString();
                it.Next();
            }
            UNIT_ASSERT(str1.Str()
                == "[10:0:0:0:0:0:0][10:0:10:0:0:0:0][20:0:0:0:0:0:0][20:0:10:0:0:0:0][20:0:300:0:0:0:0]");

            it.SeekToLast();
            TStringStream str2;
            while (it.Valid()) {
                str2 << it.GetCurKey().ToString();
                it.Prev();
            }
            UNIT_ASSERT(str2.Str()
                == "[20:0:300:0:0:0:0][20:0:10:0:0:0:0][20:0:0:0:0:0:0][10:0:10:0:0:0:0][10:0:0:0:0:0:0]");
        }

        Y_UNIT_TEST(TestSstIndexSaveLoad) {
            TTestContexts ctxs;
            TTrackableVector<TLogoBlobSst::TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));

            TVector<TLogoBlobID> ids = {
                TLogoBlobID(10, 0, 0, 0, 1, 0),
                TLogoBlobID(10, 0, 10, 0, 2, 0),
                TLogoBlobID(20, 0, 0, 0, 3, 0),
                TLogoBlobID(20, 0, 10, 0, 4, 0),
                TLogoBlobID(20, 0, 300, 0, 5, 0, 1),
                TLogoBlobID(20, 0, 300, 0, 5, 0, 2),
                TLogoBlobID(20, 1, 0, 0, 6, 0),
                TLogoBlobID(20, 0xFFFFFFFF, 0xFFFFFFFF, 0, 7, 0xFFFFFF, 3),
                TLogoBlobID(20, 0, 0, 1, 8, 0),
                TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                        TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId),
            };
            for (ui32 i = 0; i < ids.size(); ++i) {
                TMemRecLogoBlob memRec(TIngress(0x0123456789ABCDEFull + i));
                memRec.SetDiskBlob(TDiskPart(i + 1, i * 10, i * 100));
                index.emplace_back(TKeyLogoBlob(ids[i]), memRec);
            }

            TTwoLevelLogoBlobSstIndex twoLevel(ctxs.GetVCtx()->SstIndex, index);

            using TRecHigh = TTwoLevelLogoBlobSstIndex::TRecHigh;

            const auto& indexHigh = twoLevel.IndexHigh;
            UNIT_ASSERT_VALUES_EQUAL(indexHigh.size(), 6u);
            auto checkHigh = [&](size_t i, const TLogoBlobID& id, ui32 lowRangeEndIndex) {
                UNIT_ASSERT(indexHigh[i].SameKey(TRecHigh(id)));
                UNIT_ASSERT_VALUES_EQUAL(indexHigh[i].LowRangeEndIndex, lowRangeEndIndex);
            };
            checkHigh(0, ids[0], 2);
            checkHigh(1, ids[2], 6);
            checkHigh(2, ids[6], 7);
            checkHigh(3, ids[7], 8);
            checkHigh(4, ids[8], 9);
            checkHigh(5, ids[9], 10);

            const auto& indexLow = twoLevel.IndexLow;
            UNIT_ASSERT_VALUES_EQUAL(indexLow.size(), ids.size());
            for (size_t i = 0; i < ids.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(indexLow[i].Step, ids[i].Step());
                UNIT_ASSERT_VALUES_EQUAL(indexLow[i].Raw2, ids[i].GetRaw()[2]);
                UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(&indexLow[i]) % 32, 0u);
            }

            TTrackableVector<TLogoBlobSst::TRec> checkIndex(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            twoLevel.Save(&checkIndex);

            UNIT_ASSERT_VALUES_EQUAL(checkIndex.size(), index.size());
            for (size_t i = 0; i < index.size(); ++i) {
                UNIT_ASSERT_EQUAL(memcmp(&index[i], &checkIndex[i], sizeof(TLogoBlobSst::TRec)), 0);
            }

            // iterator: forward, backward and seek over every key
            TTwoLevelLogoBlobSstIndex::TIterator it(&twoLevel);
            it.SeekToFirst();
            for (const TLogoBlobID& id : ids) {
                UNIT_ASSERT(it.Valid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), id);
                it.Next();
            }
            UNIT_ASSERT(!it.Valid());

            it.SeekToLast();
            for (auto id = ids.rbegin(); id != ids.rend(); ++id) {
                UNIT_ASSERT(it.Valid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), *id);
                it.Prev();
            }

            for (const TLogoBlobID& id : ids) {
                it.Seek(TKeyLogoBlob(id));
                UNIT_ASSERT(it.Valid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), id);
            }

            // seek between keys lands on the next key
            it.Seek(TKeyLogoBlob(TLogoBlobID(20, 0, 300, 0, 5, 0)));
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), ids[4]);
            it.Seek(TKeyLogoBlob(TLogoBlobID(20, 0, 301, 0, 0, 0)));
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), ids[6]);
            it.Seek(TKeyLogoBlob(TLogoBlobID(15, 0, 0, 0, 0, 0)));
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), ids[2]);
            it.Seek(TKeyLogoBlob(TLogoBlobID(5, 0, 0, 0, 0, 0)));
            UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().LogoBlobID(), ids[0]);
        }
    } // TBlobStorageHullSstIt

    Y_UNIT_TEST_SUITE(TBlobStorageHullSstIndexForms) {

        using namespace NBlobStorageHullSstItHelpers;
        using TMemIterator = TLogoBlobSst::TMemIterator;
        using TRecs = TTrackableVector<TLogoBlobSst::TRec>;
        constexpr auto TwoLevel = ESstIndexFormPolicy::TwoLevel;
        constexpr auto Packed = ESstIndexFormPolicy::Packed;
        constexpr std::initializer_list<ESstIndexFormPolicy> AllForms = {TwoLevel, Packed};

        // Group boundaries vary generation, channel and tablet; steps have gaps within each group.
        TRecs MakeRecords(TTestContexts& ctxs, ui32 count, ui32 recordsPerGroup) {
            TRecs recs(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            for (ui32 i = 0; i < count; ++i) {
                const ui32 group = i / recordsPerGroup;
                TLogoBlobID id(7 + group / 4, 1 + group % 2, 10 + 3 * (i % recordsPerGroup),
                    group / 2 % 2, 100 + i, i % 5);
                TMemRecLogoBlob memRec;
                memRec.SetDiskBlob(TDiskPart(1, i * 4096, 100 + i));
                recs.emplace_back(TKeyLogoBlob(id), memRec);
            }
            return recs;
        }

        TString Walk(const TLogoBlobSst& sst, bool forward) {
            TStringStream str;
            TMemIterator it(&sst);
            forward ? it.SeekToFirst() : it.SeekToLast();
            while (it.Valid()) {
                str << it.GetCurKey().ToString() << ":" << it.GetMemRec().DataSize() << " ";
                forward ? it.Next() : it.Prev();
            }
            return str.Str();
        }

        Y_UNIT_TEST(LoadsInEveryForm) {
            TTestContexts ctxs;
            const TRecs recs = MakeRecords(ctxs, 100, 100);
            for (auto kind : AllForms) {
                ctxs.GetVCtx()->SstIndexFormPolicy = kind;
                TLogoBlobSstPtr sst(new TLogoBlobSst(ctxs.GetVCtx()));
                sst->LoadLinearIndex(TRecs(recs));
                UNIT_ASSERT_VALUES_EQUAL(sst->Elements(), 100);
                UNIT_ASSERT(sst->IsLoaded());
            }
            UNIT_ASSERT(!TLogoBlobSst(ctxs.GetVCtx()).IsLoaded());
        }

        Y_UNIT_TEST(OtherKeysAlwaysUseFlat) {
            TTestContexts ctxs;
            using TBlocksSst = TLevelSegment<TKeyBlock, TMemRecBlock>;
            static_assert(std::is_same_v<TBlocksSst::TIndex, TFlatSstIndex<TKeyBlock, TMemRecBlock>>);
            TTrackableVector<TBlocksSst::TRec> recs(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            recs.emplace_back(TKeyBlock(1), TMemRecBlock(1));
            recs.emplace_back(TKeyBlock(2), TMemRecBlock(2));
            TIntrusivePtr<TBlocksSst> sst(new TBlocksSst(ctxs.GetVCtx()));
            sst->LoadLinearIndex(std::move(recs));
            UNIT_ASSERT_VALUES_EQUAL(sst->Elements(), 2);
            UNIT_ASSERT_VALUES_EQUAL(sst->FirstKey().ToString(), TKeyBlock(1).ToString());
            UNIT_ASSERT_VALUES_EQUAL(sst->LastKey().ToString(), TKeyBlock(2).ToString());
        }

        Y_UNIT_TEST(FormsIterateIdentically) {
            TTestContexts ctxs;
            // One group, groups crossing packed-frame boundaries, and one record per group.
            for (ui32 recordsPerGroup : {300u, 37u, 1u}) {
                const TRecs recs = MakeRecords(ctxs, 300, recordsPerGroup);
                ctxs.GetVCtx()->SstIndexFormPolicy = TwoLevel;
                TLogoBlobSstPtr a(new TLogoBlobSst(ctxs.GetVCtx()));
                a->LoadLinearIndex(TRecs(recs));
                ctxs.GetVCtx()->SstIndexFormPolicy = Packed;
                TLogoBlobSstPtr b(new TLogoBlobSst(ctxs.GetVCtx()));
                b->LoadLinearIndex(TRecs(recs));

                UNIT_ASSERT_VALUES_EQUAL(Walk(*a, true), Walk(*b, true));
                UNIT_ASSERT_VALUES_EQUAL(Walk(*a, false), Walk(*b, false));
                UNIT_ASSERT_VALUES_EQUAL(a->FirstKey().ToString(), b->FirstKey().ToString());
                UNIT_ASSERT_VALUES_EQUAL(a->LastKey().ToString(), b->LastKey().ToString());

                auto checkRecord = [](const TMemIterator& it, const TLogoBlobSst::TRec& expected) {
                    UNIT_ASSERT(it.Valid());
                    UNIT_ASSERT(it.GetCurKey() == expected.GetKey());
                    const auto actualMemRec = it.GetMemRec();
                    const auto expectedMemRec = expected.GetMemRec();
                    UNIT_ASSERT_EQUAL(memcmp(&actualMemRec, &expectedMemRec, sizeof(actualMemRec)), 0);
                };
                auto checkSeek = [&](const TKeyLogoBlob& key) {
                    auto expected = std::lower_bound(recs.begin(), recs.end(), key);
                    TMemIterator ia(a.Get());
                    TMemIterator ib(b.Get());
                    ia.Seek(key);
                    ib.Seek(key);
                    UNIT_ASSERT_VALUES_EQUAL(ia.Valid(), expected != recs.end());
                    UNIT_ASSERT_VALUES_EQUAL(ib.Valid(), expected != recs.end());
                    if (expected != recs.end()) {
                        checkRecord(ia, *expected);
                        checkRecord(ib, *expected);
                    }
                    // Also move back from past-the-end and across the start of each high group.
                    ia.Prev();
                    ib.Prev();
                    UNIT_ASSERT_VALUES_EQUAL(ia.Valid(), expected != recs.begin());
                    UNIT_ASSERT_VALUES_EQUAL(ib.Valid(), expected != recs.begin());
                    if (expected != recs.begin()) {
                        --expected;
                        checkRecord(ia, *expected);
                        checkRecord(ib, *expected);
                    }
                };

                for (const auto& rec : recs) {
                    const TLogoBlobID id = rec.GetKey().LogoBlobID();
                    checkSeek(rec.GetKey());
                    for (ui32 step : {id.Step() - 1, id.Step() + 1}) {
                        checkSeek(TLogoBlobID(id.TabletID(), id.Generation(), step, id.Channel(),
                            id.BlobSize(), id.Cookie()));
                    }
                }
                checkSeek(TLogoBlobID(0, 0, 0, 0, 0, 0));
                checkSeek(TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), 255, 0, 0));
            }
        }

        Y_UNIT_TEST(IteratorEqualityAndCopy) {
            TTestContexts ctxs;
            for (auto kind : AllForms) {
                ctxs.GetVCtx()->SstIndexFormPolicy = kind;
                TLogoBlobSstPtr sst(new TLogoBlobSst(ctxs.GetVCtx()));
                sst->LoadLinearIndex(MakeRecords(ctxs, 50, 1));
                TMemIterator a(sst.Get());
                TMemIterator unpositioned(sst.Get());
                UNIT_ASSERT(a == unpositioned);
                UNIT_ASSERT(!a.Valid());
                a.SeekToFirst();
                UNIT_ASSERT(a.Valid());
                UNIT_ASSERT(a != unpositioned);
                unpositioned.SeekToFirst();
                UNIT_ASSERT(a == unpositioned);
                a.Next();
                TMemIterator b = a;
                UNIT_ASSERT(a == b);
                b.Next();
                UNIT_ASSERT(a != b);
                a.Next();
                UNIT_ASSERT(a == b);
                TMemIterator c(sst.Get());
                c.Seek(a.GetCurKey());
                UNIT_ASSERT(a == c);
                UNIT_ASSERT(!TMemIterator().Valid());
            }
        }

        // the states other than "at a record" that callers rely on, in every form
        Y_UNIT_TEST(IteratorEdgeStates) {
            TTestContexts ctxs;
            for (auto kind : AllForms) {
                ctxs.GetVCtx()->SstIndexFormPolicy = kind;
                TRecs recs = MakeRecords(ctxs, 130, 1);
                const TString first = recs.front().GetKey().ToString();
                const TString last = recs.back().GetKey().ToString();
                TLogoBlobSstPtr sst(new TLogoBlobSst(ctxs.GetVCtx()));
                sst->LoadLinearIndex(std::move(recs));
                TMemIterator it(sst.Get());

                // past-the-end, then back to the last record
                it.Seek(TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), 255, 0, 0));
                UNIT_ASSERT(!it.Valid());
                it.Prev();
                UNIT_ASSERT(it.Valid());
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), last);
                it.Next();
                UNIT_ASSERT(!it.Valid());
                it.Prev();
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), last);

                // before-begin, then sought again
                it.SeekToFirst();
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), first);
                it.Prev();
                UNIT_ASSERT(!it.Valid());
                it.Seek(TLogoBlobID(0, 0, 0, 0, 0, 0));
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), first);

                // a copy moves on its own
                TMemIterator copy = it;
                copy.Next();
                UNIT_ASSERT_VALUES_EQUAL(it.GetCurKey().ToString(), first);
                UNIT_ASSERT(copy.Valid() && copy.GetCurKey().ToString() != first);
            }
        }

        Y_UNIT_TEST(EmptyIndexInEveryForm) {
            TTestContexts ctxs;
            for (auto kind : AllForms) {
                ctxs.GetVCtx()->SstIndexFormPolicy = kind;
                TLogoBlobSstPtr sst(new TLogoBlobSst(ctxs.GetVCtx()));
                sst->LoadLinearIndex(TRecs(TMemoryConsumer(ctxs.GetVCtx()->SstIndex)));
                UNIT_ASSERT(!sst->IsLoaded());
                TMemIterator it(sst.Get());
                it.SeekToFirst();
                UNIT_ASSERT(!it.Valid());
                it.Seek(TLogoBlobID(1, 1, 1, 0, 0, 0));
                UNIT_ASSERT(!it.Valid());
            }
        }

        Y_UNIT_TEST(SaveLinearIndexRoundTripsEveryForm) {
            TTestContexts ctxs;
            for (auto kind : AllForms) {
                ctxs.GetVCtx()->SstIndexFormPolicy = kind;
                for (ui32 recordsPerGroup : {200u, 37u, 1u}) {
                    TRecs recs = MakeRecords(ctxs, 200, recordsPerGroup);
                    TLogoBlobSstPtr sst(new TLogoBlobSst(ctxs.GetVCtx()));
                    sst->LoadLinearIndex(TRecs(recs));
                    TRecs saved(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
                    sst->SaveLinearIndex(&saved);
                    UNIT_ASSERT_VALUES_EQUAL(saved.size(), recs.size());
                    for (size_t i = 0; i < recs.size(); ++i) {
                        UNIT_ASSERT(saved[i].GetKey() == recs[i].GetKey());
                        UNIT_ASSERT_VALUES_EQUAL(saved[i].GetMemRec().DataSize(), recs[i].GetMemRec().DataSize());
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_SUITE(TBlobStorageHullOrderedSstsIt) {

        using namespace NBlobStorageHullSstItHelpers;
        using TIterator = TLogoBlobOrderedSsts::TReadIterator;
        TTestContexts TestCtx(ChunkSize, CompWorthReadSize);

        Y_UNIT_TEST(TestSeekToFirst) {
            TLogoBlobOrderedSstsPtr ptr(GenerateOrderedSsts(10, 5, 1, 3));
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TIterator it(hullCtx, ptr.Get());
            it.SeekToFirst();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Next();
            }
            TString result("[0:0:10:0:0:0:0][0:0:11:0:0:0:0]"
                          "[0:0:12:0:0:0:0][0:0:13:0:0:0:0]"
                          "[0:0:14:0:0:0:0][0:0:15:0:0:0:0]"
                          "[0:0:16:0:0:0:0][0:0:17:0:0:0:0]"
                          "[0:0:18:0:0:0:0][0:0:19:0:0:0:0]"
                          "[0:0:20:0:0:0:0][0:0:21:0:0:0:0]"
                          "[0:0:22:0:0:0:0][0:0:23:0:0:0:0]"
                          "[0:0:24:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekToLast) {
            TLogoBlobOrderedSstsPtr ptr(GenerateOrderedSsts(10, 5, 1, 3));
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TIterator it(hullCtx, ptr.Get());
            it.SeekToLast();

            TStringStream str;
            while (it.Valid()) {
                str << it.GetCurKey().ToString();
                it.Prev();
            }
            TString result("[0:0:24:0:0:0:0][0:0:23:0:0:0:0]"
                          "[0:0:22:0:0:0:0][0:0:21:0:0:0:0]"
                          "[0:0:20:0:0:0:0][0:0:19:0:0:0:0]"
                          "[0:0:18:0:0:0:0][0:0:17:0:0:0:0]"
                          "[0:0:16:0:0:0:0][0:0:15:0:0:0:0]"
                          "[0:0:14:0:0:0:0][0:0:13:0:0:0:0]"
                          "[0:0:12:0:0:0:0][0:0:11:0:0:0:0]"
                          "[0:0:10:0:0:0:0]");
            UNIT_ASSERT(str.Str() == result);
        }

        Y_UNIT_TEST(TestSeekAfterAndPrev) {
            TLogoBlobOrderedSstsPtr ptr(GenerateOrderedSsts(10, 5, 1, 3));
            THullCtxPtr hullCtx = TestCtx.GetHullCtx();
            TIterator it(hullCtx, ptr.Get());

            TLogoBlobID id;
            id = TLogoBlobID(0, 0, 30, 0, 0, 0);
            it.Seek(id);
            UNIT_ASSERT(!it.Valid());
            it.Prev();
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT(it.GetCurKey().ToString() == "[0:0:24:0:0:0:0]");
        }

        // FIXME: not all cases covered
    }

    Y_UNIT_TEST_SUITE(TBlobStorageHullSstHeapStripe) {
        Y_UNIT_TEST(StripeIsDerivedFromChunkOwnership) {
            TTestContexts ctxs;
            using TSst = TLevelSegment<TKeyBlock, TMemRecBlock>;
            TIntrusivePtr<TSst> seg(new TSst(ctxs.GetVCtx()));
            seg->LastPartAddr = TDiskPart(7, 4064, 80);
            seg->HeapStripe = seg->LastPartAddr;
            seg->AllChunks = {7};

            // nothing about the stripe is written down; the SST address is the whole record
            NKikimrVDiskData::TDiskPart pb;
            seg->SerializeToProto(pb);
            UNIT_ASSERT_VALUES_EQUAL(pb.GetChunkIdx(), 7u);
            UNIT_ASSERT_VALUES_EQUAL(pb.GetOffset(), 4064u);
            UNIT_ASSERT_VALUES_EQUAL(pb.GetSize(), 80u);

            TSst loaded(ctxs.GetVCtx(), pb);
            UNIT_ASSERT(loaded.HeapStripe.Empty());

            // a chunk owned by the slot heap leaves the SST unstriped
            loaded.ResolveHeapStripe(THashSet<TChunkIdx>{9});
            UNIT_ASSERT(loaded.HeapStripe.Empty());

            loaded.ResolveHeapStripe(THashSet<TChunkIdx>{7});
            UNIT_ASSERT_VALUES_EQUAL(loaded.HeapStripe.ChunkIdx, 7u);
            UNIT_ASSERT_VALUES_EQUAL(loaded.HeapStripe.Offset, 4064u);
            UNIT_ASSERT_VALUES_EQUAL(loaded.HeapStripe.Size, 80u);

            TVector<ui32> ids;
            loaded.FillInChunkIds(ids);
            UNIT_ASSERT(ids.empty());

            loaded.AllChunks = {7};
            TSet<TChunkIdx> chunks;
            loaded.GetOwnedChunks(chunks);
            UNIT_ASSERT(chunks.contains(7));

            // a second stripe SST in the same chunk is allowed to claim it again
            TSst other(ctxs.GetVCtx());
            other.AllChunks = {7};
            other.HeapStripe = loaded.HeapStripe;
            other.GetOwnedChunks(chunks);
            UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 1u);
        }
    }

} // NKikimr
