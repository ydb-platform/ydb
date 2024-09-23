#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_mem_iter.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/flat_iterator.h>
#include <ydb/core/tablet_flat/flat_part_slice.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_pretty.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_iter.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTable {

namespace {
    static NPage::TConf Conf(ui32 page = NPage::TConf().Groups.at(0).PageSize) noexcept
    {
        NPage::TConf conf;

        conf.Group(0).PageSize = page;
        conf.Group(0).BTreeIndexNodeTargetSize = 128;
        conf.LargeEdge = 29;  /* need to cover external blob usage */

        return conf;
    }

    const NTest::TMass Mass0(new NTest::TModelStd(false), 6666);

    static void VerifySingleLevelNonTrivial(const TAutoPtr<TSubset>& subset)
    {
        /* parts form a single run and have non-trivial pages */
        TLevels levels(subset->Scheme->Keys);

        for (const auto &partView : subset->Flatten) {
            levels.Add(partView.Part, partView.Slices);

            auto *partStore = partView.As<NTest::TPartStore>();

            if (partStore->Store->PageCollectionArray(0).size() * 3 > partStore->Stat.Rows) {
                UNIT_ASSERT_C(false, "Part has too few rows per page"
                    << ", rows " << partStore->Stat.Rows
                    << ", pages " << partStore->Store->PageCollectionArray(0).size());
            }
        }

        UNIT_ASSERT_C(levels.size() == 1, "Levels size " << levels.size());
    }
}

using TCheckIter = NTest::TChecker<NTest::TWrapIter, TSubset>;
using TCheckReverseIter = NTest::TChecker<NTest::TWrapReverseIter, TSubset>;

Y_UNIT_TEST_SUITE(TIterator) {
    using namespace NTest;

    Y_UNIT_TEST(Basics)
    {
        TCheckIter wrap(*TMake(Mass0).Mixed(0, 1, TMixerOne{ }), { });

        { /*_ ESeek::Exact have to give at most one row */
            wrap.To(1).Seek(Mass0.Saved[7], ESeek::Exact).Is(Mass0.Saved[7]);
            wrap.To(2).Next().Is(EReady::Gone);
        }

        { /*_ Seek w/o pages have to yield EReady::Page forever */
            auto *env = new TNoEnv{ false, ELargeObjNeed::Has };

            wrap.Displace<IPages>(env);

            wrap.To(6).Seek(Mass0.Saved[7], ESeek::Lower).Is(EReady::Page);
            env->Pages = true; /* load all pages to cache */
            wrap.To(7).Next().Is(EReady::Page).Next().Is(EReady::Page);
        }
    }

    Y_UNIT_TEST(External)
    {
        TCheckIter wrap(*TMake(Mass0, Conf()).Mixed(0, 1, TMixerOne{ }), { });

        UNIT_ASSERT((*wrap).Flatten.size() == 1);

        auto *part = (*wrap).Flatten.at(0).Part.Get();

        UNIT_ASSERT(part && part->Large && part->Blobs);

        /* Work on the second row with external blobs, should check how
            iterator behaves on absense of blobs in cache on Next().
         */

        const auto frame = ui32(0) - part->Large->Relation(0).Refer;
        const auto on = part->Large->Relation(frame).Row;

        auto *env = new TNoEnv{ true, ELargeObjNeed::Has };

        wrap.Displace<IPages>(env);
        wrap.To(1).Seek(Mass0.Saved[on-1], ESeek::Lower).Is(Mass0.Saved[on-1]);
        env->Lobs = ELargeObjNeed::Yes; /* no any blobs in cache */
        wrap.To(2).Next().Is(EReady::Page).To(3).Next().Is(EReady::Page);
        env->Lobs = ELargeObjNeed::Has; /* load all blobs to cache */
        wrap.To(8).Next().Is(Mass0.Saved[on]);
    }

    Y_UNIT_TEST(Single)
    {
        auto subset = TMake(Mass0, Conf()).Mixed(0, 1, TMixerOne{ });

        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Cached, *subset);
    }

    Y_UNIT_TEST(SingleReverse)
    {
        auto subset = TMake(Mass0, Conf()).Mixed(0, 1, TMixerOne{ });

        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Cached, *subset);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, TMixerRnd(4));

        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Evicted, *subset);
        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Forward, *subset);
    }

    Y_UNIT_TEST(MixedReverse)
    {
        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, TMixerRnd(4));

        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Evicted, *subset);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());

        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, mixer);

        VerifySingleLevelNonTrivial(subset);

        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Evicted, *subset);
        TWreck<TCheckIter, TSubset>(Mass0, 666).Do(EWreck::Forward, *subset);
    }

    Y_UNIT_TEST(SerialReverse)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());

        auto subset = TMake(Mass0, Conf(384)).Mixed(2, 2, mixer);

        VerifySingleLevelNonTrivial(subset);

        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Cached, *subset);
        TWreck<TCheckReverseIter, TSubset, EDirection::Reverse>(Mass0, 666).Do(EWreck::Evicted, *subset);
    }

    struct TFirstTimeFailEnv : public NTest::TTestEnv {
        explicit TFirstTimeFailEnv(bool autoLoad = true)
            : AutoLoad(autoLoad)
        {}

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override {
            if (lob == ELargeObj::Extern) {
                if (AutoLoad) {
                    if (LoadedExtern.insert(ref).second) {
                        return { true, nullptr };
                    }
                } else if (!LoadedExtern.contains(ref)) {
                    SeenExtern.insert(ref);
                    return { true, nullptr };
                }
            }

            return NTest::TTestEnv::Locate(part, ref, lob);
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
            if (AutoLoad) {
                if (Loaded[groupId].insert(pageId).second) {
                    return nullptr;
                }
            } else if (!Loaded[groupId].contains(pageId)) {
                Seen[groupId].insert(pageId);
                return nullptr;
            }

            return NTest::TTestEnv::TryGetPage(part, pageId, groupId);
        }

        void Load() {
            for (ui64 ref : SeenExtern) {
                LoadedExtern.insert(ref);
            }
            SeenExtern.clear();

            for (auto& kv : Seen) {
                auto& dst = Loaded[kv.first];
                for (TPageId pageId : kv.second) {
                    dst.insert(pageId);
                }
            }
            Seen.clear();
        }

        void Unload() {
            SeenExtern.clear();
            LoadedExtern.clear();
            Seen.clear();
            Loaded.clear();
        }

        const bool AutoLoad;
        THashSet<ui64> SeenExtern;
        THashSet<ui64> LoadedExtern;
        THashMap<TGroupId, THashSet<TPageId>> Seen;
        THashMap<TGroupId, THashSet<TPageId>> Loaded;
    };

    Y_UNIT_TEST(GetKey)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0, NScheme::NTypeIds::String)
            .Col(0, 1, NScheme::NTypeIds::String)
            .Key({ 0 });

        // Note: we use compressed pages because they are volatile and freed
        // when advancing to the next page. We also force values to external
        // blobs so page faults are triggered after each advance to the next
        // page, which causes key reference to be invalidated unless key page
        // is pinned in memory.
        auto conf = NPage::TConf();
        conf.Group(0).PageSize = 256;
        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.LargeEdge = 24;

        auto cook = TPartCook(lay, conf);
        for (int i = 1; i <= 1000; ++i) {
            TString key = Sprintf("not_inline_and_long_key_%04d", i);
            TString value = Sprintf("not_inline_and_long_value_%04d", i);
            cook.AddN(key, value);
        }

        auto cooked = cook.Finish();

        TSubset subset(TEpoch::Zero(), cooked.Scheme);
        for (const auto& part : cooked.Parts) {
            subset.Flatten.push_back({ part, nullptr, part->Slices });
        }

        TCheckIter wrap(subset, { new TFirstTimeFailEnv });

        // Must page fault the first couple of times
        wrap.To(0).Seek({}, ESeek::Lower).Is(EReady::Page);
        do {
            wrap.Seek({}, ESeek::Lower);
        } while (wrap.GetReady() == EReady::Page);
        wrap.To(1).IsOpN(ERowOp::Upsert,
            "not_inline_and_long_key_0001",
            "not_inline_and_long_value_0001");

        TString lastSeen = TString(wrap->GetKey().Cells().at(0).AsBuf());
        UNIT_ASSERT_VALUES_EQUAL(lastSeen, "not_inline_and_long_key_0001");
        int nextExpected = 2;
        size_t pageFaults = 0;
        for (;;) {
            wrap.Next();

            if (wrap.GetReady() == EReady::Gone) {
                UNIT_ASSERT(wrap->GetKey().Cells().empty());
                break;
            }

            TString currentKey = TString(wrap->GetKey().Cells().at(0).AsBuf());
            if (wrap.GetReady() == EReady::Page) {
                UNIT_ASSERT_VALUES_EQUAL(currentKey, lastSeen);
                ++pageFaults;
                continue;
            }

            wrap.To(nextExpected).IsOpN(ERowOp::Upsert,
                Sprintf("not_inline_and_long_key_%04d", nextExpected),
                Sprintf("not_inline_and_long_value_%04d", nextExpected));
            UNIT_ASSERT_VALUES_EQUAL(currentKey, Sprintf("not_inline_and_long_key_%04d", nextExpected));
            lastSeen = currentKey;
            ++nextExpected;
        }

        UNIT_ASSERT_VALUES_EQUAL(nextExpected, 1001);
        UNIT_ASSERT_C(pageFaults >= 3, "Not enough page faults: " << pageFaults);
    }

    struct Schema : NIceDb::Schema {
        struct TestTable : Table<1> {
            struct Key : Column<1, NScheme::NTypeIds::String> {};
            struct Value : Column<2, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, Value>;
            using Precharge = NoAutoPrecharge;
        };

        using TTables = SchemaTables<TestTable>;
    };

    Y_UNIT_TEST(GetKeyWithEraseCache) {
        TLayoutCook lay;
        lay
            .Col(0, 1, NScheme::NTypeIds::String)
            .Col(0, 2, NScheme::NTypeIds::String)
            .Key({ 1 });

        auto conf = NPage::TConf();
        conf.Group(0).PageRows = 4;
        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.Final = false;

        auto cook = TPartCook(lay, conf);
        for (int i = 1; i <= 100; ++i) {
            TString key = Sprintf("not_inline_and_long_key_%04d", i);
            TString value = Sprintf("not_inline_and_long_value_%04d", i);
            if (i <= 40) {
                cook.AddOpN(ERowOp::Erase, key);
            } else {
                cook.AddN(key, value);
            }
        }
        auto cooked = cook.Finish();

        TDatabase DB;
        NIceDb::TNiceDb db(DB);
        TFirstTimeFailEnv env(/* autoLoad */ false);

        {
            DB.Begin(1, env);
            db.Materialize<Schema>();
            DB.Alter().SetEraseCache(1, true, /* minRows */ 2, /* maxBytes */ 8192);
            DB.Commit(1, true);
        }

        for (const auto& part : cooked.Parts) {
            DB.Merge(1, TPartView{ part, nullptr, part->Slices });
        }

        ui64 txId = 2;
        TString lastKey;
        bool lastKeyErase = true;
        std::vector<TString> observed;
        int iterations = 0;

        // Note: this loop mimicks read iterator logic in datashard
        for (;;) {
            ++iterations;
            DB.Begin(txId, env);

            TKeyRange range;
            std::vector<TRawTypeValue> minKey;
            if (lastKey) {
                minKey.push_back(NScheme::TString::ToRawTypeValue(lastKey));
                range.MinKey = minKey;
                range.MinInclusive = lastKeyErase;
                // Cerr << "... starting from key " << lastKey << " inclusive=" << range.MinInclusive << Endl;
            } else {
                // Cerr << "... starting from empty key" << Endl;
            }

            std::vector<TTag> tags;
            tags.push_back(2);

            auto iter = DB.IterateRange(1, range, tags);

            bool advanced = false;
            while (iter->Next(ENext::Data) == EReady::Data) {
                advanced = true;

                auto key = iter->GetKey().Cells();
                UNIT_ASSERT(key);
                TString keyValue(key.at(0).AsBuf());
                observed.push_back(keyValue);
                // Cerr << "    ... observed key " << keyValue << Endl;

                iter->Stats.DeletedRowSkips = 0;
            }

            auto key = iter->GetKey().Cells();
            if (key && (advanced || iter->Stats.DeletedRowSkips >= 4) && iter->Last() == EReady::Page) {
                TString keyValue(key.at(0).AsBuf());
                UNIT_ASSERT_C(keyValue >= lastKey,
                    "Page fault after key " << keyValue << " lastKey " << lastKey);
                lastKey = keyValue;
                lastKeyErase = iter->GetKeyState() == ERowOp::Erase;
                advanced = true;
                // Cerr << "    ... updated lastKey to " << lastKey << " inclusive=" << lastKeyErase << Endl;
            }

            bool restart = true;
            if (advanced || iter->Last() != EReady::Page) {
                // Instead of restarting (and loading missing pages) we will start a new transaction from a new key
                restart = false;
            }

            if (iter->Last() == EReady::Page) {
                DB.Commit(txId++, false);
                if (restart) {
                    // Cerr << "... restarting same tx (loading missing pages)" << Endl;
                    env.Load();
                } else {
                    // Cerr << "... restarting new tx" << Endl;
                }
                continue;
            }

            DB.Commit(txId++, true);
            // Cerr << "... finished" << Endl;
            break;
        }

        for (int i = 41; i <= 100; ++i) {
            size_t index = i - 41;
            UNIT_ASSERT_C(index < observed.size(), "Row " << i << " was not observed");
            TString key = Sprintf("not_inline_and_long_key_%04d", i);
            UNIT_ASSERT_VALUES_EQUAL(observed[index], key);
        }
        UNIT_ASSERT_VALUES_EQUAL(observed.size(), 60u);

        // Note: one restart for index and then 2 transactions per page (since we don't precharge)
        UNIT_ASSERT_C(iterations <= 51, "reading table took " << iterations << " transactions");

        {
            DB.Begin(txId, env);

            TKeyRange range;
            std::vector<TTag> tags;
            tags.push_back(2);

            auto iter = DB.IterateRange(1, range, tags);
            while (iter->Next(ENext::Data) == EReady::Data) {
                // nothing
            }

            // Should not have any page faults
            UNIT_ASSERT_C(iter->Last() != EReady::Page, "Unexpected page fault");

            // All erases should be in a single range in erase cache
            UNIT_ASSERT_VALUES_EQUAL(iter->Stats.DeletedRowSkips, 1u);

            DB.Commit(txId++, true);
        }

        env.Unload();

        std::vector<TString> lastKeys;

        for (;;) {
            DB.Begin(txId, env);

            TKeyRange range;
            std::vector<TTag> tags;
            tags.push_back(2);

            auto iter = DB.IterateRange(1, range, tags);
            while (iter->Next(ENext::Data) == EReady::Data) {
                // nothing
            }

            if (iter->Last() == EReady::Page) {
                if (auto key = iter->GetKey().Cells()) {
                    TString keyValue(key.at(0).AsBuf());
                    // Cerr << "... restarting after key " << keyValue << Endl;
                    lastKeys.push_back(keyValue);
                } else {
                    // Cerr << "... restarting without last key" << Endl;
                }
                DB.Commit(txId++, false);
                env.Load();
                continue;
            }

            DB.Commit(txId++, true);
            break;
        }

        // The first time we page fault we should get the last erased key in the cached range
        UNIT_ASSERT(!lastKeys.empty());
        UNIT_ASSERT_VALUES_EQUAL(lastKeys[0], "not_inline_and_long_key_0040");
    }

    Y_UNIT_TEST(GetKeyWithVersionSkips) {
        TLayoutCook lay;
        lay
            .Col(0, 1, NScheme::NTypeIds::String)
            .Col(0, 2, NScheme::NTypeIds::String)
            .Key({ 1 });

        auto conf = NPage::TConf();
        conf.Group(0).PageRows = 4;
        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.Final = false;

        auto cook = TPartCook(lay, conf);
        for (int i = 1; i <= 100; ++i) {
            TString key = Sprintf("not_inline_and_long_key_%04d", i);
            TString value = Sprintf("not_inline_and_long_value_%04d", i);
            // Version 1:100 has all rows
            // Version 1:90 has the last 90 rows, etc.
            cook.Ver(TRowVersion(1, 101-i)).AddN(key, value);
        }
        auto cooked = cook.Finish();

        TDatabase DB;
        NIceDb::TNiceDb db(DB);
        TFirstTimeFailEnv env(/* autoLoad */ false);

        {
            DB.Begin(1, env);
            db.Materialize<Schema>();
            DB.Alter().SetEraseCache(1, true, /* minRows */ 2, /* maxBytes */ 8192);
            DB.Commit(1, true);
        }

        for (const auto& part : cooked.Parts) {
            DB.Merge(1, TPartView{ part, nullptr, part->Slices });
        }

        ui64 txId = 2;
        TString lastKey;
        ui64 processedRows = 0;
        ui64 deletedRows = 0;

        for (;;) {
            DB.Begin(txId, env);

            TKeyRange range;
            std::vector<TRawTypeValue> minKey;
            if (lastKey) {
                minKey.push_back(NScheme::TString::ToRawTypeValue(lastKey));
                range.MinKey = minKey;
                range.MinInclusive = false;
                // Cerr << "... starting from key " << lastKey << " inclusive=" << range.MinInclusive << Endl;
            } else {
                // Cerr << "... starting from empty key" << Endl;
            }

            std::vector<TTag> tags;
            tags.push_back(2);

            auto iter = DB.IterateRange(1, range, tags, TRowVersion(1, 60));
            while (iter->Next(ENext::Data) == EReady::Data) {
                ++processedRows;
            }

            deletedRows += iter->Stats.DeletedRowSkips;
            if (iter->Last() == EReady::Page) {
                if (auto key = iter->GetKey().Cells()) {
                    TString keyValue(key.at(0).AsBuf());
                    lastKey = keyValue;
                }
                DB.Commit(txId++, false);
                env.Load();
                continue;
            }

            DB.Commit(txId++, true);
            break;
        }

        UNIT_ASSERT_VALUES_EQUAL(processedRows, 60u);
        UNIT_ASSERT_VALUES_EQUAL(deletedRows, 40u);
    }

}

}
}
