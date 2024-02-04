#include "flat_page_btree_index.h"
#include "flat_part_btree_index_iter.h"
#include "flat_part_charge.h"
#include "flat_part_charge_btree_index.h"
#include "flat_part_charge_range.h"
#include "flat_part_iter_multi.h"
#include "test/libs/table/test_writer.h"
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NPage {

namespace {
    using namespace NTest;
    using TShortChild = TBtreeIndexNode::TShortChild;
    using TChild = TBtreeIndexNode::TChild;

    struct TTouchEnv : public NTest::TTestEnv {
        const TSharedData* TryGetPage(const TPart *part, TPageId pageId, TGroupId groupId) override
        {
            Touched[groupId].insert(pageId);
            if (Loaded[groupId].contains(pageId)) {
                return NTest::TTestEnv::TryGetPage(part, pageId, groupId);
            }
            return nullptr;
        }

        void LoadTouched() {
            for (const auto &g : Touched) {
                Loaded[g.first].insert(g.second.begin(), g.second.end());
            }
            Touched.clear();
        }

        TMap<TGroupId, TSet<TPageId>> Loaded;
        TMap<TGroupId, TSet<TPageId>> Touched;
    };

    void AssertLoadedTheSame(const TPartStore& part, const TTouchEnv& bTree, const TTouchEnv& flat, const TString& message, 
            bool allowAdditionalFirstLastPartPages = false, bool allowAdditionalFirstLoadedPage = false, bool allowLastLoadedPageDifference = false) {
        TSet<TGroupId> groupIds;
        for (const auto &c : {bTree.Loaded, flat.Loaded}) {
            for (const auto &g : c) {
                groupIds.insert(g.first);
            }
        }

        for (TGroupId groupId : groupIds) {
            TSet<TPageId> bTreeDataPages, flatDataPages;
            for (TPageId pageId : bTree.Loaded.Value(groupId, TSet<TPageId>{})) {
                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                    bTreeDataPages.insert(pageId);
                }
            }
            for (TPageId pageId : flat.Loaded.Value(groupId, TSet<TPageId>{})) {
                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                    flatDataPages.insert(pageId);
                }
            }

            // Note: it's possible that B-Tree index touches extra first / last page because it doesn't have boundary keys
            // this should be resolved using slices (see ChargeRange)
            if (allowAdditionalFirstLastPartPages) {
                for (auto additionalPageId : {IndexTools::GetFirstPageId(part, groupId), IndexTools::GetLastPageId(part, groupId)}) {
                    if (bTreeDataPages.contains(additionalPageId)) {
                        flatDataPages.insert(additionalPageId);
                    }
                }
            }
            // Note: due to implementation details it is possible that B-Tree index touches an extra page
            if (groupId.IsMain() && allowAdditionalFirstLoadedPage && flatDataPages.size() + 1 == bTreeDataPages.size()) {
                flatDataPages.insert(*bTreeDataPages.begin());
            }
            if (groupId.IsMain() && allowLastLoadedPageDifference && flatDataPages.size() + 1 == bTreeDataPages.size()) {
                flatDataPages.insert(*bTreeDataPages.rbegin());
            }
            UNIT_ASSERT_VALUES_EQUAL_C(flatDataPages, bTreeDataPages,
                TStringBuilder() << message << " Group {" << groupId.Index << "," << groupId.IsHistoric() << "}");
        }
    }

    struct TMakePartParams {
        const ui32 Levels = Max<ui32>();
        const bool Groups = false;
        const bool History = false;
        const bool Slices = false;
        const ui32 Rows = 40;
    };

    TPartEggs MakePart(TMakePartParams params) {
        NPage::TConf conf;
        switch (params.Levels) {
        case 0:
            conf.Group(0).PageRows = 999;
            break;
        case 1:
            conf.Group(0).PageRows = 2;
            break;
        case 3:
            conf.Group(0).PageRows = 2;
            conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 2;
            break;
        default:
            Y_Fail("Unknown levels");
        }

        if (params.Groups) {
            conf.Group(1).PageRows = params.Levels ? 1 : 999;
            conf.Group(2).PageRows = 3;
            conf.Group(3).PageRows = 1;

            conf.Group(1).BTreeIndexNodeKeysMin = conf.Group(1).BTreeIndexNodeKeysMax = conf.Group(0).BTreeIndexNodeKeysMax;
            conf.Group(2).BTreeIndexNodeKeysMin = conf.Group(2).BTreeIndexNodeKeysMax = 2;
            conf.Group(3).BTreeIndexNodeKeysMin = conf.Group(3).BTreeIndexNodeKeysMax = 999;
        }

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(params.Groups ? 1 : 0, 2,  NScheme::NTypeIds::Uint32)
            .Col(params.Groups ? 2 : 0, 3,  NScheme::NTypeIds::Uint64)
            .Col(params.Groups ? 3 : 0, 4,  NScheme::NTypeIds::String)
            .Key({0, 1});

        conf.WriteBTreeIndex = true;
        
        TPartCook cook(lay, conf);
        
        // making part with key gaps
        const TVector<ui32> secondCells = {1, 3, 4, 6, 7, 8, 10};
        for (ui32 i : xrange<ui32>(0, 40)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 7, secondCells[i % 7], i, static_cast<ui64>(i), TString("xxxxxxxxxx_" + std::to_string(i))));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = *eggs.Lone();

        if (params.Slices) {
            TSlices slices;
            auto partSlices = (TSlices*)part.Slices.Get();

            auto getKey = [&] (const NPage::TIndex::TRecord* record) {
                TSmallVec<TCell> key;
                for (const auto& info : part.Scheme->Groups[0].ColsKeyIdx) {
                    key.push_back(record->Cell(info));
                }
                return TSerializedCellVec(key);
            };
            auto add = [&](ui32 pageIndex1 /*inclusive*/, ui32 pageIndex2 /*exclusive*/) {
                TSlice slice;
                slice.FirstInclusive = true;
                slice.FirstRowId = pageIndex1 * 2;
                slice.FirstKey = pageIndex1 > 0 ? getKey(IndexTools::GetRecord(part, pageIndex1)) : partSlices->begin()->FirstKey;
                slice.LastInclusive = false;
                slice.LastRowId = pageIndex2 * 2;
                slice.LastKey = pageIndex2 < 20 ? getKey(IndexTools::GetRecord(part, pageIndex2)) : partSlices->begin()->LastKey;
                slices.push_back(slice);
            };
            add(0, 2);
            add(3, 4);
            add(4, 6);
            add(7, 8);
            add(8, 9);
            add(10, 14);
            add(16, 17);
            add(17, 19);
            add(19, 20);

            partSlices->clear();
            for (auto s : slices) {
                partSlices->push_back(s);
            }
        }

        if (params.Slices) {
            UNIT_ASSERT_GT(part.Slices->size(), 1);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(part.Slices->size(), 1);
        }

        Cerr << "Slices";
        part.Slices->Describe(Cerr);
        Cerr << Endl;
        Cerr << DumpPart(part, 3) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[0].LevelCount, params.Levels);
        if (params.Groups) {
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[1].LevelCount, params.Levels);
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[2].LevelCount, 2);
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[3].LevelCount, 1);
        }

        return eggs;
    }

    TVector<TCell> MakeKey(ui32 firstCell, ui32 secondCell) {
        if (secondCell <= 11) {
            // valid second cell [0 .. 11]
            return {TCell::Make(firstCell), TCell::Make(secondCell)};
        }
        if (secondCell == 12) {
            return {TCell::Make(firstCell)};
        }
        if (secondCell == 13) {
            return { };
        }
        Y_UNREACHABLE();
    }

    EReady Retry(std::function<EReady()> action, TTouchEnv& env, const TString& message, ui32 failsAllowed = 10) {
        while (true) {
            if (auto ready = action(); ready != EReady::Page) {
                return ready;
            }
            env.LoadTouched();
            UNIT_ASSERT_C(failsAllowed--, "Too many fails " + message);
        }
        Y_UNREACHABLE();
    }
}

Y_UNIT_TEST_SUITE(TPartBtreeIndexIt) {
    void AssertEqual(const TPartBtreeIndexIt& bTree, EReady bTreeReady, const TPartIndexIt& flat, EReady flatReady, const TString& message, bool allowFirstLastPageDifference = false) {
        // Note: it's possible that B-Tree index don't return Gone status for keys before the first page or keys after the last page
        if (allowFirstLastPageDifference && flatReady == EReady::Gone && bTreeReady == EReady::Data && 
                (bTree.GetRowId() == 0 || bTree.GetNextRowId() == bTree.GetEndRowId())) {
            UNIT_ASSERT_C(bTree.IsValid(), message);
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(bTreeReady, flatReady, message);
        UNIT_ASSERT_VALUES_EQUAL_C(bTree.IsValid(), flat.IsValid(), message);
        if (flat.IsValid()) {
            UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetPageId(), flat.GetPageId(), message);
            UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetRowId(), flat.GetRowId(), message);
            UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetNextRowId(), flat.GetNextRowId(), message);
        }
    }

    EReady SeekRowId(IIndexIter& iter, TTouchEnv& env, TRowId rowId, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return iter.Seek(rowId);
        }, env, message, failsAllowed);
    }

    EReady SeekLast(IIndexIter& iter, TTouchEnv& env, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return iter.SeekLast();
        }, env, message, failsAllowed);
    }

    EReady SeekKey(IIndexIter& iter, TTouchEnv& env, ESeek seek, bool reverse, TCells key, const TKeyCellDefaults *keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            if (reverse) {
                return iter.SeekReverse(seek, key, keyDefaults);
            } else {
                return iter.Seek(seek, key, keyDefaults);
            }
        }, env, message, failsAllowed);
    }

    EReady NextPrev(IIndexIter& iter, TTouchEnv& env, bool next, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            if (next) {
                return iter.Next();
            } else {
                return iter.Prev();
            }
        }, env, message, failsAllowed);
    }

    void CheckSeekRowId(const TPartStore& part) {
        for (TRowId rowId1 : xrange<TRowId>(0, part.Stat.Rows + 1)) {
            for (TRowId rowId2 : xrange<TRowId>(0, part.Stat.Rows + 1)) {
                TTouchEnv bTreeEnv, flatEnv;
                TPartBtreeIndexIt bTree(&part, &bTreeEnv, { });
                TPartIndexIt flat(&part, &flatEnv, { });

                // checking initial seek:
                {
                    TString message = TStringBuilder() << "SeekRowId< " << rowId1;
                    EReady bTreeReady = SeekRowId(bTree, bTreeEnv, rowId1, message);
                    EReady flatReady = SeekRowId(flat, flatEnv, rowId1, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId1 < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }

                // checking repositioning:
                {
                    TString message = TStringBuilder() << "SeekRowId " << rowId1 << " -> " << rowId2;
                    EReady bTreeReady = SeekRowId(bTree, bTreeEnv, rowId2, message);
                    EReady flatReady = SeekRowId(flat, flatEnv, rowId2, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId2 < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }
            }
        }
    }

    void CheckSeekLast(const TPartStore& part) {
        TTouchEnv bTreeEnv, flatEnv;
        TPartBtreeIndexIt bTree(&part, &bTreeEnv, { });
        TPartIndexIt flat(&part, &flatEnv, { });

        TString message = TStringBuilder() << "SeekLast";
        EReady bTreeReady = SeekLast(bTree, bTreeEnv, message);
        EReady flatReady = SeekLast(flat, flatEnv, message);
        UNIT_ASSERT_VALUES_EQUAL(bTreeReady, EReady::Data);
        AssertEqual(bTree, bTreeReady, flat, flatReady, message);
    }

    void CheckSeekKey(const TPartStore& part, const TKeyCellDefaults *keyDefaults) {
        for (bool reverse : {false}) {
            for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
                for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCell : xrange<ui32>(0, 14)) {
                        TVector<TCell> key = MakeKey(firstCell, secondCell);

                        TTouchEnv bTreeEnv, flatEnv;
                        TPartBtreeIndexIt bTree(&part, &bTreeEnv, { });
                        TPartIndexIt flat(&part, &flatEnv, { });

                        TStringBuilder message = TStringBuilder() << (reverse ?  "SeekKeyReverse" : "SeekKey") << "(" << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        
                        EReady bTreeReady = SeekKey(bTree, bTreeEnv, seek, reverse, key, keyDefaults, message);
                        EReady flatReady = SeekKey(flat, flatEnv, seek, reverse, key, keyDefaults, message);
                        AssertEqual(bTree, bTreeReady, flat, flatReady, message, true);
                    }
                }
            }
        }
    }

    void CheckNextPrev(const TPartStore& part) {
        for (bool next : {true, false}) {
            for (TRowId rowId : xrange<TRowId>(0, part.Stat.Rows)) {
                TTouchEnv bTreeEnv, flatEnv;
                TPartBtreeIndexIt bTree(&part, &bTreeEnv, { });
                TPartIndexIt flat(&part, &flatEnv, { });

                // checking initial seek:
                {
                    TString message = TStringBuilder() << "CheckNext " << rowId;
                    EReady bTreeReady = SeekRowId(bTree, bTreeEnv, rowId, message);
                    EReady flatReady = SeekRowId(flat, flatEnv, rowId, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }

                // checking next:
                while (true)
                {
                    TString message = TStringBuilder() << "CheckNext " << rowId << " -> " << rowId;
                    EReady bTreeReady = NextPrev(bTree, bTreeEnv, next, message);
                    EReady flatReady = NextPrev(flat, flatEnv, next, message);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                    if (flatReady == EReady::Gone) {
                        break;
                    }
                }
            }
        }
    }

    void CheckPart(TMakePartParams params) {
        TPartEggs eggs = MakePart(params);
        const auto part = *eggs.Lone();

        CheckSeekRowId(part);
        CheckSeekLast(part);
        CheckSeekKey(part, eggs.Scheme->Keys.Get());
        CheckNextPrev(part);
    }

    Y_UNIT_TEST(NoNodes) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(OneNode) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(FewNodes) {
        CheckPart({.Levels = 3});
    }
}

Y_UNIT_TEST_SUITE(TChargeBTreeIndex) {
    void DoChargeRowId(ICharge& charge, TTouchEnv& env, const TRowId row1, const TRowId row2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        while (true) {
            bool ready = reverse
                ? charge.DoReverse(row2, row1, keyDefaults, itemsLimit, bytesLimit)
                : charge.Do(row1, row2, keyDefaults, itemsLimit, bytesLimit);
            if (ready) {
                return;
            }
            env.LoadTouched();
            UNIT_ASSERT_C(failsAllowed--, "Too many fails " + message);
        }
        Y_UNREACHABLE();
    }

    bool DoChargeKeys(const TPartStore& part, ICharge& charge, TTouchEnv& env, const TCells key1, const TCells key2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        while (true) {
            auto result = reverse
                ? charge.DoReverse(key1, key2, part.Stat.Rows - 1, 0, keyDefaults, itemsLimit, bytesLimit)
                : charge.Do(key1, key2, 0, part.Stat.Rows - 1, keyDefaults, itemsLimit, bytesLimit);
            if (result.Ready) {
                return result.Overshot;
            }
            env.LoadTouched();
            UNIT_ASSERT_C(failsAllowed--, "Too many fails " + message);
        }
        Y_UNREACHABLE();
    }

    void CheckChargeRowId(const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults) {
        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : TVector<ui64>{0, 1, 2, 5, 13, 19, part.Stat.Rows - 2, part.Stat.Rows - 1}) {
                for (TRowId rowId1 : xrange<TRowId>(0, part.Stat.Rows - 1)) {
                    for (TRowId rowId2 : xrange<TRowId>(rowId1, part.Stat.Rows - 1)) {
                        TTouchEnv bTreeEnv, flatEnv;
                        TChargeBTreeIndex bTree(&bTreeEnv, part, tags, true);
                        TCharge flat(&flatEnv, part, tags, true);

                        TString message = TStringBuilder() << (reverse ? "ChargeRowIdReverse " : "ChargeRowId ") << rowId1 << " " << rowId2 << " items " << itemsLimit;
                        DoChargeRowId(bTree, bTreeEnv, rowId1, rowId2, itemsLimit, 0, reverse, *keyDefaults, message);
                        DoChargeRowId(flat, flatEnv, rowId1, rowId2, itemsLimit, 0, reverse, *keyDefaults, message);
                        AssertLoadedTheSame(part, bTreeEnv, flatEnv, message,
                            false, reverse && itemsLimit, !reverse && itemsLimit);
                    }
                }
            }
        }
    }

    void CheckChargeKeys(const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults) {
        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : TVector<ui64>{0, 1, 2, 5, 13, 19, part.Stat.Rows - 2, part.Stat.Rows - 1}) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(0, 14)) {
                        for (ui32 firstCellKey2 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                            for (ui32 secondCellKey2 : xrange<ui32>(0, 14)) {
                                TVector<TCell> key1 = MakeKey(firstCellKey1, secondCellKey1);
                                TVector<TCell> key2 = MakeKey(firstCellKey2, secondCellKey2);

                                TTouchEnv bTreeEnv, flatEnv;
                                TChargeBTreeIndex bTree(&bTreeEnv, part, tags, true);
                                TCharge flat(&flatEnv, part, tags, true);

                                TStringBuilder message = TStringBuilder() << (reverse ? "ChargeKeysReverse " : "ChargeKeys ") << "(";
                                for (auto c : key1) {
                                    message << c.AsValue<ui32>() << " ";
                                }
                                message << ") (";
                                for (auto c : key2) {
                                    message << c.AsValue<ui32>() << " ";
                                }
                                message << ") items " << itemsLimit;

                                bool bTreeOvershot = DoChargeKeys(part, bTree, bTreeEnv, key1, key2, itemsLimit, 0, reverse, *keyDefaults, message);
                                bool flatOvershot = DoChargeKeys(part, flat, flatEnv, key1, key2, itemsLimit, 0, reverse, *keyDefaults, message);
                                
                                if (!itemsLimit) {
                                    UNIT_ASSERT_VALUES_EQUAL_C(bTreeOvershot, flatOvershot, message);
                                } else if (bTreeOvershot) {
                                    // Note: due to implementation details it is possible that b-tree precharge is more precise
                                    UNIT_ASSERT_VALUES_EQUAL_C(bTreeOvershot, flatOvershot, message);
                                }
                                AssertLoadedTheSame(part, bTreeEnv, flatEnv, message, 
                                    true, reverse && itemsLimit, !reverse && itemsLimit);
                            }
                        }
                    }
                }
            }
        }
    }

    void CheckChargeBytesLimit(const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults) {
        for (bool reverse : {false, true}) {
            for (ui64 bytesLimit : xrange<ui64>(1, part.Stat.Bytes + 100, part.Stat.Bytes / 100)) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(0, 14)) {
                        TVector<TCell> key1 = MakeKey(firstCellKey1, secondCellKey1);

                        TTouchEnv limitedEnv, unlimitedEnv;
                        TChargeBTreeIndex limitedCharge(&limitedEnv, part, tags, true);
                        TChargeBTreeIndex unlimitedCharge(&unlimitedEnv, part, tags, true);

                        TStringBuilder message = TStringBuilder() << (reverse ? "ChargeBytesLimitReverse " : "ChargeBytesLimit ") << "(";
                        for (auto c : key1) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        message << ") bytes " << bytesLimit;

                        DoChargeKeys(part, limitedCharge, limitedEnv, key1, { }, 0, bytesLimit, reverse, *keyDefaults, message);
                        DoChargeKeys(part, unlimitedCharge, unlimitedEnv, key1, { }, 0, 0, reverse, *keyDefaults, message);
                        
                        TSet<TGroupId> groupIds;
                        for (const auto &c : {limitedEnv.Loaded, unlimitedEnv.Loaded}) {
                            for (const auto &g : c) {
                                groupIds.insert(g.first);
                            }
                        }
                        for (auto groupId : groupIds) {
                            ui64 size = 0;
                            TSet<TPageId> expected, loaded;
                            TVector<TPageId> unlimitedLoaded(unlimitedEnv.Loaded[groupId].begin(), unlimitedEnv.Loaded[groupId].end());
                            if (reverse) {
                                std::reverse(unlimitedLoaded.begin(), unlimitedLoaded.end());
                            }
                            for (auto pageId : unlimitedLoaded) {
                                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                                    if (expected || !groupId.IsMain()) {
                                        // do not count first main page
                                        size += part.GetPageSize(pageId, groupId);
                                    }
                                    expected.insert(pageId);
                                    if (size > bytesLimit) {
                                        break;
                                    }
                                }
                            }
                            for (auto pageId : limitedEnv.Loaded[groupId]) {
                                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                                    loaded.insert(pageId);
                                }
                            }

                            UNIT_ASSERT_VALUES_EQUAL_C(expected, loaded,
                                TStringBuilder() << message << " Group {" << groupId.Index << "," << groupId.IsHistoric() << "}");
                        }
                    }
                }
            }
        }
    }

    void CheckPart(TMakePartParams params) {
        TPartEggs eggs = MakePart(params);
        const auto part = *eggs.Lone();

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        CheckChargeRowId(part, tags, eggs.Scheme->Keys.Get());
        CheckChargeKeys(part, tags, eggs.Scheme->Keys.Get());
        CheckChargeBytesLimit(part, tags, eggs.Scheme->Keys.Get());
    }

    Y_UNIT_TEST(NoNodes) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(NoNodes_Groups) {
        CheckPart({.Levels = 0, .Groups = true});
    }

    Y_UNIT_TEST(OneNode) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(OneNode_Groups) {
        CheckPart({.Levels = 1, .Groups = true});
    }

    Y_UNIT_TEST(FewNodes) {
        CheckPart({.Levels = 3});
    }

    Y_UNIT_TEST(FewNodes_Groups) {
        CheckPart({.Levels = 3, .Groups = true});
    }
}

Y_UNIT_TEST_SUITE(TPartBtreeIndexIteration) {
    void MakeRuns(const TPartEggs& eggs, TRun& btreeRun, TRun& flatRun) {
        const auto part = *eggs.Lone();

        auto flatPart = part.CloneWithEpoch(part.Epoch);
        for (auto& slice : *part.Slices) {
            btreeRun.Insert(eggs.Lone(), slice);
            auto pages = (TVector<TBtreeIndexMeta>*)&flatPart->IndexPages.BTreeGroups;
            pages->clear();
            pages = (TVector<TBtreeIndexMeta>*)&flatPart->IndexPages.BTreeHistoric;
            pages->clear();
            flatRun.Insert(flatPart, slice);
        }
    }

    void AssertEqual(const TRunIt& bTree, EReady bTreeReady, const TRunIt& flat, EReady flatReady, const TString& message) {
        UNIT_ASSERT_VALUES_EQUAL_C(bTreeReady, flatReady, message);
        UNIT_ASSERT_VALUES_EQUAL_C(bTree.IsValid(), flat.IsValid(), message);
        UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetRowId(), flat.GetRowId(), message);
    }

    EReady Seek(TRunIt& iter, TTouchEnv& env, ESeek seek, bool reverse, TCells key, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return reverse ? iter.SeekReverse(key, seek) : iter.Seek(key, seek);
        }, env, message, failsAllowed);
    }

    EReady Next(TRunIt& iter, TTouchEnv& env, bool reverse, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return reverse ? iter.Prev() : iter.Next();
        }, env, message, failsAllowed);
    }

    void Charge(const TRun &run, const TVector<TTag> tags, TTouchEnv& env, const TCells key1, const TCells key2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        while (true) {
            auto result = reverse
                ? ChargeRangeReverse(&env, key1, key2, run, keyDefaults, tags, itemsLimit, bytesLimit, true)
                : ChargeRange(&env, key1, key2, run, keyDefaults, tags, itemsLimit, bytesLimit, true);
            if (result) {
                return;
            }
            env.LoadTouched();
            UNIT_ASSERT_C(failsAllowed--, "Too many fails " + message);
        }
        Y_UNREACHABLE();
    }

    void CheckIterate(const TPartEggs& eggs) {
        const auto part = *eggs.Lone();

        TRun btreeRun(*eggs.Scheme->Keys), flatRun(*eggs.Scheme->Keys);
        MakeRuns(eggs, btreeRun, flatRun);

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        for (bool reverse : {false, true}) {
            for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
                for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCell : xrange<ui32>(0, 14)) {
                        TVector<TCell> key = MakeKey(firstCell, secondCell);

                        TTouchEnv bTreeEnv, flatEnv;
                        TRunIt flat(flatRun, tags, eggs.Scheme->Keys, &flatEnv);
                        TRunIt bTree(btreeRun, tags, eggs.Scheme->Keys, &bTreeEnv);

                        {
                            TStringBuilder message = TStringBuilder() << (reverse ?  "IterateReverse" : "Iterate") << "(" << seek << ") ";
                            for (auto c : key) {
                                message << c.AsValue<ui32>() << " ";
                            }
                            EReady bTreeReady = Seek(bTree, bTreeEnv, seek, reverse, key, message);
                            EReady flatReady = Seek(flat, flatEnv, seek, reverse, key, message);
                            AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                            AssertLoadedTheSame(part, bTreeEnv, flatEnv, message);
                        }

                        for (ui32 steps = 1; steps <= 10; steps++) {
                            TStringBuilder message = TStringBuilder() << (reverse ?  "IterateReverse" : "Iterate") << "(" << seek << ") ";
                            for (auto c : key) {
                                message << c.AsValue<ui32>() << " ";
                            }
                            message << " --> " << steps << " steps ";
                            EReady bTreeReady = Next(bTree, bTreeEnv, reverse, message);
                            EReady flatReady = Next(flat, flatEnv, reverse, message);
                            AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                            AssertLoadedTheSame(part, bTreeEnv, flatEnv, message);
                        }
                    }
                }
            }
        }
    }

    void CheckCharge(const TPartEggs& eggs) {
        const auto part = *eggs.Lone();

        TRun btreeRun(*eggs.Scheme->Keys), flatRun(*eggs.Scheme->Keys);
        MakeRuns(eggs, btreeRun, flatRun);

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : part.Slices->size() > 1 ? TVector<ui64>{0} : TVector<ui64>{0, 1, 2, 5, 13, 19, part.Stat.Rows - 2, part.Stat.Rows - 1}) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(0, 14)) {
                        for (ui32 firstCellKey2 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                            for (ui32 secondCellKey2 : xrange<ui32>(0, 14)) {
                                TVector<TCell> key1 = MakeKey(firstCellKey1, secondCellKey1);
                                TVector<TCell> key2 = MakeKey(firstCellKey2, secondCellKey2);

                                TTouchEnv bTreeEnv, flatEnv;
                                
                                TStringBuilder message = TStringBuilder() << (reverse ? "ChargeReverse " : "Charge ") << "(";
                                for (auto c : key1) {
                                    message << c.AsValue<ui32>() << " ";
                                }
                                message << ") (";
                                for (auto c : key2) {
                                    message << c.AsValue<ui32>() << " ";
                                }
                                message << ") items " << itemsLimit;

                                Charge(btreeRun, tags, bTreeEnv, key1, key2, itemsLimit, 0, reverse, *eggs.Scheme->Keys, message);
                                Charge(flatRun, tags, flatEnv, key1, key2, itemsLimit, 0, reverse, *eggs.Scheme->Keys, message);

                                AssertLoadedTheSame(part, bTreeEnv, flatEnv, message,
                                    false, reverse && itemsLimit, !reverse && itemsLimit);
                            }
                        }
                    }
                }
            }
        }
    }

    void CheckPart(TMakePartParams params) {
        TPartEggs eggs = MakePart(params);
        const auto part = *eggs.Lone();

        CheckIterate(eggs);
        CheckCharge(eggs);
    }

    Y_UNIT_TEST(NoNodes) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(NoNodes_Groups) {
        CheckPart({.Levels = 0, .Groups = true});
    }

    Y_UNIT_TEST(OneNode) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(OneNode_Groups) {
        CheckPart({.Levels = 1, .Groups = true});
    }

    Y_UNIT_TEST(OneNode_Slices) {
        CheckPart({.Levels = 1, .Slices = true});
    }

    Y_UNIT_TEST(OneNode_Groups_Slices) {
        CheckPart({.Levels = 1, .Groups = true, .Slices = true});
    }

    Y_UNIT_TEST(FewNodes) {
        CheckPart({.Levels = 3});
    }

    Y_UNIT_TEST(FewNodes_Groups) {
        CheckPart({.Levels = 3, .Groups = true});
    }

    Y_UNIT_TEST(FewNodes_Slices) {
        CheckPart({.Levels = 3, .Slices = true});
    }

    Y_UNIT_TEST(FewNodes_Groups_Slices) {
        CheckPart({.Levels = 3, .Groups = true, .Slices = true});
    }
}

}
