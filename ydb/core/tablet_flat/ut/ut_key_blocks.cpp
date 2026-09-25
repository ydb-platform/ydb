#include "ydb/core/tablet_flat/flat_table_key_blocks.h"
#include "ydb/core/tablet_flat/flat_page_data.h"
#include "ydb/core/tablet_flat/flat_iterator.h"
#include "ydb/core/tablet_flat/flat_part_index_iter_iface.h"

#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/test_cooker.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/digest/city.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>

#include <algorithm>
#include <initializer_list>
#include <set>
#include <tuple>

namespace NKikimr {
namespace NTable {
namespace {

using namespace NTest;

TSerializedCellVec Key64(ui64 value) {
    return TSerializedCellVec(TVector<TCell>{TCell::Make(value)});
}

TString Selection(ui8 tag, const TSerializedCellVec& key) {
    TString out;
    out.push_back(char(tag));
    out += key.GetBuffer();
    return out;
}

TString Show(const TBounds& bounds, const TKeyCellDefaults& keys) {
    TStringStream out;
    bounds.Describe(out, keys);
    return out.Str();
}

bool SameBounds(const TBounds& left, const TBounds& right) {
    return left.FirstInclusive == right.FirstInclusive
        && left.LastInclusive == right.LastInclusive
        && left.FirstKey.GetBuffer() == right.FirstKey.GetBuffer()
        && left.LastKey.GetBuffer() == right.LastKey.GetBuffer();
}

NPage::TConf Conf(ui32 pageRows, bool btree = true, bool groups = false) {
    NPage::TConf conf(true, 4096);
    conf.CutIndexKeys = true;
    conf.WriteBTreeIndex = btree;
    conf.WriteFlatIndex = !btree;
    conf.Group(0).PageRows = pageRows;
    if (groups) {
        conf.Group(1).PageRows = pageRows;
        conf.Group(1).PageSize = 2048;
    }
    return conf;
}

NPage::TConf SmallBTreeConf(ui32 pageRows, bool groups = false) {
    auto conf = Conf(pageRows, true, groups);
    for (ui32 group = 0; group <= ui32(groups); ++group) {
        conf.Group(group).BTreeIndexNodeTargetSize = 128;
        conf.Group(group).BTreeIndexNodeKeysMin = 2;
    }
    return conf;
}

TIntrusiveConstPtr<TRowScheme> Scheme64(bool extraGroup = false) {
    TLayoutCook lay;
    lay.Col(0, 0, NScheme::NTypeIds::Uint64);
    lay.Col(0, 1, NScheme::NTypeIds::Uint32);
    if (extraGroup) {
        lay.Col(1, 2, NScheme::NTypeIds::String);
    }
    lay.Key({0});
    return lay.RowScheme();
}

TPartView CookRows(const TIntrusiveConstPtr<TRowScheme>& scheme, const NPage::TConf& conf, TLogoBlobID label, TEpoch epoch, ui64 rows, bool extraGroup = false) {
    TPartCook cook(scheme, conf, label, epoch);
    for (ui64 row = 0; row < rows; ++row) {
        if (extraGroup) {
            cook.Add(*TSchemedCookRow(*scheme).Col(row, ui32(row), TString("v")));
        } else {
            cook.Add(*TSchemedCookRow(*scheme).Col(row, ui32(row)));
        }
    }
    // Eggs keep the part alive only through the returned view.
    return cook.Finish().ToPartView();
}

TPartView CookKeys(const TIntrusiveConstPtr<TRowScheme>& scheme, const NPage::TConf& conf, TLogoBlobID label, TEpoch epoch,
        const TVector<ui64>& keys)
{
    TPartCook cook(scheme, conf, label, epoch);
    for (ui64 key : keys) {
        cook.Add(*TSchemedCookRow(*scheme).Col(key, ui32(key)));
    }
    return cook.Finish().ToPartView();
}

void ReplaceSlice(TPartView& view, ui64 first, bool firstIncl, ui64 last, bool lastIncl) {
    TIntrusivePtr<TSlices> run = new TSlices;
    run->emplace_back(Key64(first), Key64(last), TRowId(0), Max<TRowId>(), firstIncl, lastIncl);
    view.Slices = run;
}

void ReplaceSlices(TPartView& view, TVector<TSlice> slices) {
    view.Slices = new TSlices(std::move(slices));
}

struct TIndexOnlyEnv : TTestEnv {
    const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
        const auto type = part->GetPageType(pageId, groupId);
        UNIT_ASSERT_C(type == NPage::EPage::FlatIndex || type == NPage::EPage::BTreeIndex,
            "key-block iterator fetched a data page");
        return TTestEnv::TryGetPage(part, pageId, groupId);
    }
};

struct TTrackingIndexEnv : TIndexOnlyEnv {
    using TPage = std::tuple<const TPart*, ui32, TPageId>;
    std::set<TPage> Pages;

    const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
        Pages.emplace(part, groupId.Raw(), pageId);
        return TIndexOnlyEnv::TryGetPage(part, pageId, groupId);
    }
};

struct TLoadOnRetryEnv : TTrackingIndexEnv {
    std::set<TPage> Requested;
    ui32 Faults = 0;

    const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
        if (Requested.emplace(part, groupId.Raw(), pageId).second) {
            ++Faults;
            return nullptr;
        }
        return TTrackingIndexEnv::TryGetPage(part, pageId, groupId);
    }
};

struct TTrackingDataEnv : TTestEnv {
    std::set<TTrackingIndexEnv::TPage> Pages;
    ui64 Bytes = 0;

    const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
        if (part->GetPageType(pageId, groupId) == NPage::EPage::DataPage && groupId.Index == 0
            && Pages.emplace(part, groupId.Raw(), pageId).second)
        {
            Bytes += part->GetPageSize(pageId, groupId);
        }
        return TTestEnv::TryGetPage(part, pageId, groupId);
    }
};

// Normal row iteration supplies an independent page-footprint oracle.
ui32 ReadRangeRows(const TRowScheme& scheme, std::initializer_list<const TRun*> runs,
        const TSerializedCellVec& from, const TSerializedCellVec& end, IPages& env)
{
    TTableIter read(&scheme, scheme.Tags());
    for (const auto* run : runs) {
        auto part = MakeHolder<TRunIter>(*run, read.Remap.Tags, scheme.Keys, &env);
        UNIT_ASSERT_VALUES_EQUAL(int(part->Seek(from.GetCells(), ESeek::Lower)), int(EReady::Data));
        read.Push(std::move(part));
    }
    read.StopBefore(end.GetCells());
    ui32 rows = 0;
    EReady ready;
    while ((ready = read.Next(ENext::Data)) == EReady::Data) {
        ++rows;
    }
    UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Gone));
    return rows;
}

TKeyBlockIterator::TConf Cfg(ui32 stride = 64) {
    TKeyBlockIterator::TConf conf;
    conf.MemtableStride = stride;
    return conf;
}

TVector<TKeyBlock> CollectPositioned(TKeyBlockIterator& iter) {
    TVector<TKeyBlock> units;
    while (iter.IsValid()) {
        units.push_back(iter.Get());
        const EReady ready = iter.Next();
        if (ready == EReady::Gone) {
            break;
        }
        UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Data));
    }
    return units;
}

TVector<TKeyBlock> Collect(TKeyBlockIterator& iter) {
    const EReady ready = iter.Seek({}, true);
    UNIT_ASSERT_C(ready != EReady::Page, "unexpected page fault");
    if (ready == EReady::Gone) {
        return {};
    }
    UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Data));
    return CollectPositioned(iter);
}

void AssertSameSplits(const TSplitResult& expected, const TSplitResult& actual) {
    UNIT_ASSERT_VALUES_EQUAL(actual.Truncated, expected.Truncated);
    UNIT_ASSERT_VALUES_EQUAL(actual.Keys.size(), expected.Keys.size());
    for (size_t i = 0; i < expected.Keys.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actual.Keys[i].Key.GetBuffer(), expected.Keys[i].Key.GetBuffer());
        UNIT_ASSERT_VALUES_EQUAL(int(actual.Keys[i].Side), int(expected.Keys[i].Side));
    }
}

void AssertAbut(const TVector<TKeyBlock>& units, const TKeyCellDefaults& keys) {
    for (size_t i = 1; i < units.size(); ++i) {
        UNIT_ASSERT_C(TBounds::LessByKey(units[i - 1].Bounds, units[i].Bounds, keys),
            Show(units[i - 1].Bounds, keys) << " then " << Show(units[i].Bounds, keys));
    }
}

TVector<TSerializedCellVec> IndexSeparators(const TPart& part) {
    TTestEnv env;
    auto index = CreateIndexIter(&part, &env, {});
    TVector<TSerializedCellVec> seps;
    for (size_t i = 0;; ++i) {
        const EReady ready = i == 0 ? index->Seek(0) : index->Next();
        if (ready == EReady::Gone) {
            break;
        }
        UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Data));
        if (!index->GetKeyCellsCount()) {
            seps.emplace_back();
            continue;
        }
        TSmallVec<TCell> cells;
        index->GetKeyCells(cells);
        seps.emplace_back(cells.empty() ? TSerializedCellVec() : TSerializedCellVec(cells));
    }
    return seps;
}

TVector<TCell> RowKey(const NPage::TDataPage::TRecord& record, const TPartScheme::TGroupInfo& group) {
    TVector<TCell> key;
    key.reserve(group.ColsKeyData.size());
    for (const auto& col : group.ColsKeyData) {
        key.push_back(record.Cell(col));
    }
    return key;
}

} // namespace

Y_UNIT_TEST_SUITE(KeyBlocks) {

    Y_UNIT_TEST(SinglePartPartition) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(4), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 12);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(iter);
        AssertAbut(units, *scheme->Keys);

        TVector<TKeyBlock> owned;
        for (const auto& unit : units) {
            if (!unit.FromMemtable) {
                owned.push_back(unit);
            }
        }
        const auto seps = IndexSeparators(*view.Part);
        UNIT_ASSERT_VALUES_EQUAL(owned.size(), seps.size());
        UNIT_ASSERT_VALUES_EQUAL(owned.front().Bounds.FirstInclusive, view.Slices->front().FirstInclusive);
        UNIT_ASSERT_VALUES_EQUAL(owned.front().Bounds.FirstKey.GetBuffer(), view.Slices->front().FirstKey.GetBuffer());
        UNIT_ASSERT_VALUES_EQUAL(owned.back().Bounds.LastKey.GetBuffer(), view.Slices->back().LastKey.GetBuffer());
        for (size_t i = 1; i < seps.size(); ++i) {
            UNIT_ASSERT_C(seps[i], "internal page has no separator");
            UNIT_ASSERT_VALUES_EQUAL(owned[i].Bounds.FirstKey.GetBuffer(), seps[i].GetBuffer());
            UNIT_ASSERT(owned[i].Bounds.FirstInclusive);
            UNIT_ASSERT_VALUES_EQUAL(owned[i].SelectionKey, Selection(0x01, seps[i]));
        }
    }

    Y_UNIT_TEST(OwnerPagesUnsplit) {
        auto scheme = Scheme64();
        auto big = CookRows(scheme, Conf(10), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 40);
        auto small = CookRows(scheme, Conf(1), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 16);
        UNIT_ASSERT(big->Stat.Rows > small->Stat.Rows);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(small);
        subset.Flatten.push_back(big);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(iter);

        THashSet<TString> bigSeps;
        for (const auto& sep : IndexSeparators(*big.Part)) {
            if (sep) {
                bigSeps.insert(sep.GetBuffer());
            }
        }
        for (const auto& sep : IndexSeparators(*small.Part)) {
            if (!sep || bigSeps.contains(sep.GetBuffer())) {
                continue;
            }
            for (const auto& unit : units) {
                UNIT_ASSERT_C(unit.Bounds.FirstKey.GetBuffer() != sep.GetBuffer(),
                    "small-part separator became a unit boundary");
            }
        }
        bool sawBig = false;
        for (const auto& unit : units) {
            if (!unit.FromMemtable && bigSeps.contains(unit.Bounds.FirstKey.GetBuffer())) {
                sawBig = true;
            }
        }
        UNIT_ASSERT(sawBig);
    }

    Y_UNIT_TEST(OwnerChangesAtSliceEdges) {
        auto scheme = Scheme64();
        auto partA = CookRows(scheme, Conf(100), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 10);
        auto partB = CookRows(scheme, Conf(100), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 3);
        auto partC = CookRows(scheme, Conf(100), TLogoBlobID(3, 2, 3, 1, 0, 3), TEpoch::FromIndex(3), 20);
        ReplaceSlice(partA, 0, true, 2, true);
        ReplaceSlice(partB, 1, true, 4, true);
        ReplaceSlice(partC, 3, true, 5, true);
        UNIT_ASSERT_VALUES_EQUAL(partA->Stat.Rows, 10u);
        UNIT_ASSERT_VALUES_EQUAL(partB->Stat.Rows, 3u);
        UNIT_ASSERT_VALUES_EQUAL(partC->Stat.Rows, 20u);

        auto expect = [&](const TVector<TPartView>& order) {
            TSubset subset(TEpoch::FromIndex(1), scheme);
            for (const auto& view : order) {
                subset.Flatten.push_back(view);
            }
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
            UNIT_ASSERT_VALUES_EQUAL(layout.Regions.size(), 5u);

            auto bounds = [](TSerializedCellVec first, bool firstIncl, TSerializedCellVec last, bool lastIncl) {
                return TBounds(std::move(first), std::move(last), firstIncl, lastIncl);
            };
            const TBounds expected[] = {
                bounds({}, true, Key64(0), false),
                bounds(Key64(0), true, Key64(2), true),
                bounds(Key64(2), false, Key64(3), false),
                bounds(Key64(3), true, Key64(5), true),
                bounds(Key64(5), false, {}, false),
            };
            const TPart* owners[] = {nullptr, partA.Part.Get(), partB.Part.Get(), partC.Part.Get(), nullptr};
            for (size_t i = 0; i < 5; ++i) {
                UNIT_ASSERT_C(SameBounds(layout.Regions[i].Bounds, expected[i]),
                    Show(layout.Regions[i].Bounds, *scheme->Keys) << " vs " << Show(expected[i], *scheme->Keys));
                UNIT_ASSERT_VALUES_EQUAL(layout.Regions[i].Owner, owners[i]);
            }
        };
        expect({partA, partB, partC});
        expect({partC, partA, partB});
        expect({partB, partC, partA});

        auto low = CookRows(scheme, Conf(100), TLogoBlobID(1, 2, 3, 1, 0, 4), TEpoch::FromIndex(4), 8);
        auto high = CookRows(scheme, Conf(100), TLogoBlobID(9, 2, 3, 1, 0, 5), TEpoch::FromIndex(5), 8);
        UNIT_ASSERT(low->Label < high->Label);
        UNIT_ASSERT_VALUES_EQUAL(low->Stat.Rows, high->Stat.Rows);
        ReplaceSlice(low, 0, true, 10, true);
        ReplaceSlice(high, 0, true, 10, true);
        TSubset tie(TEpoch::FromIndex(1), scheme);
        tie.Flatten.push_back(high);
        tie.Flatten.push_back(low);
        const auto tieLayout = TKeyBlockIterator::BuildLayout(tie, Cfg(), scheme->Keys);
        const TOwnerRegion* owned = nullptr;
        for (const auto& region : tieLayout.Regions) {
            if (region.Owner) {
                owned = &region;
                break;
            }
        }
        UNIT_ASSERT(owned);
        UNIT_ASSERT_VALUES_EQUAL(owned->Owner, low.Part.Get());
    }

    Y_UNIT_TEST(OwnerChangesAtCoincidentSliceEdges) {
        auto scheme = Scheme64();
        auto left = CookRows(scheme, Conf(100), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 20);
        auto right = CookRows(scheme, Conf(100), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 20);
        ReplaceSlice(left, 0, true, 10, false);
        ReplaceSlice(right, 10, true, 20, true);
        for (bool reverse : {false, true}) {
            TSubset subset(TEpoch::FromIndex(1), scheme);
            subset.Flatten = reverse ? TVector<TPartView>{right, left} : TVector<TPartView>{left, right};
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
            UNIT_ASSERT_VALUES_EQUAL(layout.Regions.size(), 4u);
            UNIT_ASSERT(!layout.Regions.front().Owner);
            UNIT_ASSERT(!layout.Regions.back().Owner);
            UNIT_ASSERT_VALUES_EQUAL(layout.Regions[1].Owner, left.Part.Get());
            UNIT_ASSERT_VALUES_EQUAL(layout.Regions[2].Owner, right.Part.Get());
            UNIT_ASSERT(SameBounds(layout.Regions[1].Bounds, TBounds(Key64(0), Key64(10), true, false)));
            UNIT_ASSERT(SameBounds(layout.Regions[2].Bounds, TBounds(Key64(10), Key64(20), true, true)));
        }
    }

    Y_UNIT_TEST(SingletonSliceOwnership) {
        auto scheme = Scheme64();
        auto large = CookRows(scheme, Conf(100), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 30);
        auto small = CookRows(scheme, Conf(100), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 5);
        ReplaceSlice(large, 7, true, 7, true);
        ReplaceSlice(small, 0, true, 20, true);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(small);
        subset.Flatten.push_back(large);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(iter);
        AssertAbut(units, *scheme->Keys);

        const TString at = Selection(0x01, Key64(7));
        const TString after = Selection(0x02, Key64(7));
        bool sawAt = false;
        bool sawAfter = false;
        for (const auto& unit : units) {
            if (unit.SelectionKey == at) {
                sawAt = true;
                UNIT_ASSERT(unit.Bounds.FirstInclusive);
                UNIT_ASSERT(unit.Bounds.LastInclusive);
                UNIT_ASSERT_VALUES_EQUAL(unit.Bounds.FirstKey.GetBuffer(), Key64(7).GetBuffer());
                UNIT_ASSERT_VALUES_EQUAL(unit.Bounds.LastKey.GetBuffer(), Key64(7).GetBuffer());
            }
            if (unit.SelectionKey == after) {
                sawAfter = true;
                UNIT_ASSERT(!unit.Bounds.FirstInclusive);
            }
        }
        UNIT_ASSERT(sawAt);
        UNIT_ASSERT(sawAfter);
        UNIT_ASSERT(at != after);
    }

    Y_UNIT_TEST(UncoveredTail) {
        auto scheme = Scheme64();
        auto part = CookRows(scheme, Conf(100), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 5);
        TCooker cooker(scheme, TEpoch::FromIndex(2));
        for (ui64 key : {ui64(2), ui64(6), ui64(8)}) {
            cooker.Add(*TSchemedCookRow(*scheme).Col(key, ui32(key)));
        }
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(part);
        subset.Frozen.emplace_back(*cooker, (*cooker)->Snapshot());
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(1), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(1), layout);
        const auto units = Collect(iter);
        AssertAbut(units, *scheme->Keys);

        THashSet<TString> selections;
        for (const auto& unit : units) {
            selections.insert(unit.SelectionKey);
        }
        UNIT_ASSERT(!selections.contains(Selection(0x01, Key64(2))));
        UNIT_ASSERT(selections.contains(Selection(0x01, Key64(6))));
        UNIT_ASSERT(selections.contains(Selection(0x01, Key64(8))));
        bool tail = false;
        for (const auto& unit : units) {
            if (unit.FromMemtable && unit.SelectionKey == Selection(0x01, Key64(6))) {
                tail = true;
                UNIT_ASSERT_VALUES_EQUAL(unit.OwnerRows, 0u);
            }
        }
        UNIT_ASSERT(tail);
    }

    Y_UNIT_TEST(MemtableAnchorsCanonical) {
        auto scheme = Scheme64();
        TCooker cooker(scheme, TEpoch::FromIndex(1));
        TVector<ui64> keys;
        for (ui64 key = 0; key < 12; ++key) {
            cooker.Add(*TSchemedCookRow(*scheme).Col(key, ui32(key)), key == 4 ? ERowOp::Erase : ERowOp::Upsert);
            keys.push_back(key);
        }
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Frozen.emplace_back(*cooker, (*cooker)->Snapshot());
        TIndexOnlyEnv env;

        {
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(1), scheme->Keys);
            TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(1), layout);
            const auto units = Collect(iter);
            UNIT_ASSERT_VALUES_EQUAL(units.size(), keys.size() + 1);
            UNIT_ASSERT_VALUES_EQUAL(units.front().SelectionKey, TString(1, '\0'));
            UNIT_ASSERT(units.front().FromMemtable);
            for (size_t i = 0; i < keys.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(units[i + 1].SelectionKey, Selection(0x01, Key64(keys[i])));
                UNIT_ASSERT(units[i + 1].SelectionKey != units.front().SelectionKey);
                UNIT_ASSERT(units[i + 1].FromMemtable);
            }
        }

        TKeyBlockIterator::TConf conf;
        conf.MemtableStride = 5;
        conf.AnchorSalt = 0x5A4D504C45;
        TVector<ui64> anchors;
        for (ui64 key : keys) {
            const TString buf = Key64(key).GetBuffer();
            if (CityHash64WithSeed(buf.data(), buf.size(), conf.AnchorSalt) % conf.MemtableStride == 0) {
                anchors.push_back(key);
            }
        }
        UNIT_ASSERT(!anchors.empty());
        const auto layout = TKeyBlockIterator::BuildLayout(subset, conf, scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, conf, layout);
        const auto units = Collect(iter);
        UNIT_ASSERT_VALUES_EQUAL(units.size(), anchors.size() + 1);
        for (size_t i = 0; i < anchors.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(units[i + 1].SelectionKey, Selection(0x01, Key64(anchors[i])));
        }
    }

    Y_UNIT_TEST(UnequalPageDensityTelemetry) {
        auto scheme = Scheme64();
        auto big = CookRows(scheme, Conf(8), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 40);
        auto small = CookRows(scheme, Conf(1), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 10);
        big.Slices = TSlices::All();
        small.Slices = TSlices::All();
        UNIT_ASSERT(big->Stat.Rows > small->Stat.Rows);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(small);
        subset.Flatten.push_back(big);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek({}, true)), int(EReady::Data));
        TSplitRequest request;
        request.MaxExpectedBytes = Max<ui64>();
        request.MaxIndexPages = Max<ui64>();
        TSplitResult split;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, split)), int(EReady::Data));
        UNIT_ASSERT(!split.Truncated);
        UNIT_ASSERT(split.Keys.empty());
        const auto units = CollectPositioned(iter);
        UNIT_ASSERT(!units.empty());
        const auto& tele = iter.Telemetry();
        UNIT_ASSERT_VALUES_EQUAL(tele.Parts, 2u);
        UNIT_ASSERT_VALUES_EQUAL(tele.OwnerMainGroupBytes, IndexTools::CountDataSize(*big.Part, {}));
        UNIT_ASSERT_VALUES_EQUAL(tele.OtherMainGroupBytes, IndexTools::CountDataSize(*small.Part, {}));
        UNIT_ASSERT(tele.OwnerRowsPerUnitMax > 0);
        UNIT_ASSERT(tele.IndexPagesTouched > 0);

        TRowId maxRows = 0;
        {
            TTestEnv raw;
            auto index = CreateIndexIter(big.Part.Get(), &raw, {});
            for (size_t i = 0;; ++i) {
                const EReady ready = i == 0 ? index->Seek(0) : index->Next();
                if (ready != EReady::Data) {
                    break;
                }
                maxRows = Max(maxRows, index->GetNextRowId() - index->GetRowId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(tele.OwnerRowsPerUnitMax, maxRows);
    }

    Y_UNIT_TEST(Slices) {
        auto scheme = Scheme64();
        auto view = CookKeys(scheme, Conf(100), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1),
            {0, 1, 2, 8, 9, 10});
        ReplaceSlices(view, {
            TSlice(Key64(0), Key64(2), 0, 2, true, true),
            TSlice(Key64(8), Key64(10), 3, 5, true, true),
        });
        view.Slices->Validate();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(iter);
        ui32 gaps = 0;
        for (const auto& unit : units) {
            if (unit.FromMemtable && SameBounds(unit.Bounds, TBounds(Key64(2), Key64(8), false, false))) {
                ++gaps;
                UNIT_ASSERT_VALUES_EQUAL(unit.OwnerRows, 0u);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(gaps, 1u);

        // Adjacent row spans can overlap between their exclusive key bounds.
        auto adjacent = CookKeys(scheme, Conf(100), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(1),
            {1, 2, 3, 4, 5, 6});
        ReplaceSlices(adjacent, {
            TSlice(Key64(1), Key64(4), 0, 3, true, false),
            TSlice(Key64(3), Key64(6), 2, 5, false, true),
        });
        adjacent.Slices->Validate();
        TSubset overlapping(TEpoch::FromIndex(1), scheme);
        overlapping.Flatten.push_back(adjacent);
        const auto adjacentLayout = TKeyBlockIterator::BuildLayout(overlapping, Cfg(), scheme->Keys);
        UNIT_ASSERT_VALUES_EQUAL(adjacentLayout.Regions.size(), 3u);
        UNIT_ASSERT(!adjacentLayout.Regions.front().Owner);
        UNIT_ASSERT(!adjacentLayout.Regions.back().Owner);
        UNIT_ASSERT_VALUES_EQUAL(adjacentLayout.Regions[1].Owner, adjacent.Part.Get());
        UNIT_ASSERT(SameBounds(adjacentLayout.Regions[1].Bounds, TBounds(Key64(1), Key64(6), true, true)));

        auto empty = CookRows(scheme, Conf(1), TLogoBlobID(3, 2, 3, 1, 0, 3), TEpoch::FromIndex(3), 20);
        empty.Slices = new TSlices;
        TSubset withEmpty(TEpoch::FromIndex(1), scheme);
        withEmpty.Flatten = {view, empty};
        const auto emptyLayout = TKeyBlockIterator::BuildLayout(withEmpty, Cfg(), scheme->Keys);
        UNIT_ASSERT_VALUES_EQUAL(emptyLayout.Regions.size(), layout.Regions.size());
        for (size_t i = 0; i < layout.Regions.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(emptyLayout.Regions[i].Owner, layout.Regions[i].Owner);
            UNIT_ASSERT(SameBounds(emptyLayout.Regions[i].Bounds, layout.Regions[i].Bounds));
        }
        TKeyBlockIterator withoutEmptyRows(withEmpty, &env, scheme->Keys, Cfg(), emptyLayout);
        UNIT_ASSERT_VALUES_EQUAL(int(withoutEmptyRows.Seek({}, true)), int(EReady::Data));
        TSplitRequest request;
        request.MaxExpectedBytes = Max<ui64>();
        request.MaxIndexPages = Max<ui64>();
        TSplitResult split;
        UNIT_ASSERT_VALUES_EQUAL(int(withoutEmptyRows.SplitPoints(request, split)), int(EReady::Data));
        const auto& telemetry = withoutEmptyRows.Telemetry();
        UNIT_ASSERT_VALUES_EQUAL(telemetry.OwnerMainGroupBytes + telemetry.OtherMainGroupBytes,
            IndexTools::CountDataSize(*view.Part, {}));
    }

    Y_UNIT_TEST(CompositeKeysAndNulls) {
        TLayoutCook lay;
        lay.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String).Key({0, 1});
        auto scheme = lay.RowScheme();
        NPage::TConf conf(true, 4096);
        conf.CutIndexKeys = true;
        conf.Group(0).PageRows = 3;
        TPartCook cook(scheme, conf, TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1));
        const char* seconds[] = {"aaa", "aab", "aac", "baaaa", "bab", "ccc"};
        for (ui32 major = 1; major <= 3; ++major) {
            cook.Add(*TSchemedCookRow(*scheme).Col(major, nullptr));
            for (const char* second : seconds) {
                cook.Add(*TSchemedCookRow(*scheme).Col(major, second));
            }
        }
        TPartView view = cook.Finish().ToPartView();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(iter);
        UNIT_ASSERT(units.size() >= 2);
        AssertAbut(units, *scheme->Keys);
    }

    Y_UNIT_TEST(SeekPrefixLowerBound) {
        TLayoutCook lay;
        lay.Col(0, 0, NScheme::NTypeIds::Uint64)
            .Col(0, 1, NScheme::NTypeIds::Uint64).Key({0, 1});
        const auto scheme = lay.RowScheme();
        const auto prefix = Key64(1);
        const TSerializedCellVec expectedStart(TVector<TCell>{TCell::Make(ui64(1)), TCell::Make(ui64(2))});
        auto check = [&](const TSubset& subset) {
            TIndexOnlyEnv env;
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(1), scheme->Keys);
            TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(1), layout);
            for (bool inclusive : {false, true}) {
                UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(prefix.GetCells(), inclusive)), int(EReady::Data));
                UNIT_ASSERT_VALUES_EQUAL(iter.Get().Bounds.FirstKey.GetBuffer(), expectedStart.GetBuffer());
            }
        };
        for (bool btree : {false, true}) {
            TPartCook cook(scheme, Conf(1, btree), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1));
            for (ui64 major : {ui64(1), ui64(2)}) {
                for (ui64 minor : {ui64(1), ui64(2)}) {
                    cook.Add(*TSchemedCookRow(*scheme).Col(major, minor));
                }
            }
            auto view = cook.Finish().ToPartView();
            view.Slices = TSlices::All();
            TSubset subset(TEpoch::FromIndex(1), scheme);
            subset.Flatten.push_back(view);
            check(subset);
        }
        TCooker cooker(scheme, TEpoch::FromIndex(1));
        for (ui64 major : {ui64(1), ui64(2)}) {
            for (ui64 minor : {ui64(1), ui64(2)}) {
                cooker.Add(*TSchemedCookRow(*scheme).Col(major, minor));
            }
        }
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Frozen.emplace_back(*cooker, (*cooker)->Snapshot());
        check(subset);
    }

    Y_UNIT_TEST(SeekMidUnit) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(8), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 24);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator scan(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(scan);

        const TKeyBlock* wide = nullptr;
        for (const auto& unit : units) {
            if (unit.FromMemtable || !unit.Bounds.FirstKey || !unit.Bounds.LastKey) {
                continue;
            }
            const ui64 first = unit.Bounds.FirstKey.GetCells()[0].AsValue<ui64>();
            const ui64 last = unit.Bounds.LastKey.GetCells()[0].AsValue<ui64>();
            if (last > first + 1) {
                wide = &unit;
                break;
            }
        }
        UNIT_ASSERT(wide);
        const ui64 mid = wide->Bounds.FirstKey.GetCells()[0].AsValue<ui64>() + 1;
        const TCell midCell = TCell::Make(mid);
        TKeyBlockIterator inc(subset, &env, scheme->Keys, Cfg(), layout);
        TKeyBlockIterator exc(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(inc.Seek({&midCell, 1}, true)), int(EReady::Data));
        UNIT_ASSERT_VALUES_EQUAL(int(exc.Seek({&midCell, 1}, false)), int(EReady::Data));
        UNIT_ASSERT_VALUES_EQUAL(inc.Get().SelectionKey, wide->SelectionKey);
        UNIT_ASSERT_VALUES_EQUAL(exc.Get().SelectionKey, wide->SelectionKey);

        for (bool inclusive : {false, true}) {
            auto& iter = inclusive ? inc : exc;
            TSplitRequest request;
            request.EndKey = Key64(mid);
            request.EndInclusive = !inclusive;
            TSplitResult result;
            result.Truncated = true;
            UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
            UNIT_ASSERT(result.Keys.empty() && !result.Truncated);
            request.EndKey = wide->Bounds.FirstKey;
            UNIT_ASSERT_EXCEPTION_CONTAINS(iter.SplitPoints(request, result), yexception, "end is before the current position");
        }

        const TKeyBlock* closed = nullptr;
        for (auto it = units.rbegin(); it != units.rend(); ++it) {
            if (!it->FromMemtable && it->Bounds.LastInclusive && it->Bounds.LastKey) {
                closed = &*it;
                break;
            }
        }
        UNIT_ASSERT(closed);
        const TCell endCell = closed->Bounds.LastKey.GetCells()[0];
        TKeyBlockIterator atEnd(subset, &env, scheme->Keys, Cfg(), layout);
        TKeyBlockIterator afterEnd(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(atEnd.Seek({&endCell, 1}, true)), int(EReady::Data));
        UNIT_ASSERT_VALUES_EQUAL(int(afterEnd.Seek({&endCell, 1}, false)), int(EReady::Data));
        UNIT_ASSERT_VALUES_EQUAL(atEnd.Get().SelectionKey, closed->SelectionKey);
        UNIT_ASSERT(afterEnd.Get().SelectionKey != closed->SelectionKey);
    }

    Y_UNIT_TEST(SeekTouchesOnlyNearbyIndexPages) {
        auto scheme = Scheme64();
        const auto conf = SmallBTreeConf(1);
        auto view = CookRows(scheme, conf, TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 1024);
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TTrackingIndexEnv env;
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto target = Key64(700);
        const ui64 pageLimit = 4 * (view->IndexPages.GetBTree({}).LevelCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(target.GetCells(), true)), int(EReady::Data));
        UNIT_ASSERT_C(env.Pages.size() <= pageLimit,
            "narrow seek fetched " << env.Pages.size() << " index pages, limit " << pageLimit);
        UNIT_ASSERT_VALUES_EQUAL(iter.Get().Bounds.FirstKey.GetBuffer(), target.GetBuffer());
        env.Pages.clear();
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Next()), int(EReady::Data));
        UNIT_ASSERT_C(env.Pages.size() <= pageLimit,
            "next unit fetched " << env.Pages.size() << " index pages, limit " << pageLimit);
        UNIT_ASSERT_VALUES_EQUAL(iter.Get().Bounds.FirstKey.GetBuffer(), Key64(701).GetBuffer());
    }

    Y_UNIT_TEST(PageFaultRestart) {
        auto scheme = Scheme64();
        const auto conf = SmallBTreeConf(1);
        auto owner = CookRows(scheme, conf, TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 80);
        auto other = CookRows(scheme, conf, TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 60);
        owner.Slices = TSlices::All();
        other.Slices = TSlices::All();
        UNIT_ASSERT(owner->IndexPages.GetBTree({}).LevelCount > 1);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten = {owner, other};
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        const auto target = Key64(10);
        TIndexOnlyEnv warmEnv;
        TKeyBlockIterator warm(subset, &warmEnv, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(warm.Seek(target.GetCells(), true)), int(EReady::Data));
        const TString selection = warm.Get().SelectionKey;
        const TBounds certain(target, Key64(40), true, false);
        TSplitRequest request;
        request.Certain = &certain;
        request.MaxExpectedBytes = 500;
        request.MaxIndexPages = Max<ui64>();
        TSplitResult expected;
        UNIT_ASSERT_VALUES_EQUAL(int(warm.SplitPoints(request, expected)), int(EReady::Data));
        UNIT_ASSERT(!expected.Truncated);
        UNIT_ASSERT(!expected.Keys.empty());

        TLoadOnRetryEnv faultEnv;
        TKeyBlocksLayout retryLayout;
        THolder<TKeyBlockIterator> faulted;
        auto restartAt = [&](const TSerializedCellVec& key, bool inclusive) {
            for (ui32 attempt = 0; attempt < 512; ++attempt) {
                faulted.Reset();
                retryLayout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
                faulted = MakeHolder<TKeyBlockIterator>(subset, &faultEnv, scheme->Keys, Cfg(), retryLayout);
                const EReady ready = faulted->Seek(key.GetCells(), inclusive);
                if (ready != EReady::Page) {
                    UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Data));
                    return ready;
                }
                UNIT_ASSERT(!faulted->IsValid());
                UNIT_ASSERT_EXCEPTION_CONTAINS(faulted->Next(), yexception, "Next after Page");
            }
            UNIT_FAIL("index page retries made no progress");
            return EReady::Gone;
        };
        restartAt(target, true);
        UNIT_ASSERT_VALUES_EQUAL(faulted->Get().SelectionKey, selection);
        UNIT_ASSERT(faultEnv.Faults > 1);

        bool nextFault = false;
        while (warm.Next() == EReady::Data) {
            const TBounds previous = faulted->Get().Bounds;
            const EReady ready = faulted->Next();
            if (ready == EReady::Page) {
                UNIT_ASSERT(!faulted->IsValid());
                UNIT_ASSERT_EXCEPTION_CONTAINS(faulted->Next(), yexception, "Next after Page");
                restartAt(previous.LastKey, !previous.LastInclusive);
                nextFault = true;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Data));
            }
            UNIT_ASSERT_VALUES_EQUAL(faulted->Get().SelectionKey, warm.Get().SelectionKey);
            if (nextFault) {
                break;
            }
        }
        UNIT_ASSERT_C(nextFault, "Next did not exercise a cold index page");
        const ui32 beforeSplit = faultEnv.Faults;
        TSplitResult sentinel;
        sentinel.Truncated = true;
        sentinel.Keys.push_back({Key64(99999), EBoundarySide::After});
        TSplitResult actual = sentinel;
        bool finished = false;
        for (ui32 attempt = 0; attempt < 512; ++attempt) {
            restartAt(target, true);
            const EReady ready = faulted->SplitPoints(request, actual);
            UNIT_ASSERT_VALUES_EQUAL(faulted->Get().SelectionKey, selection);
            if (ready != EReady::Page) {
                UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Data));
                finished = true;
                break;
            }
            AssertSameSplits(sentinel, actual);
            TSplitRequest prefix = request;
            prefix.EndKey = faulted->Get().Bounds.LastKey;
            prefix.EndInclusive = faulted->Get().Bounds.LastInclusive;
            prefix.MaxIndexPages = 0;
            TSplitResult prefixResult;
            UNIT_ASSERT_VALUES_EQUAL(int(faulted->SplitPoints(prefix, prefixResult)), int(EReady::Data));
            UNIT_ASSERT(prefixResult.Keys.empty() && !prefixResult.Truncated);
        }
        UNIT_ASSERT_C(finished, "split page retries made no progress");
        UNIT_ASSERT(faultEnv.Faults > beforeSplit);
        AssertSameSplits(expected, actual);
        CollectPositioned(*faulted);
        UNIT_ASSERT(!faulted->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(int(faulted->Next()), int(EReady::Gone));
    }

    Y_UNIT_TEST(ColdParts) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(8), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 8);
        TSubset plain(TEpoch::FromIndex(1), scheme);
        plain.Flatten.push_back(view);
        TSubset cold(TEpoch::FromIndex(1), scheme);
        cold.Flatten.push_back(view);
        cold.ColdParts.push_back(new TColdPart(TLogoBlobID(9, 2, 3, 1, 0, 9), TEpoch::FromIndex(3)));
        const auto plainLayout = TKeyBlockIterator::BuildLayout(plain, Cfg(), scheme->Keys);
        const auto coldLayout = TKeyBlockIterator::BuildLayout(cold, Cfg(), scheme->Keys);
        UNIT_ASSERT(plainLayout.LayoutId != coldLayout.LayoutId);
        TIndexOnlyEnv env;
        TKeyBlockIterator iter(cold, &env, scheme->Keys, Cfg(), coldLayout);
        UNIT_ASSERT(iter.HasColdParts());
        UNIT_ASSERT(!Collect(iter).empty());
        TKeyBlockIterator plainIter(plain, &env, scheme->Keys, Cfg(), plainLayout);
        UNIT_ASSERT(!plainIter.HasColdParts());
    }

    Y_UNIT_TEST(FlatIndexPart) {
        auto scheme = Scheme64();
        auto btree = CookRows(scheme, Conf(4, true), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 12);
        auto flat = CookRows(scheme, Conf(4, false), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 12);
        UNIT_ASSERT(btree->IndexPages.HasBTree());
        UNIT_ASSERT(!flat->IndexPages.HasBTree());
        UNIT_ASSERT(flat->IndexPages.HasFlat());

        auto owned = [&](TPartView view) {
            TSubset subset(TEpoch::FromIndex(1), scheme);
            subset.Flatten.push_back(view);
            TIndexOnlyEnv env;
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
            TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
            TVector<TKeyBlock> units;
            for (const auto& unit : Collect(iter)) {
                if (!unit.FromMemtable) {
                    units.push_back(unit);
                }
            }
            return units;
        };
        const auto left = owned(btree);
        const auto right = owned(flat);
        UNIT_ASSERT_VALUES_EQUAL(left.size(), right.size());
        for (size_t i = 0; i < left.size(); ++i) {
            UNIT_ASSERT_C(SameBounds(left[i].Bounds, right[i].Bounds),
                Show(left[i].Bounds, *scheme->Keys) << " vs " << Show(right[i].Bounds, *scheme->Keys));
            UNIT_ASSERT_VALUES_EQUAL(left[i].SelectionKey, right[i].SelectionKey);
        }
        const auto seps = IndexSeparators(*flat.Part);
        UNIT_ASSERT_VALUES_EQUAL(right.size(), seps.size());
    }

    Y_UNIT_TEST(LayoutReuse) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(8), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 8);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        const auto first = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        const auto second = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        UNIT_ASSERT_VALUES_EQUAL(first.LayoutId, second.LayoutId);
        UNIT_ASSERT_VALUES_EQUAL(first.LayoutId.size(), 16u);
        UNIT_ASSERT_VALUES_EQUAL(first.Regions.size(), second.Regions.size());

        auto shifted = view;
        ReplaceSlice(shifted, 0, true, 3, true);
        TSubset changed(TEpoch::FromIndex(1), scheme);
        changed.Flatten.push_back(shifted);
        const auto sliced = TKeyBlockIterator::BuildLayout(changed, Cfg(), scheme->Keys);
        UNIT_ASSERT(sliced.LayoutId != first.LayoutId);

        TCooker cooker(scheme, TEpoch::FromIndex(7));
        cooker.Add(*TSchemedCookRow(*scheme).Col(ui64(1), ui32(1)));
        TMemTableSnapshot before(*cooker, (*cooker)->Snapshot());
        TSubset beforeSubset(TEpoch::FromIndex(1), scheme);
        beforeSubset.Frozen.push_back(before);
        const auto beforeLayout = TKeyBlockIterator::BuildLayout(beforeSubset, Cfg(1), scheme->Keys);
        cooker.Add(*TSchemedCookRow(*scheme).Col(ui64(2), ui32(2)));
        TMemTableSnapshot after(*cooker, (*cooker)->Snapshot());
        UNIT_ASSERT_VALUES_EQUAL(before->Epoch, after->Epoch);
        UNIT_ASSERT_VALUES_EQUAL(before.Snapshot.Iterator().Size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(after.Snapshot.Iterator().Size(), 2u);
        TSubset afterSubset(TEpoch::FromIndex(1), scheme);
        afterSubset.Frozen.push_back(after);
        const auto afterLayout = TKeyBlockIterator::BuildLayout(afterSubset, Cfg(1), scheme->Keys);
        UNIT_ASSERT(beforeLayout.LayoutId != afterLayout.LayoutId);
        const auto beforeAgain = TKeyBlockIterator::BuildLayout(beforeSubset, Cfg(1), scheme->Keys);
        UNIT_ASSERT_VALUES_EQUAL(beforeAgain.LayoutId, beforeLayout.LayoutId);

        cooker.Add(*TSchemedCookRow(*scheme).Col(ui64(2), ui32(99)));
        TMemTableSnapshot updated(*cooker, (*cooker)->Snapshot());
        UNIT_ASSERT_VALUES_EQUAL(updated.Snapshot.Iterator().Size(), after.Snapshot.Iterator().Size());
        TSubset updatedSubset(TEpoch::FromIndex(1), scheme);
        updatedSubset.Frozen.push_back(updated);
        const auto updatedLayout = TKeyBlockIterator::BuildLayout(updatedSubset, Cfg(1), scheme->Keys);
        UNIT_ASSERT_VALUES_EQUAL(updatedLayout.LayoutId, afterLayout.LayoutId);
        UNIT_ASSERT_VALUES_EQUAL(TKeyBlockIterator::BuildLayout(afterSubset, Cfg(1), scheme->Keys).LayoutId, afterLayout.LayoutId);
        UNIT_ASSERT_VALUES_EQUAL(TKeyBlockIterator::BuildLayout(beforeSubset, Cfg(1), scheme->Keys).LayoutId, beforeLayout.LayoutId);

        TKeyBlockIterator::TConf salt = Cfg(1);
        salt.AnchorSalt ^= 0xff;
        const auto salted = TKeyBlockIterator::BuildLayout(beforeSubset, salt, scheme->Keys);
        UNIT_ASSERT(salted.LayoutId != beforeLayout.LayoutId);
        TKeyBlockIterator::TConf stride = Cfg(1);
        stride.MemtableStride = 3;
        const auto strided = TKeyBlockIterator::BuildLayout(beforeSubset, stride, scheme->Keys);
        UNIT_ASSERT(strided.LayoutId != beforeLayout.LayoutId);
    }

    Y_UNIT_TEST(LayoutIdentityIncludesKeySchema) {
        auto scheme = Scheme64();
        TCooker cooker(scheme, TEpoch::FromIndex(1));
        cooker.Add(*TSchemedCookRow(*scheme).Col(ui64(7), ui32(1)));
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Frozen.emplace_back(*cooker, (*cooker)->Snapshot());

        auto extended = [](ui32 value) {
            TLayoutCook lay;
            lay.Col(0, 0, NScheme::NTypeIds::Uint64)
                .Col(0, 1, NScheme::NTypeIds::Uint32)
                .Col(0, 2, NScheme::NTypeIds::Uint32, TCell::Make(value))
                .Key({0, 2});
            return lay.RowScheme();
        };
        const auto firstScheme = extended(77);
        const auto equalScheme = extended(77);
        const auto changedScheme = extended(88);
        const auto original = TKeyBlockIterator::BuildLayout(subset, Cfg(1), scheme->Keys);
        const auto first = TKeyBlockIterator::BuildLayout(subset, Cfg(1), firstScheme->Keys);
        const auto equal = TKeyBlockIterator::BuildLayout(subset, Cfg(1), equalScheme->Keys);
        const auto changed = TKeyBlockIterator::BuildLayout(subset, Cfg(1), changedScheme->Keys);
        UNIT_ASSERT(original.LayoutId != first.LayoutId);
        UNIT_ASSERT_VALUES_EQUAL(first.LayoutId, equal.LayoutId);
        UNIT_ASSERT(first.LayoutId != changed.LayoutId);
        TLayoutCook signedLay;
        signedLay.Col(0, 0, NScheme::NTypeIds::Uint64)
            .Col(0, 1, NScheme::NTypeIds::Uint32)
            .Col(0, 2, NScheme::NTypeIds::Int32, TCell::Make(i32(77)))
            .Key({0, 2});
        const auto changedType = TKeyBlockIterator::BuildLayout(subset, Cfg(1), signedLay.RowScheme()->Keys);
        UNIT_ASSERT(first.LayoutId != changedType.LayoutId);

        TIndexOnlyEnv env;
        TKeyBlockIterator firstIter(subset, &env, firstScheme->Keys, Cfg(1), first);
        TKeyBlockIterator changedIter(subset, &env, changedScheme->Keys, Cfg(1), changed);
        const auto firstUnits = Collect(firstIter);
        const auto changedUnits = Collect(changedIter);
        UNIT_ASSERT_VALUES_EQUAL(firstUnits.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(changedUnits.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(firstUnits[1].Bounds.FirstKey.GetCells().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(firstUnits[1].Bounds.FirstKey.GetCells()[1].AsValue<ui32>(), 77u);
        UNIT_ASSERT_VALUES_EQUAL(changedUnits[1].Bounds.FirstKey.GetCells()[1].AsValue<ui32>(), 88u);
        UNIT_ASSERT(firstUnits[1].SelectionKey != changedUnits[1].SelectionKey);
    }

    Y_UNIT_TEST(SeparatorCheck) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(3), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 15);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv guard;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &guard, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT(!Collect(iter).empty());

        TTestEnv env;
        auto index = CreateIndexIter(view.Part.Get(), &env, {});
        struct TPage {
            TPageId Id;
            TSerializedCellVec Sep;
            bool HasSep = false;
        };
        TVector<TPage> pages;
        for (size_t i = 0;; ++i) {
            const EReady ready = i == 0 ? index->Seek(0) : index->Next();
            if (ready != EReady::Data) {
                break;
            }
            TPage page;
            page.Id = index->GetPageId();
            if (index->GetKeyCellsCount()) {
                TSmallVec<TCell> cells;
                index->GetKeyCells(cells);
                if (!cells.empty()) {
                    page.Sep = TSerializedCellVec(cells);
                    page.HasSep = true;
                }
            }
            pages.push_back(std::move(page));
        }
        UNIT_ASSERT(pages.size() >= 2);
        const auto& group = view->Scheme->Groups[0];
        for (size_t i = 1; i < pages.size(); ++i) {
            if (!pages[i].HasSep) {
                continue;
            }
            NPage::TDataPage prev(env.TryGetPage(view.Part.Get(), pages[i - 1].Id, {}));
            NPage::TDataPage next(env.TryGetPage(view.Part.Get(), pages[i].Id, {}));
            UNIT_ASSERT(prev->Count > 0);
            UNIT_ASSERT(next->Count > 0);
            const auto last = RowKey(*prev->Record(prev->Count - 1), group);
            const auto first = RowKey(*next->Record(0), group);
            UNIT_ASSERT_C(ComparePartKeys(last, pages[i].Sep.GetCells(), *scheme->Keys) < 0,
                "last key of page " << i - 1 << " is not < separator");
            UNIT_ASSERT_C(ComparePartKeys(pages[i].Sep.GetCells(), first, *scheme->Keys) <= 0,
                "separator is not <= first key of page " << i);
        }
    }

    Y_UNIT_TEST(SplitPointsPrefixUpperBound) {
        TLayoutCook lay;
        lay.Col(0, 0, NScheme::NTypeIds::Uint64)
            .Col(0, 1, NScheme::NTypeIds::Uint64).Key({0, 1});
        auto scheme = lay.RowScheme();
        TPartCook cook(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1));
        for (ui64 suffix = 1; suffix <= 8; ++suffix) {
            cook.Add(*TSchemedCookRow(*scheme).Col(ui64(10), suffix));
        }
        auto view = cook.Finish().ToPartView();
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const TSerializedCellVec start(TVector<TCell>{TCell::Make(ui64(10)), TCell::Make(ui64(1))});
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(start.GetCells(), true)), int(EReady::Data));
        TSplitRequest request;
        request.EndKey = Key64(10);
        request.EndInclusive = true;
        request.MaxExpectedBytes = 1;
        request.MaxIndexPages = Max<ui64>();
        TSplitResult prefix;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, prefix)), int(EReady::Data));
        request.EndKey = TSerializedCellVec(TVector<TCell>{TCell::Make(ui64(10)), TCell::Make(ui64(8))});
        TSplitResult full;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, full)), int(EReady::Data));
        UNIT_ASSERT_C(!full.Keys.empty(), "full bound should split");
        AssertSameSplits(full, prefix);
    }

    Y_UNIT_TEST(SplitPointsSparseSuccessorFootprint) {
        auto scheme = Scheme64();
        auto owner = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(2), 10);
        auto sparse = CookKeys(scheme, Conf(2), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(1),
            TVector<ui64>{0, 1, 20, 21});
        owner.Slices = TSlices::All();
        sparse.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(2), scheme);
        subset.Flatten = {owner, sparse};

        TTrackingDataEnv data;
        TPageId successor = Max<TPageId>();
        {
            TTestEnv raw;
            auto index = CreateIndexIter(sparse.Part.Get(), &raw, {});
            UNIT_ASSERT_VALUES_EQUAL(int(index->Seek(TRowId(0))), int(EReady::Data));
            UNIT_ASSERT_VALUES_EQUAL(int(index->Next()), int(EReady::Data));
            successor = index->GetPageId();
            TSmallVec<TCell> cells;
            index->GetKeyCells(cells);
            UNIT_ASSERT_VALUES_EQUAL(cells[0].AsValue<ui64>(), 20u);
        }

        const auto from = Key64(1);
        const auto end = Key64(3);
        TRun ownerRun(*scheme->Keys);
        TRun sparseRun(*scheme->Keys);
        ownerRun.Insert(owner.Part, owner.Slices->front());
        sparseRun.Insert(sparse.Part, sparse.Slices->front());
        UNIT_ASSERT_VALUES_EQUAL(ReadRangeRows(*scheme, {&ownerRun, &sparseRun}, from, end, data), 2u);
        UNIT_ASSERT_C(data.Pages.contains({sparse.Part.Get(), 0, successor}),
            "real IterateRange path did not read sparse page at 20");

        TIndexOnlyEnv indexEnv;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator estimate(subset, &indexEnv, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(estimate.Seek(from.GetCells(), true)), int(EReady::Data));
        TSplitRequest request;
        request.EndKey = end;
        request.Rate = 1;
        request.MaxExpectedBytes = data.Bytes - 1;
        request.MaxIndexPages = Max<ui64>();
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(estimate.SplitPoints(request, result)), int(EReady::Data));
        const auto& telemetry = estimate.Telemetry();
        UNIT_ASSERT_C(!result.Keys.empty(), "preflight accepted actual " << data.Bytes
            << " bytes with budget " << request.MaxExpectedBytes << "; metadata counted "
            << telemetry.OwnerMainGroupBytes + telemetry.OtherMainGroupBytes);
    }

    Y_UNIT_TEST(SplitPointsProbeNextRunSliceAcrossGap) {
        const auto scheme = Scheme64();
        auto owner = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(3), 10);
        auto first = CookKeys(scheme, Conf(2), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), {0, 1});
        auto future = CookKeys(scheme, Conf(2), TLogoBlobID(3, 2, 3, 1, 0, 3), TEpoch::FromIndex(1), {20, 21});
        owner.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(3), scheme);
        subset.Flatten = {owner, first, future};
        const auto from = Key64(1);
        const auto end = Key64(3);
        TRun ownerRun(*scheme->Keys);
        TRun disjointRun(*scheme->Keys);
        ownerRun.Insert(owner.Part, owner.Slices->front());
        disjointRun.Insert(first.Part, first.Slices->front());
        disjointRun.Insert(future.Part, future.Slices->front());
        TTrackingDataEnv data;
        UNIT_ASSERT_VALUES_EQUAL(ReadRangeRows(*scheme, {&ownerRun, &disjointRun}, from, end, data), 2u);
        bool probedFuture = false;
        for (const auto& page : data.Pages) {
            probedFuture |= std::get<0>(page) == future.Part.Get();
        }
        UNIT_ASSERT_C(probedFuture, "range iteration did not probe the next slice beyond its end");

        TTrackingIndexEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(from.GetCells(), true)), int(EReady::Data));
        TSplitRequest request;
        request.EndKey = end;
        request.MaxExpectedBytes = data.Bytes - 1;
        request.MaxIndexPages = Max<ui64>();
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT(!result.Truncated);
        UNIT_ASSERT(!result.Keys.empty());
        UNIT_ASSERT(iter.Telemetry().OwnerMainGroupBytes + iter.Telemetry().OtherMainGroupBytes >= data.Bytes);
    }

    Y_UNIT_TEST(SplitPointsProbeRespectsExclusiveSliceStart) {
        const auto scheme = Scheme64();
        auto owner = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(2), 50);
        auto sparse = CookKeys(scheme, Conf(1), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(1), {19, 20, 21, 22});
        owner.Slices = TSlices::All();
        ReplaceSlices(sparse, {TSlice(Key64(19), Key64(22), 0, 3, false, true)});
        TSubset subset(TEpoch::FromIndex(2), scheme);
        subset.Flatten = {owner, sparse};
        const auto from = Key64(18);
        const auto end = Key64(20);
        TRun ownerRun(*scheme->Keys);
        TRun sparseRun(*scheme->Keys);
        ownerRun.Insert(owner.Part, owner.Slices->front());
        sparseRun.Insert(sparse.Part, sparse.Slices->front());
        TTrackingDataEnv data;
        UNIT_ASSERT_VALUES_EQUAL(ReadRangeRows(*scheme, {&ownerRun, &sparseRun}, from, end, data), 2u);
        UNIT_ASSERT_VALUES_EQUAL(data.Pages.size(), 4u);
        TTestEnv raw;
        auto index = CreateIndexIter(sparse.Part.Get(), &raw, {});
        UNIT_ASSERT_VALUES_EQUAL(int(index->Seek(TRowId(1))), int(EReady::Data));
        UNIT_ASSERT(data.Pages.contains({sparse.Part.Get(), 0, index->GetPageId()}));
        ui32 sparsePages = 0;
        for (const auto& page : data.Pages) {
            sparsePages += std::get<0>(page) == sparse.Part.Get();
        }
        UNIT_ASSERT_VALUES_EQUAL(sparsePages, 1u);

        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(from.GetCells(), true)), int(EReady::Data));
        TSplitRequest request;
        request.EndKey = end;
        request.MaxExpectedBytes = data.Bytes;
        request.MaxIndexPages = Max<ui64>();
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT(!result.Truncated);
        UNIT_ASSERT(result.Keys.empty());
        UNIT_ASSERT_VALUES_EQUAL(iter.Telemetry().OwnerMainGroupBytes + iter.Telemetry().OtherMainGroupBytes, data.Bytes);
    }

    Y_UNIT_TEST(SplitPointsSingleUnitExempt) {
        const auto scheme = Scheme64();
        // Cover a whole one-page table and a unit inside a multilevel index.
        for (ui64 rows : {ui64(1), ui64(1024)}) {
            auto view = CookRows(scheme, SmallBTreeConf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), rows);
            view.Slices = TSlices::All();
            TSubset subset(TEpoch::FromIndex(1), scheme);
            subset.Flatten.push_back(view);
            TTrackingIndexEnv env;
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
            TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
            const auto from = rows == 1 ? TSerializedCellVec() : Key64(700);
            UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(from.GetCells(), true)), int(EReady::Data));
            if (rows == 1) {
                UNIT_ASSERT(!iter.Get().Bounds.FirstKey);
                UNIT_ASSERT(!iter.Get().Bounds.LastKey);
            }
            TSplitRequest request;
            request.EndKey = iter.Get().Bounds.LastKey;
            request.EndInclusive = iter.Get().Bounds.LastInclusive;
            request.MaxExpectedBytes = 0;
            env.Pages.clear();
            for (ui64 limit : {ui64(0), ui64(1), Max<ui64>()}) {
                request.MaxIndexPages = limit;
                TSplitResult result;
                UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
                UNIT_ASSERT_C(!result.Truncated, "single-unit range has no valid split boundary");
                UNIT_ASSERT(result.Keys.empty());
                UNIT_ASSERT(env.Pages.empty());
            }
        }
    }

    Y_UNIT_TEST(SplitPointsIgnoreSecondaryGroupIndexes) {
        const auto scheme = Scheme64(true);
        auto conf = SmallBTreeConf(4, true);
        conf.Group(1).PageRows = 1;
        auto view = CookRows(scheme, conf, TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 80, true);
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);

        TTrackingIndexEnv mainEnv;
        auto index = CreateIndexIter(view.Part.Get(), &mainEnv, {});
        EReady ready = index->Seek(TRowId(0));
        while (ready == EReady::Data) {
            ready = index->Next();
        }
        UNIT_ASSERT_VALUES_EQUAL(int(ready), int(EReady::Gone));
        UNIT_ASSERT(!mainEnv.Pages.empty());

        TTrackingIndexEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek({}, true)), int(EReady::Data));
        env.Pages.clear();
        TSplitRequest request;
        request.MaxExpectedBytes = Max<ui64>();
        request.MaxIndexPages = mainEnv.Pages.size();
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT(!result.Truncated);
        UNIT_ASSERT(result.Keys.empty());
        UNIT_ASSERT(env.Pages.size() <= request.MaxIndexPages);
        for (const auto& page : env.Pages) {
            UNIT_ASSERT_VALUES_EQUAL(std::get<1>(page), 0u);
        }
    }

    Y_UNIT_TEST(SplitPointsDisjointParts) {
        const auto scheme = Scheme64();
        TSubset all(TEpoch::FromIndex(1), scheme);
        TSubset relevant(TEpoch::FromIndex(1), scheme);
        std::set<const TPart*> expectedParts;
        std::set<const TPart*> allowedParts;
        for (ui32 part = 0; part < 24; ++part) {
            const ui64 first = 10 * part;
            auto view = CookKeys(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, part + 1), TEpoch::FromIndex(part + 1),
                {first, first + 1, first + 2, first + 3});
            all.Flatten.push_back(view);
            if (8 <= part && part <= 13) {
                relevant.Flatten.push_back(view);
                allowedParts.insert(view.Part.Get());
                if (part <= 12) {
                    expectedParts.insert(view.Part.Get());
                }
            }
        }
        std::reverse(all.Flatten.begin(), all.Flatten.end());
        const auto start = Key64(82);
        TSplitRequest request;
        request.EndKey = Key64(123);
        request.MaxExpectedBytes = 1;
        request.MaxIndexPages = Max<ui64>();
        auto split = [&](const TSubset& subset) {
            TTrackingIndexEnv env;
            const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
            TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
            UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(start.GetCells(), true)), int(EReady::Data));
            TSplitResult result;
            UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
            UNIT_ASSERT(!result.Truncated);
            UNIT_ASSERT(!result.Keys.empty());
            std::set<const TPart*> touched;
            for (const auto& page : env.Pages) {
                touched.insert(std::get<0>(page));
                UNIT_ASSERT(allowedParts.contains(std::get<0>(page)));
            }
            for (const auto* part : expectedParts) {
                UNIT_ASSERT(touched.contains(part));
            }
            return result;
        };
        AssertSameSplits(split(relevant), split(all));
    }

    Y_UNIT_TEST(SplitPointsFit) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(2), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 8);
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek({}, true)), int(EReady::Data));
        TSplitRequest request;
        request.Rate = 1.0;
        request.MaxExpectedBytes = ui64(1) << 40;
        request.MaxIndexPages = ui64(1) << 20;
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT(!result.Truncated);
        UNIT_ASSERT(result.Keys.empty());
        UNIT_ASSERT(iter.IsValid());
        const TString stayed = iter.Get().SelectionKey;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Next()), int(EReady::Data));
        UNIT_ASSERT(iter.Get().SelectionKey != stayed);
    }

    Y_UNIT_TEST(SplitPointsByteCuts) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 6);
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator scan(subset, &env, scheme->Keys, Cfg(), layout);
        const auto units = Collect(scan);
        UNIT_ASSERT(units.size() >= 3);

        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek({}, true)), int(EReady::Data));
        TSplitRequest request;
        request.Rate = 1.0;
        request.MaxExpectedBytes = 1;
        request.MaxIndexPages = ui64(1) << 20;
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT(!result.Truncated);
        UNIT_ASSERT_VALUES_EQUAL(result.Keys.size(), units.size() - 1);
        for (size_t i = 0; i < result.Keys.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(result.Keys[i].Key.GetBuffer(), units[i + 1].Bounds.FirstKey.GetBuffer());
            UNIT_ASSERT_VALUES_EQUAL(int(result.Keys[i].Side), int(EBoundarySide::Before));
            if (i > 0) {
                UNIT_ASSERT(ComparePartKeys(result.Keys[i - 1].Key.GetCells(), result.Keys[i].Key.GetCells(), *scheme->Keys) < 0);
            }
        }
    }

    Y_UNIT_TEST(SplitPointsIndexBudgetTail) {
        auto scheme = Scheme64();
        auto view = CookRows(scheme, SmallBTreeConf(4), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 80);
        view.Slices = TSlices::All();
        UNIT_ASSERT(view->IndexPages.GetBTree({}).LevelCount > 1);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        const auto target = Key64(5);

        for (ui64 budget : {ui64(0), ui64(1), ui64(2), ui64(4), ui64(8), ui64(16), ui64(32)}) {
            TIndexOnlyEnv env;
            TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
            UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(target.GetCells(), true)), int(EReady::Data));
            const auto unit = iter.Get();
            UNIT_ASSERT(ComparePartKeys(unit.Bounds.FirstKey.GetCells(), target.GetCells(), *scheme->Keys) < 0);
            UNIT_ASSERT(ComparePartKeys(target.GetCells(), unit.Bounds.LastKey.GetCells(), *scheme->Keys) < 0);
            TSplitRequest request;
            request.MaxExpectedBytes = Max<ui64>();
            request.MaxIndexPages = budget;
            TSplitResult result;
            UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
            UNIT_ASSERT_VALUES_EQUAL(iter.Get().SelectionKey, unit.SelectionKey);
            UNIT_ASSERT(SameBounds(iter.Get().Bounds, unit.Bounds));
            if (budget <= 1) {
                // The first unit cannot fit the index budget, but cannot be divided.
                UNIT_ASSERT(result.Truncated);
                UNIT_ASSERT_VALUES_EQUAL(result.Keys.size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(result.Keys.front().Key.GetBuffer(), unit.Bounds.LastKey.GetBuffer());
                UNIT_ASSERT_VALUES_EQUAL(int(result.Keys.front().Side), int(EBoundarySide::Before));
                auto prefix = request;
                prefix.EndKey = unit.Bounds.LastKey;
                prefix.EndInclusive = unit.Bounds.LastInclusive;
                TSplitResult exempt;
                UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(prefix, exempt)), int(EReady::Data));
                UNIT_ASSERT(!exempt.Truncated);
                UNIT_ASSERT(exempt.Keys.empty());
                UNIT_ASSERT_VALUES_EQUAL(iter.Get().SelectionKey, unit.SelectionKey);
            }
            if (!result.Truncated) {
                continue;
            }
            UNIT_ASSERT(!result.Keys.empty());
            const auto& firstCut = result.Keys.back();
            const bool inclusive = firstCut.Side == EBoundarySide::Before;
            TIndexOnlyEnv env2;
            TKeyBlockIterator again(subset, &env2, scheme->Keys, Cfg(), layout);
            UNIT_ASSERT_VALUES_EQUAL(int(again.Seek(firstCut.Key.GetCells(), inclusive)), int(EReady::Data));
            TSplitResult tail;
            UNIT_ASSERT_VALUES_EQUAL(int(again.SplitPoints(request, tail)), int(EReady::Data));
            if (!tail.Keys.empty()) {
                UNIT_ASSERT(ComparePartKeys(firstCut.Key.GetCells(), tail.Keys.front().Key.GetCells(), *scheme->Keys) < 0);
            } else {
                UNIT_ASSERT(!tail.Truncated);
            }
        }
    }

    Y_UNIT_TEST(SplitPointsBudgetLimitsActualPageFetches) {
        auto scheme = Scheme64();
        const auto conf = SmallBTreeConf(1);
        auto view = CookRows(scheme, conf, TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 1024);
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TTrackingIndexEnv env;
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto target = Key64(700);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(target.GetCells(), true)), int(EReady::Data));
        const TString selection = iter.Get().SelectionKey;
        env.Pages.clear();
        TSplitRequest request;
        request.MaxExpectedBytes = Max<ui64>();
        request.MaxIndexPages = 4 * (view->IndexPages.GetBTree({}).LevelCount + 1);
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT_C(env.Pages.size() <= request.MaxIndexPages,
            "split budget " << request.MaxIndexPages << " allowed " << env.Pages.size() << " index page fetches");
        UNIT_ASSERT(result.Truncated);
        UNIT_ASSERT(!result.Keys.empty());
        UNIT_ASSERT(ComparePartKeys(target.GetCells(), result.Keys.back().Key.GetCells(), *scheme->Keys) < 0);
        UNIT_ASSERT_VALUES_EQUAL(iter.Get().SelectionKey, selection);
    }

    Y_UNIT_TEST(SplitPointsIgnoreRemovedRowsAfterEarlierRange) {
        auto scheme = Scheme64();
        auto owner = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 30);
        auto removed = CookRows(scheme, Conf(1), TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 20);
        owner.Slices = TSlices::All();
        ReplaceSlice(removed, 0, true, 1, true);
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten = {owner, removed};
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TIndexOnlyEnv env;
        TKeyBlockIterator earlier(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(earlier.Seek({}, true)), int(EReady::Data));
        TSplitRequest warmRequest;
        warmRequest.EndKey = Key64(2);
        warmRequest.MaxExpectedBytes = Max<ui64>();
        warmRequest.MaxIndexPages = Max<ui64>();
        TSplitResult ignored;
        UNIT_ASSERT_VALUES_EQUAL(int(earlier.SplitPoints(warmRequest, ignored)), int(EReady::Data));

        TSplitRequest request;
        request.MaxExpectedBytes = IndexTools::CountDataSize(*owner.Part, {});
        request.MaxIndexPages = Max<ui64>();
        const auto target = Key64(5);
        TKeyBlockIterator fresh(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(fresh.Seek(target.GetCells(), true)), int(EReady::Data));
        UNIT_ASSERT_VALUES_EQUAL(int(earlier.Seek(target.GetCells(), true)), int(EReady::Data));
        TSplitResult freshResult;
        TSplitResult earlierResult;
        UNIT_ASSERT_VALUES_EQUAL(int(fresh.SplitPoints(request, freshResult)), int(EReady::Data));
        UNIT_ASSERT_VALUES_EQUAL(int(earlier.SplitPoints(request, earlierResult)), int(EReady::Data));
        UNIT_ASSERT(!freshResult.Truncated);
        UNIT_ASSERT(freshResult.Keys.empty());
        AssertSameSplits(freshResult, earlierResult);
    }

    Y_UNIT_TEST(SplitPointsCertain) {
        const auto scheme = Scheme64();
        auto view = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 4);
        view.Slices = TSlices::All();
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(view);
        TIndexOnlyEnv env;
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek({}, true)), int(EReady::Data));
        const TBounds certain = iter.Get().Bounds;
        const TString selection = iter.Get().SelectionKey;
        TTestEnv raw;
        auto index = CreateIndexIter(view.Part.Get(), &raw, {});
        UNIT_ASSERT_VALUES_EQUAL(int(index->Seek(0)), int(EReady::Data));
        const ui64 firstBytes = view->GetPageSize(index->GetPageId(), {});
        UNIT_ASSERT_VALUES_EQUAL(int(index->Next()), int(EReady::Data));
        const ui64 nextBytes = view->GetPageSize(index->GetPageId(), {});
        // The second budget only overflows when the successor is certain too.
        for (ui64 budget : {firstBytes / 2, firstBytes + nextBytes / 2}) {
            TSplitRequest request;
            request.Rate = 0.001;
            request.MaxExpectedBytes = budget;
            request.MaxIndexPages = Max<ui64>();
            TSplitResult result;
            UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
            UNIT_ASSERT(!result.Truncated);
            UNIT_ASSERT(result.Keys.empty());
            request.Certain = &certain;
            UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
            UNIT_ASSERT(!result.Truncated);
            UNIT_ASSERT(!result.Keys.empty());
            UNIT_ASSERT_VALUES_EQUAL(result.Keys.front().Key.GetBuffer(), certain.LastKey.GetBuffer());
            UNIT_ASSERT_VALUES_EQUAL(int(result.Keys.front().Side), int(EBoundarySide::Before));
            UNIT_ASSERT_VALUES_EQUAL(iter.Get().SelectionKey, selection);
        }
    }

    Y_UNIT_TEST(SplitPointsBudgetAcrossOwnerChanges) {
        auto scheme = Scheme64();
        const auto conf = SmallBTreeConf(1);
        auto continuous = CookRows(scheme, conf, TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 512);
        auto singletons = CookRows(scheme, conf, TLogoBlobID(2, 2, 3, 1, 0, 2), TEpoch::FromIndex(2), 513);
        continuous.Slices = TSlices::All();
        TVector<TSlice> slices;
        for (ui64 key = 32; key <= 480; key += 2) {
            slices.emplace_back(Key64(key), Key64(key), TRowId(key), TRowId(key), true, true);
        }
        ReplaceSlices(singletons, std::move(slices));
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten = {continuous, singletons};
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(), scheme->Keys);
        UNIT_ASSERT(layout.Regions.size() > 400);
        TTrackingIndexEnv env;
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(), layout);
        const auto target = Key64(32);
        UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(target.GetCells(), false)), int(EReady::Data));
        const TString selection = iter.Get().SelectionKey;
        env.Pages.clear();
        TSplitRequest request;
        request.EndKey = Key64(481);
        request.MaxExpectedBytes = Max<ui64>();
        request.MaxIndexPages = 4 * (Max(continuous->IndexPages.GetBTree({}).LevelCount,
            singletons->IndexPages.GetBTree({}).LevelCount) + 1);
        TSplitResult result;
        UNIT_ASSERT_VALUES_EQUAL(int(iter.SplitPoints(request, result)), int(EReady::Data));
        UNIT_ASSERT_C(env.Pages.size() <= request.MaxIndexPages,
            "owner changes fetched " << env.Pages.size() << " pages for budget " << request.MaxIndexPages);
        UNIT_ASSERT_C(result.Truncated, "owner changes must advance through new index pages");
        UNIT_ASSERT(!result.Keys.empty());
        UNIT_ASSERT(ComparePartKeys(target.GetCells(), result.Keys.back().Key.GetCells(), *scheme->Keys) < 0);
        UNIT_ASSERT(ComparePartKeys(result.Keys.back().Key.GetCells(), request.EndKey.GetCells(), *scheme->Keys) < 0);
        UNIT_ASSERT_VALUES_EQUAL(iter.Get().SelectionKey, selection);
    }

    Y_UNIT_TEST(MemtableAnchorsAcrossManyGaps) {
        auto scheme = Scheme64();
        auto part = CookRows(scheme, Conf(1), TLogoBlobID(1, 2, 3, 1, 0, 1), TEpoch::FromIndex(1), 40);
        TVector<TSlice> slices;
        for (ui64 key = 0; key < 40; key += 2) {
            slices.emplace_back(Key64(key), Key64(key), TRowId(key), TRowId(key), true, true);
        }
        ReplaceSlices(part, std::move(slices));
        TCooker older(scheme, TEpoch::FromIndex(2));
        TCooker newer(scheme, TEpoch::FromIndex(3));
        for (ui64 key = 0; key < 40; ++key) {
            older.Add(*TSchemedCookRow(*scheme).Col(key, ui32(1)));
            newer.Add(*TSchemedCookRow(*scheme).Col(key, ui32(2)));
        }
        TSubset subset(TEpoch::FromIndex(1), scheme);
        subset.Flatten.push_back(part);
        subset.Frozen.emplace_back(*older, (*older)->Snapshot());
        subset.Frozen.emplace_back(*newer, (*newer)->Snapshot());
        const auto layout = TKeyBlockIterator::BuildLayout(subset, Cfg(1), scheme->Keys);
        TIndexOnlyEnv env;
        TKeyBlockIterator iter(subset, &env, scheme->Keys, Cfg(1), layout);
        const auto units = Collect(iter);
        UNIT_ASSERT_VALUES_EQUAL(units.size(), 61u);
        UNIT_ASSERT_VALUES_EQUAL(iter.Telemetry().UnitsMemtable, 41u);
        AssertAbut(units, *scheme->Keys);
        THashSet<TString> selections;
        for (const auto& unit : units) {
            UNIT_ASSERT_C(selections.insert(unit.SelectionKey).second, "duplicate unit selection key");
        }
        for (ui64 key = 0; key < 40; ++key) {
            const auto target = Key64(key);
            UNIT_ASSERT_VALUES_EQUAL(int(iter.Seek(target.GetCells(), true)), int(EReady::Data));
            UNIT_ASSERT_VALUES_EQUAL(iter.Get().SelectionKey, Selection(0x01, target));
            UNIT_ASSERT_VALUES_EQUAL(iter.Get().FromMemtable, bool(key % 2));
            UNIT_ASSERT_VALUES_EQUAL(iter.Get().OwnerRows, key % 2 ? 0u : 1u);
        }
    }
}

}
}
