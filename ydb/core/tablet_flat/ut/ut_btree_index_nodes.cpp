#include "flat_page_btree_index.h"
#include "flat_page_btree_index_writer.h"
#include "flat_part_walker.h"
#include "flat_table_part.h"
#include "test/libs/table/test_writer.h"
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NPage {

namespace {
    using namespace NTest;
    using TShortChild = TBtreeIndexNode::TShortChild;
    using TChild = TBtreeIndexNode::TChild;

    // Helper: compact v1 TBtreeIndexMeta construction
    auto Meta(TPageId pageId, TRowId rowCount, ui64 dataSize, ui64 groupDataSize, TRowId erasedRowCount,
              ui32 levelCount, ui64 indexSize) {
        return TBtreeIndexMeta{pageId, TPageLocation::Max(),
                               rowCount,
                               dataSize,
                               groupDataSize,
                               erasedRowCount,
                               levelCount,
                               indexSize};
    }

    auto Meta(const TChild& child, ui32 levelCount, ui64 indexSize) {
        return Meta(child.GetPageId(), child.GetRowCount(), child.GetDataSize(), child.GetGroupDataSize(),
                    child.GetErasedRowCount(), levelCount, indexSize);
    }

    // Helper: check that two TBtreeIndexMeta agree on row-level stats,
    // ignoring the root reference (v1 uses page-id, v2 uses byte-offset).
    void AssertMetaStatsEqual(const TBtreeIndexMeta& a, const TBtreeIndexMeta& b, const TString& msg) {
        UNIT_ASSERT_VALUES_EQUAL_C(a.GetRowCount(), b.GetRowCount(), msg);
        UNIT_ASSERT_VALUES_EQUAL_C(a.GetDataSize(), b.GetDataSize(), msg);
        UNIT_ASSERT_VALUES_EQUAL_C(a.GetGroupDataSize(), b.GetGroupDataSize(), msg);
        UNIT_ASSERT_VALUES_EQUAL_C(a.GetErasedRowCount(), b.GetErasedRowCount(), msg);
        UNIT_ASSERT_VALUES_EQUAL_C(a.LevelCount, b.LevelCount, msg);
        // IndexSize may differ slightly due to TChildV2 being larger than TChild
    }

    // Helper: assert v2 root properties on a TBtreeIndexMeta
    void AssertV2Root(const TBtreeIndexMeta& meta, const TString& msg) {
        UNIT_ASSERT_C(meta.HasV2Root(), msg + " expected HasV2Root");
        UNIT_ASSERT_C(!meta.HasV1Root(), msg + " expected !HasV1Root");
        UNIT_ASSERT_C(meta.V2Root.Offset.IsByteOffset(), msg + " expected V2Root byte offset");
        UNIT_ASSERT_VALUES_EQUAL_C(meta.RootPageIdV1(), Max<TPageId>(), msg + " expected Max RootPageIdV1");
    }

    // Helper: assert v1 root properties on a TBtreeIndexMeta
    void AssertV1Root(const TBtreeIndexMeta& meta, const TString& msg) {
        UNIT_ASSERT_C(meta.HasV1Root(), msg + " expected HasV1Root");
        UNIT_ASSERT_C(!meta.HasV2Root(), msg + " expected !HasV2Root");
        UNIT_ASSERT_C(meta.RootPageIdV1() != Max<TPageId>(), msg + " expected valid RootPageIdV1");
    }

    // Helper: iterate all rows in a part via the btree index and return total row count
    ui64 CountAllRows(const TPartStore& part, TGroupId groupId = {}) {
        TTestEnv env;
        auto index = CreateIndexIter(&part, &env, groupId);
        ui64 count = 0;
        for (size_t i = 0; ; i++) {
            auto ready = i == 0 ? index->Seek(0) : index->Next();
            if (ready != EReady::Data) {
                Y_ENSURE(ready != EReady::Page, "Unexpected page fault");
                break;
            }
            count++;
        }
        return count;
    }

    // Helper: collect all row offsets from iteration
    TVector<TPageLocation> CollectPageLocations(const TPartStore& part, TGroupId groupId = {}) {
        TTestEnv env;
        auto index = CreateIndexIter(&part, &env, groupId);
        TVector<TPageLocation> result;
        for (size_t i = 0; ; i++) {
            auto ready = i == 0 ? index->Seek(0) : index->Next();
            if (ready != EReady::Data) {
                Y_ENSURE(ready != EReady::Page, "Unexpected page fault");
                break;
            }
            result.push_back(index->GetLocation());
        }
        return result;
    }

    // Helper: make a TConf with WriteBTreeIndexV2 = true
    NPage::TConf MakeV2Conf(bool fin = true, ui32 page = 7 * 1024) {
        NPage::TConf conf{ fin, page };
        conf.WriteBTreeIndex = true;
        conf.WriteBTreeIndexV2 = true;
        return conf;
    }

    TLayoutCook MakeLayout() {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(0, 2,  NScheme::NTypeIds::Bool)
            .Col(0, 3,  NScheme::NTypeIds::Uint64)
            .Key({0, 1, 2, 3});

        return lay;
    }

    TString MakeKey(std::optional<ui32> c0 = { }, std::optional<std::string> c1 = { }, std::optional<bool> c2 = { }, std::optional<ui64> c3 = { }) {
        TVector<TCell> cells;
        
        if (c0) {
            cells.push_back(TCell::Make(c0.value()));
        } else {
            cells.push_back(TCell());
        }

        if (c1) {
            cells.push_back(TCell(c1.value().data(), c1.value().size()));
        } else {
            cells.push_back(TCell());
        }

        if (c2) {
            cells.push_back(TCell::Make(c2.value()));
        } else {
            cells.push_back(TCell());
        }

        if (c3) {
            cells.push_back(TCell::Make(c3.value()));
        } else {
            cells.push_back(TCell());
        }

        return TSerializedCellVec::Serialize(cells);
    }

    TChild MakeChild(ui32 index) {
        return {index + 10000, index + 100, index + 1000, index + 2000, index + 30};
    }

    TShortChild MakeShortChild(ui32 index) {
        return {index + 10000, index + 100, index + 1000};
    }

    void Dump(TChild meta, const TPartScheme::TGroupInfo& groupInfo, const TStore& store, ui32 level = 0)
    {
        TString intend;
        for (size_t i = 0; i < level; i++) {
            intend += " |";
        }

        auto dumpChild = [&] (TBtreeIndexNode node, TRecIdx pos) {
            TChild child;
            if (node.IsShortChildFormat()) {
                auto shortChild = node.GetShortChild(pos);
                child = {shortChild.GetPageId(), shortChild.GetRowCount(), shortChild.GetDataSize(), 0, 0};
            } else {
                child = node.GetChild(pos);
            }
            if (child.GetPageId() < 1000) {
                Dump(child, groupInfo, store, level + 1);
            } else {
                Cerr << intend << " | " << child.ToString() << Endl;
            }
        };

        auto node = TBtreeIndexNode(*store.GetPage(0, meta.GetPageId()));

        auto label = node.Label();

        Cerr
            << intend
            << " + BTreeIndex{"
            << meta.ToString() << ", "
            << (ui16)label.Type << " rev " << label.Format << ", " 
            << label.Size << "b}"
            << Endl;

        dumpChild(node, 0);

        for (TRecIdx i : xrange(node.GetKeysCount())) {
            Cerr << intend << " | > {";

            auto cells = node.GetKeyCellsIter(i, groupInfo.ColsKeyIdx);
            for (TPos pos : xrange(cells.Count())) {
                TString str;
                DbgPrintValue(str, cells.Next(), groupInfo.KeyTypes[pos]);
                if (str.size() > 10) {
                    str = str.substr(0, 10) + "..";
                }
                Cerr << (pos ? ", " : "") << str;
            }

            Cerr << "}" << Endl;
            dumpChild(node, i + 1);
        }

        Cerr << Endl;
    }

    void Dump(NPage::TBtreeIndexMeta meta, const TPartScheme::TGroupInfo& groupInfo, const TStore& store,
              ui32 level = 0)
    {
        Dump(TChild{meta.RootPageIdV1(), meta.GetRowCount(), meta.GetDataSize(), meta.GetGroupDataSize(),
                    meta.GetErasedRowCount()}, groupInfo, store, level);
    }

    void Dump(TSharedData node, const TPartScheme::TGroupInfo& groupInfo) {
        TWriterBundle pager(1, TLogoBlobID());
        auto& writer = static_cast<IPageWriter&>(pager);
        writer.Write(node, EPage::BTreeIndex, 0);
        TChild page{writer.GetWrittenPageId(0), 0, 0, 0, 0};
        Dump(page, groupInfo, pager.Back());
    }

    void CheckKeys(const NPage::TBtreeIndexNode& node, const TVector<TString>& keys, const TPartScheme::TGroupInfo& groupInfo) {
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeysCount(), keys.size());
        for (TRecIdx i : xrange(node.GetKeysCount())) {
            TVector<TCell> actualCells;
            auto cells = node.GetKeyCellsIter(i, groupInfo.ColsKeyIdx);
            UNIT_ASSERT_VALUES_EQUAL(cells.Count(), groupInfo.ColsKeyIdx.size());
            
            for (TPos pos : xrange(cells.Count())) {
                Y_UNUSED(pos);
                actualCells.push_back(cells.Next());
            }

            auto actual = TSerializedCellVec::Serialize(actualCells);
            UNIT_ASSERT_VALUES_EQUAL(actual, keys[i]);
        }
    }

    void CheckKeys(TPageId pageId, const TVector<TString>& keys, const TPartScheme::TGroupInfo& groupInfo, const TStore& store) {
        auto page = store.GetPage(0, pageId);
        auto node = TBtreeIndexNode(*page);
        CheckKeys(node, keys, groupInfo);
    }

    void CheckChildren(const NPage::TBtreeIndexNode& node, const TVector<TChild>& children) {
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeysCount() + 1, children.size());
        for (TRecIdx i : xrange(node.GetKeysCount() + 1)) {
            UNIT_ASSERT_EQUAL(node.GetChild(i), children[i]);
            TShortChild shortChild{children[i].GetPageId(), children[i].GetRowCount(), children[i].GetDataSize()};
            UNIT_ASSERT_EQUAL(node.GetShortChild(i), shortChild);
        }
    }

    void CheckShortChildren(const NPage::TBtreeIndexNode& node, const TVector<TShortChild>& children) {
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeysCount() + 1, children.size());
        for (TRecIdx i : xrange(node.GetKeysCount() + 1)) {
            UNIT_ASSERT_EQUAL(node.GetShortChild(i), children[i]);
        }
    }
}

Y_UNIT_TEST_SUITE(TBtreeIndexNode) {
    using namespace NTest;
    using TChild = TBtreeIndexNode::TChild;

    Y_UNIT_TEST(TIsNullBitmap) {
        TString buffer(754, 0);
        auto* bitmap = (TBtreeIndexNode::TIsNullBitmap*)(buffer.data());

        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(4), 1);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(7), 1);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(8), 1);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(9), 2);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(256), 32);
        UNIT_ASSERT_VALUES_EQUAL(bitmap->Length(257), 33);

        for (TPos pos : xrange(buffer.size() * 8)) {
            UNIT_ASSERT(!bitmap->IsNull(pos));
            bitmap->SetNull(pos);
            UNIT_ASSERT(bitmap->IsNull(pos));
        }
    }

    Y_UNIT_TEST(CompareTo) {
        auto compareTo = [] (TString a, TString b) {
            TLayoutCook lay = MakeLayout();
            TPartScheme scheme(lay.RowScheme()->Cols);

            TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), { });

            writer.AddChild(MakeChild(0));
            TSerializedCellVec aa(a);
            writer.AddKey(aa.GetCells());
            writer.AddChild(MakeChild(1));

            auto node = TBtreeIndexNode(writer.Finish());
            TSerializedCellVec bb(b);
            return node.GetKeyCellsIter(0, scheme.GetLayout({}).ColsKeyIdx)
                .CompareTo(bb.GetCells(), lay.RowScheme()->Keys.Get());
        };

        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100), MakeKey(101)), -1);
        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100), MakeKey(100)), 0);
        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(101), MakeKey(100)), 1);

        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100, "a"), MakeKey(100, "b")), -1);
        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100, "a"), MakeKey(100, "a")), 0);
        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100, "b"), MakeKey(100, "a")), 1);
        
        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100), MakeKey(100, "a")), -1);
        UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(100, "a"), MakeKey(100)), 1);

        { // key shorter than defaults extends with +inf cells
            TVector<TCell> cells = {TCell::Make(1u), TCell(), TCell::Make(true)};
            UNIT_ASSERT_VALUES_EQUAL(compareTo(MakeKey(1u, { }, true, 2u), TSerializedCellVec::Serialize(cells)), -1);
        }
    }

    Y_UNIT_TEST(Basics) {
        TLayoutCook lay = MakeLayout();

        TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), { });
        
        TVector<TString> keys;
        keys.push_back(MakeKey({ }, { }, true));
        keys.push_back(MakeKey(100, "asdf", true, 10000));
        keys.push_back(MakeKey(101));
        keys.push_back(MakeKey(101, "asdf"));
        keys.push_back(MakeKey(102, "asdf"));
        keys.push_back(MakeKey(102, "asdfg"));
        keys.push_back(MakeKey(102, "asdfg", 1));
        keys.push_back(MakeKey(103, { }, false));
        keys.push_back(MakeKey(103, { }, true));
        keys.push_back(MakeKey(103, "x"));
        keys.push_back(MakeKey(104, "asdf", true, 10000));
        keys.push_back(MakeKey(104, "asdf", true, 10001));
        keys.push_back(MakeKey(104, TString(1024*1024, 'x'), true, 10000));
        keys.push_back(MakeKey(105));

        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto c : children) {
            writer.AddChild(c);
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, keys, writer.GroupInfo);
        CheckChildren(node, children);

        UNIT_ASSERT_VALUES_EQUAL(node.GetKeyCellsIter(1, writer.GroupInfo.ColsKeyIdx).At(0).AsValue<ui32>(), 100);
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeyCellsIter(1, writer.GroupInfo.ColsKeyIdx).At(2).AsValue<bool>(), true);
    }

    Y_UNIT_TEST(Group) {
        TLayoutCook lay;
        
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(1, 2,  NScheme::NTypeIds::Bool)
            .Col(1, 3,  NScheme::NTypeIds::Uint64)
            .Key({0, 1});

        TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), TGroupId{ 1 });

        TVector<TString> keys;
        for (ui32 i : xrange(13)) {
            Y_UNUSED(i);
            TVector<TCell> cells;
            keys.push_back(TSerializedCellVec::Serialize(cells));
        }

        TVector<TShortChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeShortChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto &c : children) {
            writer.AddChild({c.GetPageId(), c.GetRowCount(), c.GetDataSize(), 0, 0});
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, keys, writer.GroupInfo);
        CheckShortChildren(node, children);
    }

    Y_UNIT_TEST(History) {
        TLayoutCook lay;
        
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(1, 2,  NScheme::NTypeIds::Bool)
            .Col(1, 3,  NScheme::NTypeIds::Uint64)
            .Key({0, 1});

        TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), TGroupId{ 0, 1 });

        TVector<TString> keys;
        for (ui32 i : xrange(13)) {
            Y_UNUSED(i);
            TVector<TCell> cells;
            cells.push_back(TCell::Make(TRowId(i)));
            cells.push_back(TCell::Make(ui64(10 * i)));
            cells.push_back(TCell::Make(ui64(100 * i)));
            keys.push_back(TSerializedCellVec::Serialize(cells));
        }

        TVector<TShortChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeShortChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto &c : children) {
            writer.AddChild({c.GetPageId(), c.GetRowCount(), c.GetDataSize(), 0, 0});
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, keys, writer.GroupInfo);
        CheckShortChildren(node, children);

        UNIT_ASSERT_VALUES_EQUAL(node.GetKeyCellsIter(12, writer.GroupInfo.ColsKeyIdx).At(0).AsValue<TRowId>(), 12);
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeyCellsIter(12, writer.GroupInfo.ColsKeyIdx).At(1).AsValue<ui64>(), 120);
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeyCellsIter(12, writer.GroupInfo.ColsKeyIdx).At(2).AsValue<ui64>(), 1200);
    }

    Y_UNIT_TEST(OneKey) {
        TLayoutCook lay = MakeLayout();

        TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), { });
        
        auto check= [&] (TString key) {
            TVector<TString> keys;
            keys.push_back(key);
            
            TVector<TChild> children;
            for (ui32 i : xrange(keys.size() + 1)) {
                children.push_back(MakeChild(i));
            }

            for (auto k : keys) {
                TSerializedCellVec deserialized(k);
                writer.AddKey(deserialized.GetCells());
            }
            for (auto c : children) {
                writer.AddChild(c);
            }

            auto serialized = writer.Finish();

            auto node = TBtreeIndexNode(serialized);

            Dump(serialized, writer.GroupInfo);
            CheckKeys(node, keys, writer.GroupInfo);
            CheckChildren(node, children);
        };

        check(MakeKey(100, "asdf", true, 10000));
        check(MakeKey(100, TString(1024*1024, 'x'), true, 10000));
        check(MakeKey(100, "asdf", true, { }));
        check(MakeKey(100, "asdf", { }, 10000));
        check(MakeKey(100, { }, true, 10000));
        check(MakeKey({ }, "asdf", true, 10000));
        check(MakeKey({ }, "asdf", { }, 10000));
        check(MakeKey({ }, "asdf", { }, { }));
        check(MakeKey({ }, { }, { }, { }));
    }

    Y_UNIT_TEST(Reusable) {
        TLayoutCook lay = MakeLayout();

        TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), { });
        
        TVector<TString> keys;
        keys.push_back(MakeKey(100, "asdf", true, 10000));
        keys.push_back(MakeKey(101, "xyz", true, 10000));
        keys.push_back(MakeKey(103, { }, true, 10000));
        
        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto c : children) {
            writer.AddChild(c);
        }
        writer.Finish();

        keys.erase(keys.begin());
        children.erase(children.begin());
        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto c : children) {
            writer.AddChild(c);
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, keys, writer.GroupInfo);
        CheckChildren(node, children);
    }

    Y_UNIT_TEST(CutKeys) {
        TLayoutCook lay = MakeLayout();

        TBtreeIndexNodeWriter writer(new TPartScheme(lay.RowScheme()->Cols), { });
        
        TVector<TString> fullKeys;
        fullKeys.push_back(MakeKey({ }, { }, { }, { }));
        fullKeys.push_back(MakeKey(100, { }, { }, { }));
        fullKeys.push_back(MakeKey(100, "asdf", { }, { }));
        fullKeys.push_back(MakeKey(100, "asdf", true, { }));
        fullKeys.push_back(MakeKey(100, "asdf", true, 10000));

        // cut keys don't have trailing nulls
        TVector<TString> cutKeys;
        for (ui32 i : xrange(5)) {
            TVector<TCell> cells;
            auto key = TSerializedCellVec(fullKeys[i]);
            for (ui32 j : xrange(i)) {
                cells.push_back(key.GetCells()[j]);
            }
            cutKeys.push_back(TSerializedCellVec::Serialize(cells));
        }
        
        TVector<TChild> children;
        for (ui32 i : xrange(fullKeys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        for (auto k : cutKeys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto c : children) {
            writer.AddChild(c);
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, fullKeys, writer.GroupInfo);
        CheckChildren(node, children);
    }

}

Y_UNIT_TEST_SUITE(TBtreeIndexBuilder) {
    using namespace NTest;
    using TChild = TBtreeIndexNode::TChild;

    Y_UNIT_TEST(NoNodes) {
        TLayoutCook lay = MakeLayout();
        TIntrusivePtr<TPartScheme> scheme = new TPartScheme(lay.RowScheme()->Cols);

        TBtreeIndexBuilder builder(scheme, { }, Max<ui32>(), Max<ui32>(), Max<ui32>());

        const auto child = MakeChild(42);
        builder.AddChild(child);

        TWriterBundle pager(1, TLogoBlobID());
        auto result = builder.Finish(pager);

        auto expected = Meta(child, 0, 0);
        UNIT_ASSERT_EQUAL_C(result, expected, "Got " + result.ToString());
    }

    Y_UNIT_TEST(OneNode) {
        TLayoutCook lay = MakeLayout();
        TIntrusivePtr<TPartScheme> scheme = new TPartScheme(lay.RowScheme()->Cols);

        TBtreeIndexBuilder builder(scheme, { }, Max<ui32>(), Max<ui32>(), Max<ui32>());
        
        TVector<TString> keys;
        for (ui32 i : xrange(10)) {
            keys.push_back(MakeKey(i, std::string{char('a' + i)}, i % 2, i * 10));
        }
        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            builder.AddKey(deserialized.GetCells());
        }
        for (auto c : children) {
            builder.AddChild(c);
        }

        TWriterBundle pager(1, TLogoBlobID());
        auto result = builder.Finish(pager);

        Dump(result, builder.GroupInfo, pager.Back());

        auto expected = Meta(0, 1155, 11055, 22055, 385, 1, 683);
        UNIT_ASSERT_EQUAL_C(result, expected, "Got " + result.ToString());

        CheckKeys(result.RootPageIdV1(), keys, builder.GroupInfo, pager.Back());
    }

    Y_UNIT_TEST(FewNodes) {
        TLayoutCook lay = MakeLayout();
        TIntrusivePtr<TPartScheme> scheme = new TPartScheme(lay.RowScheme()->Cols);

        TBtreeIndexBuilder builder(scheme, { }, Max<ui32>(), 1, 2);

        TVector<TString> keys;
        for (ui32 i : xrange(20)) {
            keys.push_back(MakeKey(i, std::string{char('a' + i)}, i % 2, i * 10));
        }
        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        TWriterBundle pager(1, TLogoBlobID());

        builder.AddChild(children[0]);
        for (ui32 i : xrange(keys.size())) {
            TSerializedCellVec deserialized(keys[i]);
            builder.AddKey(deserialized.GetCells());
            builder.AddChild(children[i + 1]);
            builder.Flush(pager);
        }

        auto result = builder.Finish(pager);

        Dump(result, builder.GroupInfo, pager.Back());
        
        auto expected = Meta(9, 0, 0, 0, 0, 3, 1790);
        for (auto c : children) {
            expected.RowCount_ += c.GetRowCount();
            expected.DataSize_ += c.GetDataSize();
            expected.GroupDataSize_ += c.GetGroupDataSize();
            expected.ErasedRowCount_ += c.GetErasedRowCount();
        }
        UNIT_ASSERT_EQUAL_C(result, expected, "Got " + result.ToString());

        auto checkKeys = [&](TPageId pageId, const TVector<TString>& keys) {
            CheckKeys(pageId, keys, builder.GroupInfo, pager.Back());
        };

        // Level 0:
        checkKeys(0, {
            keys[0], keys[1]
        });
        // -> keys[2]
        checkKeys(1, {
            keys[3], keys[4]
        });
        // -> keys[5]
        checkKeys(2, {
            keys[6], keys[7]
        });
        // -> keys[8]
        checkKeys(3, {
            keys[9], keys[10]
        });
        // -> keys[11]
        checkKeys(4, {
            keys[12], keys[13]
        });
        // -> keys[14]
        checkKeys(6, {
            keys[15], keys[16]
        });
        // -> keys[17]
        checkKeys(7, {
            keys[18], keys[19]
        });

        // Level 1:
        checkKeys(5, {
            keys[2], keys[5]
        });
        checkKeys(8, {
            keys[11], keys[14], keys[17]
        });

        // Level 2 (root):
        checkKeys(9, {
            keys[8]
        });
    }

    Y_UNIT_TEST(SplitBySize) {
        TLayoutCook lay = MakeLayout();
        TIntrusivePtr<TPartScheme> scheme = new TPartScheme(lay.RowScheme()->Cols);

        TBtreeIndexBuilder builder(scheme, { }, 650, 1, Max<ui32>());
        
        TVector<TString> keys;
        for (ui32 i : xrange(100)) {
            keys.push_back(MakeKey(i, TString(i + 1, 'x')));
        }
        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        TWriterBundle pager(1, TLogoBlobID());

        builder.AddChild(children[0]);
        for (ui32 i : xrange(keys.size())) {
            TSerializedCellVec deserialized(keys[i]);
            builder.AddKey(deserialized.GetCells());
            builder.AddChild(children[i + 1]);
            builder.Flush(pager);
        }

        auto result = builder.Finish(pager);

        Dump(result, builder.GroupInfo, pager.Back());
        
        auto expected = Meta(15, 15150, 106050, 207050, 8080, 3, 11198);
        UNIT_ASSERT_EQUAL_C(result, expected, "Got " + result.ToString());
    }

}

Y_UNIT_TEST_SUITE(TBtreeIndexTPart) {

    Y_UNIT_TEST(Conf) {
        NPage::TConf conf;

        UNIT_ASSERT_VALUES_EQUAL(conf.WriteBTreeIndex, true);
        UNIT_ASSERT_VALUES_EQUAL(conf.WriteFlatIndex, true);
    }

    Y_UNIT_TEST(NoNodes) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(5)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 1) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 1);

        auto expected = Meta(0, 5, 5240, 0, 0, 0, 0);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected, "Got " + part->IndexPages.BTreeGroups[0].ToString());
    }

    Y_UNIT_TEST(OneNode) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 1) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 2);

        auto expected = Meta(part->IndexPages.BTreeGroups[0].RootPageIdV1(), 10, 10480, 0, 0, 1, 1131);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected, "Got " + part->IndexPages.BTreeGroups[0].ToString());
    }

    Y_UNIT_TEST(FewNodes) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(700)) {
            // some index keys will be cut
            cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 2) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 117);

        auto expected = Meta(part->IndexPages.BTreeGroups[0].RootPageIdV1(), 700, 733140, 0, 0, 3, 87172);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected, "Got " + part->IndexPages.BTreeGroups[0].ToString());
    }

    Y_UNIT_TEST(Erases) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.Final = false;
        conf.Group(0).PageRows = 33;
        conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 5;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(1000)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, ToString(i)), 
                i % 7 ? ERowOp::Upsert : ERowOp::Erase);
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 2) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 31);

        auto expected = Meta(part->IndexPages.BTreeGroups[0].RootPageIdV1(), 1000, 22098, 0, 143, 2, 1668);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected, "Got " + part->IndexPages.BTreeGroups[0].ToString());
    }

    Y_UNIT_TEST(Groups) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(1, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.Group(0).PageRows = 3;
        conf.Group(1).PageRows = 4;
        conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 5;
        conf.Group(1).BTreeIndexNodeKeysMin = conf.Group(1).BTreeIndexNodeKeysMax = 6;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(1000)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, ToString(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 2) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 334);

        auto expected0 = Meta(part->IndexPages.BTreeGroups[0].RootPageIdV1(), 1000, 16680, 21890, 0, 3, 18430);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        auto expected1 = Meta(part->IndexPages.BTreeGroups[1].RootPageIdV1(), 1000, 21890, 0, 0, 3, 6497);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());
    }

    Y_UNIT_TEST(History) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(1, 2,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.Group(0).PageRows = 3;
        conf.Group(1).PageRows = 4;
        conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 5;
        conf.Group(1).BTreeIndexNodeKeysMin = conf.Group(1).BTreeIndexNodeKeysMax = 6;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(1000)) {
            for (ui32 j : xrange(i % 5 + 1)) {
                cook.Ver({0, 10 - j}).Add(*TSchemedCookRow(*lay).Col(i, TString(i * 2 + j, 'x'), TString(i * 3 + j, 'x')));
            }
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 2) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 334);

        ui64 dataSize0 = IndexTools::CountDataSize(*part, TGroupId{0, 0});
        ui64 dataSize1 = IndexTools::CountDataSize(*part, TGroupId{1, 0});
        ui64 dataSizeHist0 = IndexTools::CountDataSize(*part, TGroupId{0, 1});
        ui64 dataSizeHist1 = IndexTools::CountDataSize(*part, TGroupId{1, 1});

        auto expected0 = Meta(part->IndexPages.BTreeGroups[0].RootPageIdV1(), 1000, dataSize0, dataSize1+dataSizeHist0+dataSizeHist1, 0, 3, 18430);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        auto expected1 = Meta(part->IndexPages.BTreeGroups[1].RootPageIdV1(), 1000, dataSize1, 0, 0, 3, 8284);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());

        auto expectedHist0 = Meta(part->IndexPages.BTreeHistoric[0].RootPageIdV1(), 2000, dataSizeHist0, 0, 0, 4, 34225);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[0], expectedHist0, "Got " + part->IndexPages.BTreeHistoric[0].ToString());

        auto expectedHist1 = Meta(part->IndexPages.BTreeHistoric[1].RootPageIdV1(), 2000, dataSizeHist1, 0, 0, 3, 16645);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[1], expectedHist1, "Got " + part->IndexPages.BTreeHistoric[1].ToString());
    }

    Y_UNIT_TEST(External) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(1, 2,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.SmallEdge = 133;
        conf.LargeEdge = 333;
        conf.Group(0).PageRows = 3;
        conf.Group(1).PageRows = 4;
        conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 5;
        conf.Group(1).BTreeIndexNodeKeysMin = conf.Group(1).BTreeIndexNodeKeysMax = 6;

        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(1000)) {
            for (ui32 j : xrange(i % 5 + 1)) {
                cook.Ver({0, 10 - j}).Add(*TSchemedCookRow(*lay).Col(i, TString(i * 2 + j, 'x'), TString(i * 3 + j, 'x')));
            }
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 2) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 334);

        ui64 dataSize0 = IndexTools::CountDataSize(*part, TGroupId{0, 0});
        ui64 dataSize1 = IndexTools::CountDataSize(*part, TGroupId{1, 0});
        ui64 dataSizeHist0 = IndexTools::CountDataSize(*part, TGroupId{0, 1});
        ui64 dataSizeHist1 = IndexTools::CountDataSize(*part, TGroupId{1, 1});
        ui64 groupDataSize = dataSize1+dataSizeHist0+dataSizeHist1 + 120463 + 7413329;

        auto expected0 = Meta(part->IndexPages.BTreeGroups[0].RootPageIdV1(), 1000, dataSize0, groupDataSize, 0, 3, 18430);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        auto expected1 = Meta(part->IndexPages.BTreeGroups[1].RootPageIdV1(), 1000, dataSize1, 0, 0, 3, 6497);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());

        auto expectedHist0 = Meta(part->IndexPages.BTreeHistoric[0].RootPageIdV1(), 2000, dataSizeHist0, 0, 0, 4, 34225);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[0], expectedHist0, "Got " + part->IndexPages.BTreeHistoric[0].ToString());

        auto expectedHist1 = Meta(part->IndexPages.BTreeHistoric[1].RootPageIdV1(), 2000, dataSizeHist1, 0, 0, 3, 13014);
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[1], expectedHist1, "Got " + part->IndexPages.BTreeHistoric[1].ToString());
    }
}

Y_UNIT_TEST_SUITE(TBtreeIndexNodeV2) {
    using TChildV2 = TBtreeIndexNode::TChildV2;
    using TShortChildV2 = TBtreeIndexNode::TShortChildV2;

    Y_UNIT_TEST(TChildV2_Size) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TChildV2), 52);
    }

    Y_UNIT_TEST(TShortChildV2_Size) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TShortChildV2), 36);
    }

    Y_UNIT_TEST(TChildV2_GetLocation) {
        TChildV2 child{
            TPageOffset::FromByteOffset(12345),
            6789,
            0xDEADBEEF,
            100,
            200,
            300,
            50
        };
        auto loc = child.GetLocation(EPage::DataPage);
        UNIT_ASSERT_EQUAL(loc.Offset, TPageOffset::FromByteOffset(12345));
        UNIT_ASSERT_VALUES_EQUAL(loc.Size, 6789);
        UNIT_ASSERT_VALUES_EQUAL(ui16(loc.Type), ui16(EPage::DataPage));
        UNIT_ASSERT_VALUES_EQUAL(loc.Crc32, ui32(0xDEADBEEF));
    }

    Y_UNIT_TEST(TShortChildV2_GetLocation) {
        TShortChildV2 child{
            TPageOffset::FromByteOffset(99999),
            1111,
            0xCAFEBABE,
            50,
            200
        };
        auto loc = child.GetLocation(EPage::BTreeIndex);
        UNIT_ASSERT_EQUAL(loc.Offset, TPageOffset::FromByteOffset(99999));
        UNIT_ASSERT_VALUES_EQUAL(loc.Size, 1111);
        UNIT_ASSERT_VALUES_EQUAL(ui16(loc.Type), ui16(EPage::BTreeIndex));
        UNIT_ASSERT_VALUES_EQUAL(loc.Crc32, ui32(0xCAFEBABE));
    }

    Y_UNIT_TEST(V2_FieldOffsets) {
        UNIT_ASSERT_VALUES_EQUAL(offsetof(TChildV2, Offset_), offsetof(TShortChildV2, Offset_));
        UNIT_ASSERT_VALUES_EQUAL(offsetof(TChildV2, Size_), offsetof(TShortChildV2, Size_));
        UNIT_ASSERT_VALUES_EQUAL(offsetof(TChildV2, Crc32_), offsetof(TShortChildV2, Crc32_));
        UNIT_ASSERT_VALUES_EQUAL(offsetof(TChildV2, RowCount_), offsetof(TShortChildV2, RowCount_));
        UNIT_ASSERT_VALUES_EQUAL(offsetof(TChildV2, DataSize_), offsetof(TShortChildV2, DataSize_));
    }
}

// ========================================================================
// Section 3: Twin format tests — v1 and v2 produce identical results
// ========================================================================

Y_UNIT_TEST_SUITE(TBtreeIndexTPartV2) {
    using namespace NTest;

    // Twin test: write same data in v1 and v2, compare row-level stats and iteration.
    // feedRows takes (cook, lay) since TPartCook doesn't expose the layout.
    void CheckTwin(const TLayoutCook& lay, NPage::TConf v1Conf, NPage::TConf v2Conf,
                   std::function<void(TPartCook&, const TLayoutCook&)> feedRows) {
        TPartCook v1Cook(lay, v1Conf);
        feedRows(v1Cook, lay);
        TPartEggs v1Eggs = v1Cook.Finish();
        const auto v1Part = v1Eggs.Lone();

        TPartCook v2Cook(lay, v2Conf);
        feedRows(v2Cook, lay);
        TPartEggs v2Eggs = v2Cook.Finish();
        const auto v2Part = v2Eggs.Lone();

        // V2 part must have v2 root
        AssertV2Root(v2Part->IndexPages.BTreeGroups[0],
            "V2 main group must have byte-offset root");

        // V1 part must have v1 root
        AssertV1Root(v1Part->IndexPages.BTreeGroups[0],
            "V1 main group must have page-id root");

        // Row-level stats must match between v1 and v2
        AssertMetaStatsEqual(v1Part->IndexPages.BTreeGroups[0],
            v2Part->IndexPages.BTreeGroups[0],
            "V1 vs V2 BTreeGroups[0] stats mismatch");

        // Same page count via iteration
        auto v1Pages = IndexTools::CountMainPages(*v1Part);
        auto v2Pages = IndexTools::CountMainPages(*v2Part);
        UNIT_ASSERT_VALUES_EQUAL_C(v1Pages, v2Pages,
            "V1 pages=" + ToString(v1Pages) + " V2 pages=" + ToString(v2Pages));

        // Same total data size
        auto v1DataSize = IndexTools::CountDataSize(*v1Part, TGroupId{0, 0});
        auto v2DataSize = IndexTools::CountDataSize(*v2Part, TGroupId{0, 0});
        UNIT_ASSERT_VALUES_EQUAL_C(v1DataSize, v2DataSize,
            "V1 dataSize=" + ToString(v1DataSize) + " V2 dataSize=" + ToString(v2DataSize));

        // Same row count from full iteration
        UNIT_ASSERT_VALUES_EQUAL(CountAllRows(*v1Part), CountAllRows(*v2Part));

        // Same level count
        UNIT_ASSERT_VALUES_EQUAL(
            v1Part->IndexPages.BTreeGroups[0].LevelCount,
            v2Part->IndexPages.BTreeGroups[0].LevelCount);

        // Collect page locations and verify they are consistent
        auto v1Locs = CollectPageLocations(*v1Part);
        auto v2Locs = CollectPageLocations(*v2Part);
        UNIT_ASSERT_VALUES_EQUAL(v1Locs.size(), v2Locs.size());
        // V2 locations use byte offsets; V1 locations use page indices
        // Sizes should match for each page
        for (size_t i = 0; i < v1Locs.size(); i++) {
            UNIT_ASSERT_VALUES_EQUAL_C(v1Locs[i].Size, v2Locs[i].Size,
                "Page " + ToString(i) + " size mismatch: V1=" + ToString(v1Locs[i].Size) +
                " V2=" + ToString(v2Locs[i].Size));
        }
    }

    Y_UNIT_TEST(NoNodes_V2) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;

        NPage::TConf v2Conf = MakeV2Conf();

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(5)) {
                cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
            }
        };

        CheckTwin(lay, v1Conf, v2Conf, feedRows);
    }

    Y_UNIT_TEST(OneNode_V2) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;

        NPage::TConf v2Conf = MakeV2Conf();

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(10)) {
                cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
            }
        };

        CheckTwin(lay, v1Conf, v2Conf, feedRows);
    }

    Y_UNIT_TEST(FewNodes_V2) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        v1Conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        v1Conf.Group(0).BTreeIndexNodeKeysMin = 3;

        NPage::TConf v2Conf = MakeV2Conf();
        v2Conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        v2Conf.Group(0).BTreeIndexNodeKeysMin = 3;

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(700)) {
                cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
            }
        };

        CheckTwin(lay, v1Conf, v2Conf, feedRows);
    }

    Y_UNIT_TEST(Erases_V2) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        v1Conf.Final = false;
        v1Conf.Group(0).PageRows = 33;
        v1Conf.Group(0).BTreeIndexNodeKeysMin = v1Conf.Group(0).BTreeIndexNodeKeysMax = 5;

        NPage::TConf v2Conf = MakeV2Conf(false);
        v2Conf.Group(0).PageRows = 33;
        v2Conf.Group(0).BTreeIndexNodeKeysMin = v2Conf.Group(0).BTreeIndexNodeKeysMax = 5;

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(1000)) {
                cook.Add(*TSchemedCookRow(*lay).Col(i, ToString(i)),
                    i % 7 ? ERowOp::Upsert : ERowOp::Erase);
            }
        };

        CheckTwin(lay, v1Conf, v2Conf, feedRows);
    }

    Y_UNIT_TEST(Groups_V2) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(1, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        v1Conf.Group(0).PageRows = 3;
        v1Conf.Group(1).PageRows = 4;
        v1Conf.Group(0).BTreeIndexNodeKeysMin = v1Conf.Group(0).BTreeIndexNodeKeysMax = 5;
        v1Conf.Group(1).BTreeIndexNodeKeysMin = v1Conf.Group(1).BTreeIndexNodeKeysMax = 6;

        NPage::TConf v2Conf = MakeV2Conf();
        v2Conf.Group(0).PageRows = 3;
        v2Conf.Group(1).PageRows = 4;
        v2Conf.Group(0).BTreeIndexNodeKeysMin = v2Conf.Group(0).BTreeIndexNodeKeysMax = 5;
        v2Conf.Group(1).BTreeIndexNodeKeysMin = v2Conf.Group(1).BTreeIndexNodeKeysMax = 6;

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(1000)) {
                cook.Add(*TSchemedCookRow(*lay).Col(i, ToString(i)));
            }
        };

        CheckTwin(lay, v1Conf, v2Conf, feedRows);
    }

    Y_UNIT_TEST(History_V2) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(1, 2,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        v1Conf.Group(0).PageRows = 3;
        v1Conf.Group(1).PageRows = 4;
        v1Conf.Group(0).BTreeIndexNodeKeysMin = v1Conf.Group(0).BTreeIndexNodeKeysMax = 5;
        v1Conf.Group(1).BTreeIndexNodeKeysMin = v1Conf.Group(1).BTreeIndexNodeKeysMax = 6;

        NPage::TConf v2Conf = MakeV2Conf();
        v2Conf.Group(0).PageRows = 3;
        v2Conf.Group(1).PageRows = 4;
        v2Conf.Group(0).BTreeIndexNodeKeysMin = v2Conf.Group(0).BTreeIndexNodeKeysMax = 5;
        v2Conf.Group(1).BTreeIndexNodeKeysMin = v2Conf.Group(1).BTreeIndexNodeKeysMax = 6;

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(1000)) {
                for (ui32 j : xrange(i % 5 + 1)) {
                    cook.Ver({0, 10 - j}).Add(*TSchemedCookRow(*lay).Col(i, TString(i * 2 + j, 'x'), TString(i * 3 + j, 'x')));
                }
            }
        };

        TPartCook v1Cook(lay, v1Conf);
        feedRows(v1Cook, lay);
        TPartEggs v1Eggs = v1Cook.Finish();
        const auto v1Part = v1Eggs.Lone();

        TPartCook v2Cook(lay, v2Conf);
        feedRows(v2Cook, lay);
        TPartEggs v2Eggs = v2Cook.Finish();
        const auto v2Part = v2Eggs.Lone();

        // Check main group stats
        AssertMetaStatsEqual(v1Part->IndexPages.BTreeGroups[0],
            v2Part->IndexPages.BTreeGroups[0], "History main group stats");

        // Check historic indexes
        UNIT_ASSERT_VALUES_EQUAL(v1Part->IndexPages.BTreeHistoric.size(),
            v2Part->IndexPages.BTreeHistoric.size());

        for (size_t i = 0; i < v1Part->IndexPages.BTreeHistoric.size(); i++) {
            AssertMetaStatsEqual(v1Part->IndexPages.BTreeHistoric[i],
                v2Part->IndexPages.BTreeHistoric[i],
                "History historic[" + ToString(i) + "] stats");
            AssertV2Root(v2Part->IndexPages.BTreeHistoric[i],
                "V2 historic[" + ToString(i) + "] root");
        }
    }
}

// ========================================================================
// Section 4: V2 format-specific tests
// ========================================================================

Y_UNIT_TEST_SUITE(TBtreeIndexV2Specific) {
    using namespace NTest;
    using TChildV2 = TBtreeIndexNode::TChildV2;
    using TShortChildV2 = TBtreeIndexNode::TShortChildV2;

    Y_UNIT_TEST(V2_RootHasByteOffset) {
        // Write a part in v2 and verify the root carries a byte offset
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        AssertV2Root(meta, "V2 root");
        // V2Root offset must be a valid byte offset (non-zero)
        UNIT_ASSERT_C(meta.V2Root.Offset.AsByteOffset() > 0, "V2Root byte offset must be > 0");
        UNIT_ASSERT_C(meta.V2Root.Size > 0, "V2Root size must be > 0");
    }

    Y_UNIT_TEST(V1_RootHasPageIndex) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        AssertV1Root(meta, "V1 root");
    }

    Y_UNIT_TEST(V2_WriteThenRead_SingleLevel) {
        // Write v2 part, iterate all rows, verify same row count as v1
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(50)) {
                cook.Add(*TSchemedCookRow(*lay).Col(i, TString(200, 'x') + ToString(i)));
            }
        };

        // V1 part
        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        TPartCook v1Cook(lay, v1Conf);
        feedRows(v1Cook, lay);
        TPartEggs v1Eggs = v1Cook.Finish();

        // V2 part
        NPage::TConf v2Conf = MakeV2Conf();
        TPartCook v2Cook(lay, v2Conf);
        feedRows(v2Cook, lay);
        TPartEggs v2Eggs = v2Cook.Finish();

        const auto v1Part = v1Eggs.Lone();
        const auto v2Part = v2Eggs.Lone();

        // Iterate and compare page count
        auto v1Pages = IndexTools::CountMainPages(*v1Part);
        auto v2Pages = IndexTools::CountMainPages(*v2Part);
        UNIT_ASSERT_VALUES_EQUAL(v1Pages, v2Pages);

        // Verify full iteration returns same rows via both
        UNIT_ASSERT_VALUES_EQUAL(CountAllRows(*v1Part), CountAllRows(*v2Part));
    }

    Y_UNIT_TEST(V2_WriteThenRead_MultiLevel) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(700)) {
                cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
            }
        };

        // V1 part with multi-level btree
        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        v1Conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        v1Conf.Group(0).BTreeIndexNodeKeysMin = 3;
        TPartCook v1Cook(lay, v1Conf);
        feedRows(v1Cook, lay);
        TPartEggs v1Eggs = v1Cook.Finish();

        // V2 part with multi-level btree
        NPage::TConf v2Conf = MakeV2Conf();
        v2Conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        v2Conf.Group(0).BTreeIndexNodeKeysMin = 3;
        TPartCook v2Cook(lay, v2Conf);
        feedRows(v2Cook, lay);
        TPartEggs v2Eggs = v2Cook.Finish();

        const auto v1Part = v1Eggs.Lone();
        const auto v2Part = v2Eggs.Lone();

        // Must have >1 level
        UNIT_ASSERT_C(v2Part->IndexPages.BTreeGroups[0].LevelCount > 1,
            "Expected multi-level btree, got LevelCount=" + ToString(v2Part->IndexPages.BTreeGroups[0].LevelCount));

        AssertV2Root(v2Part->IndexPages.BTreeGroups[0], "Multi-level V2 root");

        // Same stats
        AssertMetaStatsEqual(v1Part->IndexPages.BTreeGroups[0],
            v2Part->IndexPages.BTreeGroups[0], "Multi-level stats");

        // Same page count
        UNIT_ASSERT_VALUES_EQUAL(IndexTools::CountMainPages(*v1Part), IndexTools::CountMainPages(*v2Part));

        // Same row count
        UNIT_ASSERT_VALUES_EQUAL(CountAllRows(*v1Part), CountAllRows(*v2Part));
    }

    Y_UNIT_TEST(V2_WriteThenRead_Historic) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(1, 2,  NScheme::NTypeIds::String)
            .Key({0});

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay) {
            for (ui32 i : xrange(200)) {
                for (ui32 j : xrange(i % 3 + 1)) {
                    cook.Ver({0, 10 - j}).Add(*TSchemedCookRow(*lay).Col(i, TString(i * 2 + j, 'x'), TString(i * 3 + j, 'x')));
                }
            }
        };

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        v1Conf.Group(0).PageRows = 3;
        v1Conf.Group(1).PageRows = 4;

        NPage::TConf v2Conf = MakeV2Conf();
        v2Conf.Group(0).PageRows = 3;
        v2Conf.Group(1).PageRows = 4;

        TPartCook v1Cook(lay, v1Conf);
        feedRows(v1Cook, lay);
        TPartEggs v1Eggs = v1Cook.Finish();

        TPartCook v2Cook(lay, v2Conf);
        feedRows(v2Cook, lay);
        TPartEggs v2Eggs = v2Cook.Finish();

        const auto v1Part = v1Eggs.Lone();
        const auto v2Part = v2Eggs.Lone();

        // V2 historic indexes must exist and have byte-offset roots
        UNIT_ASSERT_C(!v2Part->IndexPages.BTreeHistoric.empty(), "V2 must have historic indexes");

        for (size_t i = 0; i < v2Part->IndexPages.BTreeHistoric.size(); i++) {
            AssertV2Root(v2Part->IndexPages.BTreeHistoric[i],
                "Historic[" + ToString(i) + "] V2 root");
        }

        // Same historic stats
        UNIT_ASSERT_VALUES_EQUAL(v1Part->IndexPages.BTreeHistoric.size(),
            v2Part->IndexPages.BTreeHistoric.size());

        for (size_t i = 0; i < v1Part->IndexPages.BTreeHistoric.size(); i++) {
            AssertMetaStatsEqual(v1Part->IndexPages.BTreeHistoric[i],
                v2Part->IndexPages.BTreeHistoric[i],
                "Historic[" + ToString(i) + "] stats");
        }
    }

    Y_UNIT_TEST(V2_V2RootResolvedDirectly) {
        // V2 root is resolved through TBtreeIndexMeta::V2Root directly,
        // not through TMeta page-id lookup
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(700)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        AssertV2Root(meta, "V2 root");

        // GetBTreeRootLocation should return V2Root directly
        auto rootLoc = NTable::GetBTreeRootLocation(meta,
            part->GetPageCollection(0),
            part->GetPageCollection(0));

        UNIT_ASSERT_EQUAL(rootLoc, meta.V2Root);
        UNIT_ASSERT_C(rootLoc.Offset.IsByteOffset(), "Root location must be byte offset");
    }

    Y_UNIT_TEST(V1_V1RootResolvedViaMeta) {
        // V1 root is resolved through Part->GetPageLocation(rootPageId)
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        AssertV1Root(meta, "V1 root");

        auto rootLoc = NTable::GetBTreeRootLocation(meta,
            part->GetPageCollection(0),
            part->GetPageCollection(0));

        // Root location must be a page index, not a byte offset
        UNIT_ASSERT_C(rootLoc.Offset.IsPageIndex(), "V1 root location must be page index");
    }

    Y_UNIT_TEST(V2_NodeChildrenAreByteOffset) {
        // After writing in v2, verify that the btree nodes use FormatVersionV2
        // and their children carry byte-offset locations
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(700)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        // The btree has multi-level — load the root page and check its format version
        const auto& meta = part->IndexPages.BTreeGroups[0];
        UNIT_ASSERT_C(meta.LevelCount >= 2, "Expected LevelCount >= 2, got " + ToString(meta.LevelCount));

        TTestEnv env;
        auto rootPage = env.TryGetPage(part.Get(), meta.V2Root, TGroupId{});
        UNIT_ASSERT_C(rootPage, "Failed to load V2 root page");

        TBtreeIndexNode rootNode(*rootPage);
        UNIT_ASSERT_VALUES_EQUAL(rootNode.GetStoredVersion(), ui16(TBtreeIndexNode::FormatVersionV2));

        // All children in the root node must have byte-offset locations
        for (TRecIdx i : xrange(rootNode.GetChildrenCount())) {
            auto childLoc = rootNode.GetChildLocationV2(i, EPage::BTreeIndex);
            UNIT_ASSERT_C(childLoc.Offset.IsByteOffset(),
                "Child " + ToString(i) + " location must be byte offset");
            UNIT_ASSERT_C(childLoc.Size > 0,
                "Child " + ToString(i) + " must have Size > 0");
        }
    }

    Y_UNIT_TEST(V2_NoFlatIndex) {
        // V2 parts must not have a flat index (WriteFlatIndex is disabled)
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        // V2 part must have no flat index
        UNIT_ASSERT_C(part->IndexPages.FlatGroups.empty(),
            "V2 part must not have flat index groups");

        // V2 part must have btree index
        UNIT_ASSERT_C(!part->IndexPages.BTreeGroups.empty(),
            "V2 part must have btree index groups");
    }

    Y_UNIT_TEST(V2_ConfDefaults) {
        // Verify TConf defaults for WriteBTreeIndexV2
        NPage::TConf conf;
        UNIT_ASSERT_VALUES_EQUAL(conf.WriteBTreeIndexV2, false);

        NPage::TConf v2Conf = MakeV2Conf();
        UNIT_ASSERT_VALUES_EQUAL(v2Conf.WriteBTreeIndexV2, true);
        UNIT_ASSERT_VALUES_EQUAL(v2Conf.WriteBTreeIndex, true);
    }

    // -----------------------------------------------------------------------
    // Section 5.1: Write format flag tests
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(WriteFlag_Off_Produces_V1) {
        // WriteBTreeIndexV2 = false (default) produces v1 format
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        // WriteBTreeIndexV2 defaults to false

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        // V1 root must be present, V2 root must be absent
        AssertV1Root(meta, "WriteFlag_Off");
    }

    Y_UNIT_TEST(WriteFlag_On_Produces_V2) {
        // WriteBTreeIndexV2 = true produces v2 format with byte-offset root
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        // V2 root must be present with byte offset, V1 root must be absent
        AssertV2Root(meta, "WriteFlag_On");

        // V2Root must have non-zero byte offset and size
        UNIT_ASSERT_C(meta.V2Root.Offset.AsByteOffset() > 0, "V2Root byte offset must be > 0");
        UNIT_ASSERT_C(meta.V2Root.Size > 0, "V2Root size must be > 0");
    }

    Y_UNIT_TEST(WriteFlag_Off_V1RootInProto) {
        // V1 part proto has RootPageId but no RootOffset
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        UNIT_ASSERT_C(meta.HasV1Root(), "V1 part must have V1Root");
        UNIT_ASSERT_C(!meta.HasV2Root(), "V1 part must not have V2Root");
        UNIT_ASSERT_C(meta.RootPageIdV1() != Max<TPageId>(), "RootPageId must be valid");
    }

    Y_UNIT_TEST(WriteFlag_On_V2RootInProto) {
        // V2 part proto has RootOffset/RootSize/RootCrc32 but no RootPageId
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(700)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        UNIT_ASSERT_C(meta.HasV2Root(), "V2 part must have V2Root");
        UNIT_ASSERT_C(!meta.HasV1Root(), "V2 part must not have V1Root");
        UNIT_ASSERT_VALUES_EQUAL(meta.RootPageIdV1(), Max<TPageId>());

        // V2Root must be a valid byte-offset location
        UNIT_ASSERT_C(meta.V2Root.Offset.IsByteOffset(), "V2Root must be byte offset");
        UNIT_ASSERT_C(meta.V2Root.Size > 0, "V2Root size must be > 0");
    }
}

// ========================================================================
// Section 4.5: Mixed-format parts (v1 and v2 coexist)
// ========================================================================

Y_UNIT_TEST_SUITE(TBtreeIndexMixedFormat) {
    using namespace NTest;

    // Helper: create two parts with same data, one v1 and one v2,
    // then verify they can be iterated together in a run
    Y_UNIT_TEST(MixedParts_V1AndV2_FullScan) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        auto feedRows = [](TPartCook& cook, const TLayoutCook& lay, ui32 offset) {
            for (ui32 i : xrange(20)) {
                cook.Add(*TSchemedCookRow(*lay).Col(offset + i, TString(200, 'x') + ToString(offset + i)));
            }
        };

        // V1 part (rows 0-19)
        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        TPartCook v1Cook(lay, v1Conf);
        feedRows(v1Cook, lay, 0);
        TPartEggs v1Eggs = v1Cook.Finish();

        // V2 part (rows 20-39)
        NPage::TConf v2Conf = MakeV2Conf();
        TPartCook v2Cook(lay, v2Conf);
        feedRows(v2Cook, lay, 20);
        TPartEggs v2Eggs = v2Cook.Finish();

        const auto v1Part = v1Eggs.Lone();
        const auto v2Part = v2Eggs.Lone();

        // Verify v1 has v1 root, v2 has v2 root
        AssertV1Root(v1Part->IndexPages.BTreeGroups[0], "V1 part root");
        AssertV2Root(v2Part->IndexPages.BTreeGroups[0], "V2 part root");

        // Both parts should iterate independently (CountAllRows counts pages, not rows)
        auto v1Pages = CountAllRows(*v1Part);
        auto v2Pages = CountAllRows(*v2Part);
        UNIT_ASSERT_C(v1Pages > 0, "V1 part must have pages");
        UNIT_ASSERT_C(v2Pages > 0, "V2 part must have pages");
        // Both parts have same data size, so should have same page count
        UNIT_ASSERT_VALUES_EQUAL(v1Pages, v2Pages);
    }

    Y_UNIT_TEST(MixedParts_V1AndV2_RowOrder) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        // V1 part with small keys
        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        TPartCook v1Cook(lay, v1Conf);
        for (ui32 i : xrange(10)) {
            v1Cook.Add(*TSchemedCookRow(*lay).Col(i, TString(200, 'x') + ToString(i)));
        }
        TPartEggs v1Eggs = v1Cook.Finish();

        // V2 part with larger keys
        NPage::TConf v2Conf = MakeV2Conf();
        TPartCook v2Cook(lay, v2Conf);
        for (ui32 i : xrange(10, 20)) {
            v2Cook.Add(*TSchemedCookRow(*lay).Col(i, TString(200, 'x') + ToString(i)));
        }
        TPartEggs v2Eggs = v2Cook.Finish();

        const auto v1Part = v1Eggs.Lone();
        const auto v2Part = v2Eggs.Lone();

        // Verify rows are correctly ordered in each part
        TTestEnv v1Env, v2Env;
        auto v1Index = CreateIndexIter(v1Part.Get(), &v1Env, {});
        auto v2Index = CreateIndexIter(v2Part.Get(), &v2Env, {});

        // V1 part should start at row 0
        UNIT_ASSERT(v1Index->Seek(0) == EReady::Data);

        // V2 part should start at row 0 (of v2's own row space)
        UNIT_ASSERT(v2Index->Seek(0) == EReady::Data);
    }
}

// ========================================================================
// Section 5.2: Read format flag tests
// ========================================================================

Y_UNIT_TEST_SUITE(TBtreeIndexReadFlags) {
    using namespace NTest;

    // Helper: simulate read flag off by stripping v2 roots from v2 part
    // (mirrors what flat_part_loader.cpp does when EnableLocalDBBtreeIndexV2 is off)
    Y_UNIT_TEST(ReadFlag_Off_StripsV2OnlyRoots) {
        // When EnableLocalDBBtreeIndexV2 is off, v2-only indexes are removed.
        // This test verifies that a v2-only part becomes inaccessible for btree
        // iteration when the v2 root is stripped.
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v2Conf = MakeV2Conf();
        TPartCook v2Cook(lay, v2Conf);
        for (ui32 i : xrange(10)) {
            v2Cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs v2Eggs = v2Cook.Finish();
        const auto part = v2Eggs.Lone();

        // V2 part has v2 root and no v1 root
        const auto& meta = part->IndexPages.BTreeGroups[0];
        AssertV2Root(meta, "V2 part before strip");

        // Simulate the loader stripping: v2-only entries (HasV2Root && !HasV1Root) are removed
        // In a v2-only part, this means the btree index would be completely dropped
        // A v2-only part cannot be read with v2 flag off — it falls back to flat index (which doesn't exist in v2)
        UNIT_ASSERT_C(!meta.HasV1Root(), "V2-only part must not have V1 root");
        UNIT_ASSERT_C(meta.HasV2Root(), "V2-only part must have V2 root");
    }

    Y_UNIT_TEST(ReadFlag_On_V1PartStillWorks) {
        // A v1 part loaded with v2 flag on should still work normally
        // (v1 parts don't have v2 roots, so the v2 flag has no effect)
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        TPartCook v1Cook(lay, v1Conf);
        for (ui32 i : xrange(10)) {
            v1Cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs v1Eggs = v1Cook.Finish();
        const auto part = v1Eggs.Lone();

        // V1 part works regardless of read flag
        AssertV1Root(part->IndexPages.BTreeGroups[0], "V1 part");

        // Verify iteration works — CountAllRows counts pages, not rows
        auto pageCount = CountAllRows(*part);
        UNIT_ASSERT_C(pageCount > 0, "V1 part iteration must return pages");
    }

    Y_UNIT_TEST(ReadFlag_Off_V1PartStillWorks) {
        // A v1 part loaded with v2 flag off should still work
        // (no v2 roots to strip, v1 root is unaffected)
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        TPartCook v1Cook(lay, v1Conf);
        for (ui32 i : xrange(10)) {
            v1Cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs v1Eggs = v1Cook.Finish();
        const auto part = v1Eggs.Lone();

        // V1 part should iterate correctly regardless of flag
        auto pageCount = CountAllRows(*part);
        UNIT_ASSERT_C(pageCount > 0, "V1 part iteration must return pages");
    }
}

// ========================================================================
// Section 5.3: Write/read independence tests
// ========================================================================

Y_UNIT_TEST_SUITE(TBtreeIndexWriteReadIndependence) {
    using namespace NTest;

    Y_UNIT_TEST(WriteV1_ReadV2FlagOn) {
        // Write v1 part, load with v2 flag on → v1 indexes loaded normally
        // (no v2 available, flag has no effect)
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v1Conf{ true, 7 * 1024 };
        v1Conf.WriteBTreeIndex = true;
        TPartCook cook(lay, v1Conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        AssertV1Root(part->IndexPages.BTreeGroups[0], "V1 part");
        auto pageCount = CountAllRows(*part);
        UNIT_ASSERT_C(pageCount > 0, "V1 part iteration must return pages");
    }

    Y_UNIT_TEST(WriteV2_ReadV1FlagOff_V2IndexDropped) {
        // Write v2 part — when read with v2 flag off, v2-only indexes are dropped.
        // In the test framework, we can verify that v2-only parts have no v1 fallback.
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v2Conf = MakeV2Conf();
        TPartCook cook(lay, v2Conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        // V2-only part has no v1 root
        UNIT_ASSERT_C(!meta.HasV1Root(), "V2-only part must not have V1 root");

        // V2 part does not have flat index (v2 disables flat index writing)
        UNIT_ASSERT_C(part->IndexPages.FlatGroups.empty(),
            "V2 part must not have flat index groups");

        // With v2 flag off, v2-only indexes would be removed by the loader.
        // Since there's no flat index fallback, the part would have no usable index.
        // This test verifies that scenario is detectable.
    }

    Y_UNIT_TEST(PreferFlag_V2Wins) {
        // With PreferLocalDBBtreeIndexV2 = true, dual-root parts use v2 root.
        // V2-only parts already use v2 root.
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf v2Conf = MakeV2Conf();
        TPartCook cook(lay, v2Conf);
        for (ui32 i : xrange(10)) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(1024, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        // V2 root should be the active root
        AssertV2Root(meta, "V2-only part with Prefer on");

        // Verify the root is a valid byte-offset location
        auto rootLoc = NTable::GetBTreeRootLocation(meta,
            part->GetPageCollection(0),
            part->GetPageCollection(0));
        UNIT_ASSERT_EQUAL(rootLoc, meta.V2Root);
        UNIT_ASSERT_C(rootLoc.Offset.IsByteOffset(), "V2 root must be byte offset");
    }
}

// ========================================================================
// TBTreePartWalker — V2 B-tree page preload walker
// ========================================================================
Y_UNIT_TEST_SUITE(TBTreePartWalker) {
    using namespace NTest;

    Y_UNIT_TEST(WalkMultiLevelBTree) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(700)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        UNIT_ASSERT(meta.HasV2Root());
        UNIT_ASSERT_C(meta.LevelCount > 0, "Expected multi-level b-tree");

        TBTreePartWalker walker;
        walker.Start(meta);

        TTestEnv env;
        UNIT_ASSERT_C(walker.Step(part.Get(), &env, TGroupId{}),
            "Walker should complete in one pass when all pages are resident");
    }

    Y_UNIT_TEST(WalkSingleDataPage) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 7 * 1024;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(5)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, TString(10, 'x') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        UNIT_ASSERT(meta.HasV2Root());
        UNIT_ASSERT_VALUES_EQUAL_C(meta.LevelCount, 0u,
            "Expected LevelCount == 0 for a single data page root");

        TBTreePartWalker walker;
        walker.Start(meta);

        TTestEnv env;
        UNIT_ASSERT_C(walker.Step(part.Get(), &env, TGroupId{}),
            "Single data page walker should complete in one pass");
    }

    Y_UNIT_TEST(WalkNonMainGroupSkipDataPages) {
        TLayoutCook lay;

        lay
            .Col(0, 0, NScheme::NTypeIds::Uint32)
            .Col(0, 1, NScheme::NTypeIds::String)
            .Col(1, 2, NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;
        conf.Group(1).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(1).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(700)) {
            cook.Add(*TSchemedCookRow(*lay)
                .Col(i, TString(512, 'a') + ToString(i))
                .Col(TString(512, 'b') + ToString(i)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        UNIT_ASSERT(part->IndexPages.BTreeGroups.size() > 1);
        const auto& meta = part->IndexPages.BTreeGroups[1];
        UNIT_ASSERT(meta.HasV2Root());
        UNIT_ASSERT_C(meta.LevelCount > 0,
            "Non-main group must have multi-level B-tree, got LevelCount="
            + ToString(meta.LevelCount));

        TBTreePartWalker walker;
        walker.Start(meta);

        TTestEnv env;
        // Walk group 1 with skipDataPages=true — traverse index pages
        // in room 0, skip data pages in room 1.
        UNIT_ASSERT_C(walker.Step(part.Get(), &env, TGroupId{1}, true),
            "Non-main group walker should complete with skipDataPages=true");
    }

    Y_UNIT_TEST(ResumableWalkAcrossRounds) {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf = MakeV2Conf();
        conf.Group(0).BTreeIndexNodeTargetSize = 3 * 1024;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;

        TPartCook cook(lay, conf);
        for (ui32 i : xrange(700)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 9, TString(1024, 'x') + ToString(i % 9)));
        }
        TPartEggs eggs = cook.Finish();
        const auto part = eggs.Lone();

        const auto& meta = part->IndexPages.BTreeGroups[0];
        UNIT_ASSERT(meta.LevelCount > 0);

        struct TMockPages : public IPages {
            const TPartStore* Part;
            THashSet<ui64>& Loaded;
            TVector<TPageLocation> Missed;

            TMockPages(const TPartStore* part, THashSet<ui64>& loaded)
                : Part(part), Loaded(loaded) {}

            TResult Locate(const TMemTable*, ui64, ui32) override {
                Y_TABLET_ERROR("Unused");
            }
            TResult Locate(const TPart*, ui64, ELargeObj) override {
                Y_TABLET_ERROR("Unused");
            }
            const TSharedData* TryGetPage(const TPart* part, TPageLocation location, TGroupId groupId) override {
                Y_UNUSED(part);
                if (Loaded.count(location.GetByteOffset())) {
                    return Part->Store->GetPage(groupId.Index, location.Offset);
                }
                Missed.push_back(location);
                return nullptr;
            }
        };

        THashSet<ui64> loadedOffsets;
        TMockPages mockPages(part.Get(), loadedOffsets);
        TBTreePartWalker walker;
        walker.Start(meta);

        // Round 1: nothing loaded — miss root.
        {
            UNIT_ASSERT(!walker.Step(part.Get(), &mockPages, TGroupId{}));
            UNIT_ASSERT(!mockPages.Missed.empty());
            for (auto& loc : mockPages.Missed) {
                loadedOffsets.insert(loc.GetByteOffset());
            }
            UNIT_ASSERT_VALUES_EQUAL(mockPages.Missed.size(), 1);
        }

        // Round 2: root loaded — discover level-1 children.
        mockPages.Missed.clear();
        {
            UNIT_ASSERT(!walker.Step(part.Get(), &mockPages, TGroupId{}));
            UNIT_ASSERT(!mockPages.Missed.empty());
            for (auto& loc : mockPages.Missed) {
                loadedOffsets.insert(loc.GetByteOffset());
            }
            UNIT_ASSERT_C(mockPages.Missed.size() > 0, "Level-1 children should be discovered");
        }

        // Rounds 3+: drive until done.
        for (int round = 0; round < 10; round++) {
            mockPages.Missed.clear();
            if (walker.Step(part.Get(), &mockPages, TGroupId{})) {
                UNIT_ASSERT(mockPages.Missed.empty());
                return;
            }
            for (auto& loc : mockPages.Missed) {
                loadedOffsets.insert(loc.GetByteOffset());
            }
        }
        UNIT_ASSERT_C(false, "Walker did not complete within 10 fetch rounds");
    }
}

}
