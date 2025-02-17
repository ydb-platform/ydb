#include "flat_page_btree_index.h"
#include "flat_page_btree_index_writer.h"
#include "test/libs/table/test_writer.h"
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NPage {

namespace {
    using namespace NTest;
    using TShortChild = TBtreeIndexNode::TShortChild;
    using TChild = TBtreeIndexNode::TChild;

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

    void Dump(TChild meta, const TPartScheme::TGroupInfo& groupInfo, const TStore& store, ui32 level = 0) noexcept
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
            Cerr << intend << " | > ";

            auto cells = node.GetKeyCellsIter(i, groupInfo.ColsKeyIdx);
            for (TPos pos : xrange(cells.Count())) {
                TString str;
                DbgPrintValue(str, cells.Next(), groupInfo.KeyTypes[pos]);
                if (str.size() > 10) {
                    str = str.substr(0, 10) + "..";
                }
                Cerr << (pos ? ", " : "") << str;
            }

            Cerr << Endl;
            dumpChild(node, i + 1);
        }

        Cerr << Endl;
    }

    void Dump(TSharedData node, const TPartScheme::TGroupInfo& groupInfo) {
        TWriterBundle pager(1, TLogoBlobID());
        auto pageId = ((IPageWriter*)&pager)->Write(node, EPage::BTreeIndex, 0);
        TChild page{pageId, 0, 0, 0, 0};
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

        TBtreeIndexMeta expected{child, 0, 0};
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

        TBtreeIndexMeta expected{{0, 1155, 11055, 22055, 385}, 1, 683};
        UNIT_ASSERT_EQUAL_C(result, expected, "Got " + result.ToString());

        CheckKeys(result.GetPageId(), keys, builder.GroupInfo, pager.Back());
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
        
        TBtreeIndexMeta expected{{9, 0, 0, 0, 0}, 3, 1790};
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
        
        TBtreeIndexMeta expected{{15, 15150, 106050, 207050, 8080}, 3, 11198};
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

        TBtreeIndexMeta expected{{0 /*Data page*/, 5, 5240, 0, 0}, 0, 0};
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

        TBtreeIndexMeta expected{{part->IndexPages.BTreeGroups[0].GetPageId(), 10, 10480, 0, 0}, 1, 1131};
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

        TBtreeIndexMeta expected{{part->IndexPages.BTreeGroups[0].GetPageId(), 700, 733140, 0, 0}, 3, 87172};
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

        TBtreeIndexMeta expected{{part->IndexPages.BTreeGroups[0].GetPageId(), 1000, 22098, 0, 143}, 2, 1668};
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

        TBtreeIndexMeta expected0{{part->IndexPages.BTreeGroups[0].GetPageId(), 1000, 16680, 21890, 0}, 3, 18430};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        TBtreeIndexMeta expected1{{part->IndexPages.BTreeGroups[1].GetPageId(), 1000, 21890, 0, 0}, 3, 6497};
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

        TBtreeIndexMeta expected0{{part->IndexPages.BTreeGroups[0].GetPageId(), 1000, dataSize0, dataSize1+dataSizeHist0+dataSizeHist1, 0}, 3, 18430};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        TBtreeIndexMeta expected1{{part->IndexPages.BTreeGroups[1].GetPageId(), 1000, dataSize1, 0, 0}, 3, 8284};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());

        TBtreeIndexMeta expectedHist0{{part->IndexPages.BTreeHistoric[0].GetPageId(), 2000, dataSizeHist0, 0, 0}, 4, 34225};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[0], expectedHist0, "Got " + part->IndexPages.BTreeHistoric[0].ToString());

        TBtreeIndexMeta expectedHist1{{part->IndexPages.BTreeHistoric[1].GetPageId(), 2000, dataSizeHist1, 0, 0}, 3, 16645};
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

        TBtreeIndexMeta expected0{{part->IndexPages.BTreeGroups[0].GetPageId(), 1000, dataSize0, groupDataSize, 0}, 3, 18430};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        TBtreeIndexMeta expected1{{part->IndexPages.BTreeGroups[1].GetPageId(), 1000, dataSize1, 0, 0}, 3, 6497};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());

        TBtreeIndexMeta expectedHist0{{part->IndexPages.BTreeHistoric[0].GetPageId(), 2000, dataSizeHist0, 0, 0}, 4, 34225};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[0], expectedHist0, "Got " + part->IndexPages.BTreeHistoric[0].ToString());

        TBtreeIndexMeta expectedHist1{{part->IndexPages.BTreeHistoric[1].GetPageId(), 2000, dataSizeHist1, 0, 0}, 3, 13014};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[1], expectedHist1, "Got " + part->IndexPages.BTreeHistoric[1].ToString());
    }
}

}
