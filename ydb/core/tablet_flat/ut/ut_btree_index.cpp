#include "flat_page_btree_index.h"
#include "flat_page_btree_index_writer.h"
#include "flat_part_btree_index_iter.h"
#include "flat_part_charge.h"
#include "flat_part_charge_btree_index.h"
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

        static void LoadTouched(IPages& env, bool clearHas) {
            auto touchEnv = dynamic_cast<TTouchEnv*>(&env);
            if (touchEnv) {
                auto &has = touchEnv->Loaded;
                auto &touched = touchEnv->Touched;

                if (clearHas) {
                    has.clear();
                }
                for (const auto &g : touched) {
                    has[g.first].insert(g.second.begin(), g.second.end());
                }
                touched.clear();
            }
        }

        TMap<TGroupId, TSet<TPageId>> Loaded;
        TMap<TGroupId, TSet<TPageId>> Touched;
    };

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
        return TChild{index + 10000, index + 100, index + 1000, index + 30};
    }

    void Dump(TChild meta, const TPartScheme::TGroupInfo& groupInfo, const TStore& store, ui32 level = 0) noexcept
    {
        TString intend;
        for (size_t i = 0; i < level; i++) {
            intend += " |";
        }

        auto dumpChild = [&] (TChild child) {
            if (child.PageId < 1000) {
                Dump(child, groupInfo, store, level + 1);
            } else {
                Cerr << intend << " | " << child.ToString() << Endl;
            }
        };

        auto node = TBtreeIndexNode(*store.GetPage(0, meta.PageId));

        auto label = node.Label();

        Cerr
            << intend
            << " + BTreeIndex{"
            << meta.ToString() << ", "
            << (ui16)label.Type << " rev " << label.Format << ", " 
            << label.Size << "b}"
            << Endl;

        dumpChild(node.GetChild(0));

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
            dumpChild(node.GetChild(i + 1));
        }

        Cerr << Endl;
    }

    void Dump(TSharedData node, const TPartScheme::TGroupInfo& groupInfo) {
        TWriterBundle pager(1, TLogoBlobID());
        auto pageId = ((IPageWriter*)&pager)->Write(node, EPage::BTreeIndex, 0);
        TChild page{pageId, 0, 0, 0};
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
            TShortChild shortChild{children[i].PageId, children[i].RowCount, children[i].DataSize};
            UNIT_ASSERT_EQUAL(node.GetShortChild(i), shortChild);
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

        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto &c : children) {
            c.ErasedRowCount = 0;
            writer.AddChild(c);
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, keys, writer.GroupInfo);
        CheckChildren(node, children);
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

        TVector<TChild> children;
        for (ui32 i : xrange(keys.size() + 1)) {
            children.push_back(MakeChild(i));
        }

        for (auto k : keys) {
            TSerializedCellVec deserialized(k);
            writer.AddKey(deserialized.GetCells());
        }
        for (auto &c : children) {
            c.ErasedRowCount = 0;
            writer.AddChild(c);
        }

        auto serialized = writer.Finish();

        auto node = TBtreeIndexNode(serialized);

        Dump(serialized, writer.GroupInfo);
        CheckKeys(node, keys, writer.GroupInfo);
        CheckChildren(node, children);

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
        auto result = builder.Flush(pager, true);
        UNIT_ASSERT(result);

        TBtreeIndexMeta expected{child, 0, 0};
        UNIT_ASSERT_EQUAL_C(*result, expected, "Got " + result->ToString());
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
        auto result = builder.Flush(pager, true);
        UNIT_ASSERT(result);

        Dump(*result, builder.GroupInfo, pager.Back());

        TBtreeIndexMeta expected{{0, 1155, 11055, 385}, 1, 595};
        UNIT_ASSERT_EQUAL_C(*result, expected, "Got " + result->ToString());

        CheckKeys(result->PageId, keys, builder.GroupInfo, pager.Back());
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
            UNIT_ASSERT(!builder.Flush(pager, false));
        }

        auto result = builder.Flush(pager, true);
        UNIT_ASSERT(result);

        Dump(*result, builder.GroupInfo, pager.Back());
        
        UNIT_ASSERT_VALUES_EQUAL(result->LevelCount, 3);
        
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

        TBtreeIndexMeta expected{{9, 0, 0, 0}, 3, 1550};
        for (auto c : children) {
            expected.RowCount += c.RowCount;
            expected.DataSize += c.DataSize;
            expected.ErasedRowCount += c.ErasedRowCount;
        }
        UNIT_ASSERT_EQUAL_C(*result, expected, "Got " + result->ToString());
    }

    Y_UNIT_TEST(SplitBySize) {
        TLayoutCook lay = MakeLayout();
        TIntrusivePtr<TPartScheme> scheme = new TPartScheme(lay.RowScheme()->Cols);

        TBtreeIndexBuilder builder(scheme, { }, 600, 1, Max<ui32>());
        
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
            UNIT_ASSERT(!builder.Flush(pager, false));
        }

        auto result = builder.Flush(pager, true);
        UNIT_ASSERT(result);

        Dump(*result, builder.GroupInfo, pager.Back());
        
        TBtreeIndexMeta expected{{15, 15150, 106050, 8080}, 3, 10270};
        UNIT_ASSERT_EQUAL_C(*result, expected, "Got " + result->ToString());
    }

}

Y_UNIT_TEST_SUITE(TBtreeIndexTPart) {

    Y_UNIT_TEST(Conf) {
        NPage::TConf conf;

        // do not accidentally turn this setting on in trunk
        UNIT_ASSERT_VALUES_EQUAL(conf.WriteBTreeIndex, false);
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

        TBtreeIndexMeta expected{{0 /*Data page*/, 5, 5240, 0}, 0, 0};
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

        TBtreeIndexMeta expected{{3, 10, 10480, 0}, 1, 1115};
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

        TBtreeIndexMeta expected{{143, 700, 733140, 0}, 3, 86036};
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

        TBtreeIndexMeta expected{{37, 1000, 22098, 143}, 2, 1380};
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

        TBtreeIndexMeta expected0{{438, 1000, 16680, 0}, 3, 15246};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        TBtreeIndexMeta expected1{{441, 1000, 21890, 0}, 3, 6497};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());
    }

    Y_UNIT_TEST(History) {
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
            for (ui32 j : xrange(i % 5 + 1)) {
                cook.Ver({0, 10 - j}).Add(*TSchemedCookRow(*lay).Col(i, ToString(i * 10 + j)));
            }
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << DumpPart(*part, 2) << Endl;

        auto pages = IndexTools::CountMainPages(*part);
        UNIT_ASSERT_VALUES_EQUAL(pages, 334);

        TBtreeIndexMeta expected0{{1315, 1000, 32680, 0}, 3, 15246};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[0], expected0, "Got " + part->IndexPages.BTreeGroups[0].ToString());

        TBtreeIndexMeta expected1{{1318, 1000, 22889, 0}, 3, 6497};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeGroups[1], expected1, "Got " + part->IndexPages.BTreeGroups[1].ToString());

        TBtreeIndexMeta expectedHist0{{1322, 2000, 77340, 0}, 4, 34225};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[0], expectedHist0, "Got " + part->IndexPages.BTreeHistoric[0].ToString());

        TBtreeIndexMeta expectedHist1{{1325, 2000, 45780, 0}, 3, 13014};
        UNIT_ASSERT_EQUAL_C(part->IndexPages.BTreeHistoric[1], expectedHist1, "Got " + part->IndexPages.BTreeHistoric[1].ToString());
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

    EReady Retry(std::function<EReady()> action, IPages& env, const TString& message, ui32 failsAllowed = 10) {
        while (true) {
            if (auto ready = action(); ready != EReady::Page) {
                return ready;
            }
            TTouchEnv::LoadTouched(env, false);
            UNIT_ASSERT_C(failsAllowed--, "Too many fails " + message);
        }
        Y_UNREACHABLE();
    }

    EReady SeekRowId(IIndexIter& iter, IPages& env, TRowId rowId, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return iter.Seek(rowId);
        }, env, message, failsAllowed);
    }

    EReady SeekLast(IIndexIter& iter, IPages& env, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return iter.SeekLast();
        }, env, message, failsAllowed);
    }

    EReady SeekKey(IIndexIter& iter, IPages& env, ESeek seek, bool reverse, TCells key, const TKeyCellDefaults *keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            if (reverse) {
                return iter.SeekReverse(seek, key, keyDefaults);
            } else {
                return iter.Seek(seek, key, keyDefaults);
            }
        }, env, message, failsAllowed);
    }

    EReady NextPrev(IIndexIter& iter, IPages& env, bool next, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            if (next) {
                return iter.Next();
            } else {
                return iter.Prev();
            }
        }, env, message, failsAllowed);
    }

    template<typename TEnv>
    void CheckSeekRowId(const TPartStore& part) {
        for (TRowId rowId1 : xrange(part.Stat.Rows + 1)) {
            for (TRowId rowId2 : xrange(part.Stat.Rows + 1)) {
                TEnv env;
                TPartBtreeIndexIt bTree(&part, &env, { });
                TPartIndexIt flat(&part, &env, { });

                // checking initial seek:
                {
                    TString message = TStringBuilder() << "SeekRowId<" << typeid(TEnv).name() << "> " << rowId1;
                    EReady bTreeReady = SeekRowId(bTree, env, rowId1, message);
                    EReady flatReady = SeekRowId(flat, env, rowId1, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId1 < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }

                // checking repositioning:
                {
                    TString message = TStringBuilder() << "SeekRowId<" << typeid(TEnv).name() << "> " << rowId1 << " -> " << rowId2;
                    EReady bTreeReady = SeekRowId(bTree, env, rowId2, message);
                    EReady flatReady = SeekRowId(flat, env, rowId2, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId2 < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }
            }
        }
    }

    template<typename TEnv>
    void CheckSeekLast(const TPartStore& part) {
        TEnv env;
        TPartBtreeIndexIt bTree(&part, &env, { });
        TPartIndexIt flat(&part, &env, { });

        TString message = TStringBuilder() << "SeekLast<" << typeid(TEnv).name() << ">";
        EReady bTreeReady = SeekLast(bTree, env, message);
        EReady flatReady = SeekLast(flat, env, message);
        UNIT_ASSERT_VALUES_EQUAL(bTreeReady, EReady::Data);
        AssertEqual(bTree, bTreeReady, flat, flatReady, message);
    }

    template<typename TEnv>
    void CheckSeekKey(const TPartStore& part, const TKeyCellDefaults *keyDefaults) {
        for (bool reverse : {false, true}) {
            for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
                for (ui32 keyId : xrange(0u, static_cast<ui32>(part.Stat.Rows) + 2)) {
                    TVector<TCell> key{TCell::Make(keyId / 7), TCell::Make(keyId % 7)};

                    while (true) {
                        TEnv env;
                        TPartBtreeIndexIt bTree(&part, &env, { });
                        TPartIndexIt flat(&part, &env, { });

                        TStringBuilder message = TStringBuilder() << (reverse ?  "SeekKeyReverse<" : "SeekKey<") << typeid(TEnv).name() << ">(" << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        
                        EReady bTreeReady = SeekKey(bTree, env, seek, reverse, key, keyDefaults, message);
                        EReady flatReady = SeekKey(flat, env, seek, reverse, key, keyDefaults, message);
                        UNIT_ASSERT_VALUES_EQUAL_C(bTreeReady, key.empty() ? flatReady : EReady::Data, "Can't be exhausted");
                        AssertEqual(bTree, bTreeReady, flat, flatReady, message, !key.empty());

                        if (!key) {
                            break;
                        }
                        key.pop_back();
                    }
                }
            }
        }
    }

    template<typename TEnv>
    void CheckNextPrev(const TPartStore& part) {
        for (bool next : {true, false}) {
            for (TRowId rowId : xrange(part.Stat.Rows)) {
                TEnv env;
                TPartBtreeIndexIt bTree(&part, &env, { });
                TPartIndexIt flat(&part, &env, { });

                // checking initial seek:
                {
                    TString message = TStringBuilder() << "CheckNext<" << typeid(TEnv).name() << "> " << rowId;
                    EReady bTreeReady = SeekRowId(bTree, env, rowId, message);
                    EReady flatReady = SeekRowId(flat, env, rowId, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }

                // checking next:
                while (true)
                {
                    TString message = TStringBuilder() << "CheckNext<" << typeid(TEnv).name() << "> " << rowId << " -> " << rowId;
                    EReady bTreeReady = NextPrev(bTree, env, next, message);
                    EReady flatReady = NextPrev(flat, env, next, message);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                    if (flatReady == EReady::Gone) {
                        break;
                    }
                }
            }
        }
    }

    void CheckPart(TConf&& conf, ui32 rows, ui32 levels) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Key({0, 1});

        conf.WriteBTreeIndex = true;
        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(1u, rows + 1)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 7, i % 7));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = *eggs.Lone();

        Cerr << DumpPart(part, 1) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[0].LevelCount, levels);

        CheckSeekRowId<TTestEnv>(part);
        CheckSeekRowId<TTouchEnv>(part);
        CheckSeekLast<TTestEnv>(part);
        CheckSeekLast<TTouchEnv>(part);
        CheckSeekKey<TTestEnv>(part, eggs.Scheme->Keys.Get());
        CheckSeekKey<TTouchEnv>(part, eggs.Scheme->Keys.Get());
        CheckNextPrev<TTestEnv>(part);
        CheckNextPrev<TTouchEnv>(part);
    }

    Y_UNIT_TEST(NoNodes) {
        NPage::TConf conf;

        CheckPart(std::move(conf), 100, 0);
    }

    Y_UNIT_TEST(OneNode) {
        NPage::TConf conf;
        conf.Group(0).PageRows = 2;

        CheckPart(std::move(conf), 100, 1);
    }

    Y_UNIT_TEST(FewNodes) {
        NPage::TConf conf;
        conf.Group(0).PageRows = 2;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;
        conf.Group(0).BTreeIndexNodeKeysMax = 4;

        CheckPart(std::move(conf), 300, 3);
    }
}

Y_UNIT_TEST_SUITE(TChargeBTreeIndex) {
    void AssertEqual(const TPartStore& part, const TMap<TGroupId, TSet<TPageId>>& bTree, const TMap<TGroupId, TSet<TPageId>>& flat, const TString& message) {
        TSet<TGroupId> groupIds;
        for (const auto &c : {bTree, flat}) {
            for (const auto &g : c) {
                groupIds.insert(g.first);
            }
        }

        for (TGroupId groupId : groupIds) {
            TSet<TPageId> bTreeDataPages, flatDataPages;
            for (TPageId pageId : bTree.Value(groupId, TSet<TPageId>{})) {
                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                    bTreeDataPages.insert(pageId);
                }
            }
            for (TPageId pageId : flat.Value(groupId, TSet<TPageId>{})) {
                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                    flatDataPages.insert(pageId);
                }
            }

            UNIT_ASSERT_VALUES_EQUAL_C(flatDataPages, bTreeDataPages, message);
        }
    }

    void AssertEqual(const TPartStore& part, const TTouchEnv& bTree, const TTouchEnv& flat, const TString& message) {
        AssertEqual(part, bTree.Loaded, flat.Loaded, message);
        AssertEqual(part, bTree.Touched, flat.Touched, message);
    }

    void DoChargeRowId(ICharge& charge, IPages& env, const TRowId row1, const TRowId row2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        while (true) {
            bool ready = reverse
                ? charge.DoReverse(row1, row2, keyDefaults, itemsLimit, bytesLimit)
                : charge.Do(row1, row2, keyDefaults, itemsLimit, bytesLimit);
            if (ready) {
                return;
            }
            TTouchEnv::LoadTouched(env, false);
            UNIT_ASSERT_C(failsAllowed--, "Too many fails " + message);
        }
        Y_UNREACHABLE();
    }

    void CheckChargeRowId(const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults, bool reverse) {
        for (TRowId rowId1 : xrange(part.Stat.Rows + 1)) {
            for (TRowId rowId2 : xrange(part.Stat.Rows + 1)) {
                TTouchEnv bTreeEnv, flatEnv;
                TChargeBTreeIndex bTree(&bTreeEnv, part, tags, true);
                TCharge flat(&flatEnv, part, tags, true);

                TString message = TStringBuilder() << (reverse ? "ChargeRowIdReverse " : "ChargeRowId ") << rowId1 << " " << rowId2;
                DoChargeRowId(bTree, bTreeEnv, rowId1, rowId2, 0, 0, reverse, *keyDefaults, message);
                DoChargeRowId(flat, flatEnv, rowId1, rowId2, 0, 0, reverse, *keyDefaults, message);
                AssertEqual(part, bTreeEnv, flatEnv, message);
            }
        }
    }

    void CheckPart(TConf&& conf, ui32 rows, ui32 levels) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Key({0, 1});

        conf.WriteBTreeIndex = true;
        TPartCook cook(lay, conf);
        
        for (ui32 i : xrange(1u, rows + 1)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i / 7, i % 7));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = *eggs.Lone();

        Cerr << DumpPart(part, 1) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[0].LevelCount, levels);

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        CheckChargeRowId(part, tags, eggs.Scheme->Keys.Get(), false);
        CheckChargeRowId(part, tags, eggs.Scheme->Keys.Get(), true);
    }

    Y_UNIT_TEST(NoNodes) {
        NPage::TConf conf;

        CheckPart(std::move(conf), 100, 0);
    }

    Y_UNIT_TEST(OneNode) {
        NPage::TConf conf;
        conf.Group(0).PageRows = 2;

        CheckPart(std::move(conf), 100, 1);
    }

    Y_UNIT_TEST(FewNodes) {
        NPage::TConf conf;
        conf.Group(0).PageRows = 2;
        conf.Group(0).BTreeIndexNodeKeysMin = 3;
        conf.Group(0).BTreeIndexNodeKeysMax = 4;

        CheckPart(std::move(conf), 300, 3);
    }
}

}
