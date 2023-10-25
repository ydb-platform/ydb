#include "flat_page_btree_index.h"
#include "flat_page_btree_index_writer.h"
#include "test/libs/table/test_writer.h"
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NPage {

namespace {
    using namespace NTest;
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
        return TChild{index + 10000, index + 100, index + 30, index + 1000};
    }

    void Dump(TChild page, const TPartScheme& scheme, const TStore& store, ui32 level = 0) noexcept
    {
        TString intend(level * 2, ' ');
        auto dumpChild = [&] (TChild child) {
            if (child.PageId < 1000) {
                Dump(child, scheme, store, level + 1);
            } else {
                Cerr << intend << "| " << child.ToString() << Endl;
            }
        };

        auto node = TBtreeIndexNode(*store.GetPage(0, page.PageId));

        auto label = node.Label();

        Cerr
            << intend
            << "+ BTreeIndex{id=" << page.PageId << ", "
            << "cnt=" << page.Count << ", "
            << "size=" << page.Size << ", "
            << (ui16)label.Type << " rev " << label.Format << ", " 
            << label.Size << "b}"
            << Endl;

        dumpChild(node.GetChild(0));

        for (TRecIdx i : xrange(node.GetKeysCount())) {
            Cerr << intend << "|-> ";

            auto cells = node.GetKeyCells(i, scheme.Groups[0].ColsKeyIdx);
            for (TPos pos : xrange(cells.Count())) {
                TString str;
                DbgPrintValue(str, cells.Next(), scheme.Groups[0].KeyTypes[pos]);
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

    void Dump(TSharedData node, const TPartScheme& scheme) {
        TWriterBundle pager(1, TLogoBlobID());
        auto pageId = ((IPageWriter*)&pager)->Write(node, EPage::BTreeIndex, 0);
        TChild page{pageId, 0, 0, 0};
        Dump(page, scheme, pager.Back());
    }

    void CheckKeys(const NPage::TBtreeIndexNode& node, const TVector<TString>& keys, const TPartScheme& scheme) {
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeysCount(), keys.size());
        for (TRecIdx i : xrange(node.GetKeysCount())) {
            TVector<TCell> actualCells;
            auto cells = node.GetKeyCells(i, scheme.Groups[0].ColsKeyIdx);
            UNIT_ASSERT_VALUES_EQUAL(cells.Count(), scheme.Groups[0].ColsKeyIdx.size());
            
            for (TPos pos : xrange(cells.Count())) {
                Y_UNUSED(pos);
                actualCells.push_back(cells.Next());
            }

            auto actual = TSerializedCellVec::Serialize(actualCells);
            UNIT_ASSERT_VALUES_EQUAL(actual, keys[i]);
        }
    }

    void CheckKeys(TPageId pageId, const TVector<TString>& keys, const TPartScheme& scheme, const TStore& store) {
        auto page = store.GetPage(0, pageId);
        auto node = TBtreeIndexNode(*page);
        CheckKeys(node, keys, scheme);
    }

    void CheckChildren(const NPage::TBtreeIndexNode& node, const TVector<TChild>& children) {
        UNIT_ASSERT_VALUES_EQUAL(node.GetKeysCount() + 1, children.size());
        for (TRecIdx i : xrange(node.GetKeysCount() + 1)) {
            UNIT_ASSERT_EQUAL(node.GetChild(i), children[i]);
        }
    }
}

Y_UNIT_TEST_SUITE(TBtreeIndexNode) {
    using namespace NTest;
    using TChild = TBtreeIndexNode::TChild;

    Y_UNIT_TEST(TKeyBitmap) {
        TString buffer(754, 0);
        auto* key = (TBtreeIndexNode::TKey*)(buffer.data());

        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(4), 1);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(7), 1);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(8), 1);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(9), 2);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(256), 32);
        UNIT_ASSERT_VALUES_EQUAL(key->NullBitmapLength(257), 33);

        for (TPos pos : xrange(buffer.size() * 8)) {
            UNIT_ASSERT(!key->IsNull(pos));
            key->SetNull(pos);
            UNIT_ASSERT(key->IsNull(pos));
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

        Dump(serialized, *writer.Scheme);
        CheckKeys(node, keys, *writer.Scheme);
        CheckChildren(node, children);
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

            Dump(serialized, *writer.Scheme);
            CheckKeys(node, keys, *writer.Scheme);
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

        Dump(serialized, *writer.Scheme);
        CheckKeys(node, keys, *writer.Scheme);
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

        Dump(serialized, *writer.Scheme);
        CheckKeys(node, fullKeys, *writer.Scheme);
        CheckChildren(node, children);
    }

}

Y_UNIT_TEST_SUITE(TBtreeIndexBuilder) {
    using namespace NTest;
    using TChild = TBtreeIndexNode::TChild;

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

        Dump(*result, *scheme, pager.Back());

        UNIT_ASSERT_VALUES_EQUAL(result->LevelsCount, 1);
        CheckKeys(result->PageId, keys, *scheme, pager.Back());
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

        Dump(*result, *scheme, pager.Back());
        
        UNIT_ASSERT_VALUES_EQUAL(result->LevelsCount, 3);
        
        auto checkKeys = [&](TPageId pageId, const TVector<TString>& keys) {
            CheckKeys(pageId, keys, *scheme, pager.Back());
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

        TChild expected{9, 0, 0, 0};
        for (auto c : children) {
            expected.Count += c.Count;
            expected.ErasedCount += c.ErasedCount;
            expected.Size += c.Size;
        }
        UNIT_ASSERT_EQUAL(*result, expected);
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

        Dump(*result, *scheme, pager.Back());
        
        UNIT_ASSERT_VALUES_EQUAL(result->LevelsCount, 3);
    }

}

}
