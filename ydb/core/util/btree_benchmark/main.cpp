#include <ydb/core/util/btree.h>
#include <ydb/core/util/btree_cow.h>

#include <library/cpp/testing/benchmark/bench.h>
#include <library/cpp/threading/skip_list/skiplist.h>

#include <util/generic/vector.h>
#include <util/random/random.h>

#include <unordered_map>
#include <map>

namespace {

template<class TKey, class TValue>
struct TKeyValue {
    TKey Key;
    TValue Value;

    struct TCompare {
        int operator()(const TKeyValue& a, const TKeyValue& b) const {
            if (a.Key < b.Key) {
                return -1;
            } else if (b.Key < a.Key) {
                return +1;
            } else {
                return 0;
            }
        }

        int operator()(const TKeyValue& a, const TKey& b) const {
            if (a.Key < b) {
                return -1;
            } else if (b < a.Key) {
                return +1;
            } else {
                return 0;
            }
        }
    };
};

template<class TKey, class TValue>
TKeyValue<TKey, TValue> MakeKeyValue(TKey key, TValue value) {
    return TKeyValue<TKey, TValue>{ std::move(key), std::move(value) };
}

// Models TIdxKey<TKeyLogoBlobKey, ...> from blobstorage
struct TLargeKey {
    ui64 Lsn;
    ui64 Raw[3];

    bool operator<(const TLargeKey& rhs) const {
        if (Raw[0] != rhs.Raw[0]) { return Raw[0] < rhs.Raw[0]; }
        if (Raw[1] != rhs.Raw[1]) { return Raw[1] < rhs.Raw[1]; }
        if (Raw[2] != rhs.Raw[2]) { return Raw[2] < rhs.Raw[2]; }
        return Lsn < rhs.Lsn;
    }

    static TLargeKey Random(ui64 lsn) {
        return TLargeKey{ lsn, { RandomNumber<ui64>(), RandomNumber<ui64>(), RandomNumber<ui64>() } };
    }
};

// Models TMemRecLogoBlob from blobstorage
struct TLargeValue {
    ui64 Ingress;
    ui32 Other[3];

    static TLargeValue Random() {
        return TLargeValue{ RandomNumber<ui64>(), { RandomNumber<ui32>(), RandomNumber<ui32>(), RandomNumber<ui32>() } };
    }
};

class TFixtureData {
public:
    using TMyTree = NKikimr::TBTree<ui64, ui64>;
    using TMyCowTree = NKikimr::TCowBTree<ui64, ui64>;
    using TMyList = NThreading::TSkipList<TKeyValue<ui64, ui64>, TKeyValue<ui64, ui64>::TCompare>;
    using TMyHashMap = std::unordered_map<ui64, ui64>;
    using TMyMap = std::map<ui64, ui64>;

    using TMyLargeTree = NKikimr::TBTree<TLargeKey, TLargeValue, TLess<TLargeKey>, TMemoryPool, 1024>;
    using TMyLargeCowTree = NKikimr::TCowBTree<TLargeKey, TLargeValue, TLess<TLargeKey>, std::allocator<void>, 1024>;
    using TMyLargeList = NThreading::TSkipList<TKeyValue<TLargeKey, TLargeValue>, TKeyValue<TLargeKey, TLargeValue>::TCompare>;
    using TMyLargeMap = std::map<TLargeKey, TLargeValue>;

    static constexpr size_t Count = 1000000;
    static constexpr size_t LargeKeysCount = 500000;

    TVector<ui64> UniqueKeys;

    TMemoryPool TreePool{8192};
    TMyTree Tree{TreePool};
    TMyTree::TSafeAccess TreeSafeAccess;

    TMyCowTree CowTree;
    TMyCowTree::TSnapshot CowSnapshot;

    TMemoryPool ListPool{8192};
    TMyList List{ListPool};

    TMyHashMap HashMap;
    TMyMap Map;

    TFixtureData() {
        while (Tree.Size() < Count) {
            ui64 key = RandomNumber<ui64>();
            if (Tree.Emplace(key, ~key)) {
                UniqueKeys.push_back(key);
            }
        }
        for (ui64 key : UniqueKeys) {
            CowTree.Emplace(key, ~key);
            List.Insert(MakeKeyValue(key, ~key));
            HashMap.emplace(key, ~key);
            Map.emplace(key, ~key);
        }
        Y_ABORT_UNLESS(Tree.Size() == Count);
        Y_ABORT_UNLESS(CowTree.Size() == Count);
        Y_ABORT_UNLESS(List.GetSize() == Count);
        Y_ABORT_UNLESS(HashMap.size() == Count);
        Y_ABORT_UNLESS(Map.size() == Count);

        TreeSafeAccess = Tree.SafeAccess();

        // Snapshot creation is not thread-safe
        CowSnapshot = CowTree.Snapshot();
    }
};

TFixtureData FixtureData;

}

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(TestRandomInsertBTree, iface) {
    TMemoryPool pool(8192);
    TFixtureData::TMyTree tree(pool);

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            tree.Emplace(key, ~key);
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertBTreeThreadSafe, iface) {
    TMemoryPool pool(8192);
    TFixtureData::TMyTree tree(pool);

    // Having a live iterator is enough to force thread-safe mode
    auto it = tree.SafeAccess().Iterator();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            tree.Emplace(key, ~key);
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertBTreeLargeKeys, iface) {
    TMemoryPool pool(8192);
    TFixtureData::TMyLargeTree tree(pool);

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 lsn = 0;
        for (size_t j = 0; j < TFixtureData::LargeKeysCount; ++j) {
            tree.Emplace(TLargeKey::Random(++lsn), TLargeValue::Random());
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertBTreeThreadSafeLargeKeys, iface) {
    TMemoryPool pool(8192);
    TFixtureData::TMyLargeTree tree(pool);

    // Having a live iterator is enough to force thread-safe mode
    auto it = tree.SafeAccess().Iterator();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 lsn = 0;
        for (size_t j = 0; j < TFixtureData::LargeKeysCount; ++j) {
            tree.Emplace(TLargeKey::Random(++lsn), TLargeValue::Random());
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertCowBTree, iface) {
    TFixtureData::TMyCowTree tree;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            tree.Emplace(key, ~key);
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertCowBTreeWorstCase, iface) {
    TFixtureData::TMyCowTree tree;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            auto snapshot = tree.Snapshot();
            tree.Emplace(key, ~key);
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertCowBTreeLargeKeys, iface) {
    TFixtureData::TMyLargeCowTree tree;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 lsn = 0;
        for (size_t j = 0; j < TFixtureData::LargeKeysCount; ++j) {
            tree.Emplace(TLargeKey::Random(++lsn), TLargeValue::Random());
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertCowBTreeLargeKeysWorstCase, iface) {
    TFixtureData::TMyLargeCowTree tree;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 lsn = 0;
        for (size_t j = 0; j < TFixtureData::LargeKeysCount; ++j) {
            auto snapshot = tree.Snapshot();
            tree.Emplace(TLargeKey::Random(++lsn), TLargeValue::Random());
        }
        tree.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertSkipList, iface) {
    TMemoryPool pool(8192);
    TFixtureData::TMyList list(pool);

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            list.Insert(MakeKeyValue(key, ~key));
        }
        list.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertSkipListLargeKeys, iface) {
    TMemoryPool pool(8192);
    TFixtureData::TMyLargeList list(pool);

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 lsn = 0;
        for (size_t j = 0; j < TFixtureData::LargeKeysCount; ++j) {
            list.Insert(MakeKeyValue(TLargeKey::Random(++lsn), TLargeValue::Random()));
        }
        list.Clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertHashMap, iface) {
    TFixtureData::TMyHashMap map;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            map.emplace(key, ~key);
        }
        map.clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertMap, iface) {
    TFixtureData::TMyMap map;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (ui64 key : FixtureData.UniqueKeys) {
            map.emplace(key, ~key);
        }
        map.clear();
    }
}

Y_CPU_BENCHMARK(TestRandomInsertMapLargeKeys, iface) {
    TFixtureData::TMyLargeMap map;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 lsn = 0;
        for (size_t j = 0; j < TFixtureData::LargeKeysCount; ++j) {
            map.emplace(TLargeKey::Random(++lsn), TLargeValue::Random());
        }
        map.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(TestRandomSearchBTree, iface) {
    auto it = FixtureData.TreeSafeAccess.Iterator();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        for (ui64 key : FixtureData.UniqueKeys) {
            it.SeekLowerBound(key);
            Y_ABORT_UNLESS(it.IsValid() && it.GetKey() == key);
            sum += it.GetValue();
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
    }
}

Y_CPU_BENCHMARK(TestRandomSearchCowBTree, iface) {
    auto it = FixtureData.CowSnapshot.Iterator();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        for (ui64 key : FixtureData.UniqueKeys) {
            it.SeekLowerBound(key);
            Y_ABORT_UNLESS(it.IsValid() && it.GetKey() == key);
            sum += it.GetValue();
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
    }
}

Y_CPU_BENCHMARK(TestRandomSearchCowBTreeWorstCase, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        for (ui64 key : FixtureData.UniqueKeys) {
            auto it = FixtureData.CowSnapshot.Iterator();
            it.SeekLowerBound(key);
            Y_ABORT_UNLESS(it.IsValid() && it.GetKey() == key);
            sum += it.GetValue();
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
    }
}

Y_CPU_BENCHMARK(TestRandomSearchSkipList, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        for (ui64 key : FixtureData.UniqueKeys) {
            auto it = FixtureData.List.SeekTo(key);
            Y_ABORT_UNLESS(it.IsValid() && it.GetValue().Key == key);
            sum += it.GetValue().Value;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
    }
}

Y_CPU_BENCHMARK(TestRandomSearchHashMap, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        for (ui64 key : FixtureData.UniqueKeys) {
            auto it = FixtureData.HashMap.find(key);
            Y_ABORT_UNLESS(it != FixtureData.HashMap.end());
            sum += it->second;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
    }
}

Y_CPU_BENCHMARK(TestRandomSearchMap, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        for (ui64 key : FixtureData.UniqueKeys) {
            auto it = FixtureData.Map.find(key);
            Y_ABORT_UNLESS(it != FixtureData.Map.end());
            sum += it->second;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(TestIterateBTree, iface) {
    auto it = FixtureData.TreeSafeAccess.Iterator();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        size_t count = 0;
        for (it.SeekFirst(); it.IsValid(); it.Next()) {
            sum += it.GetValue();
            ++count;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
        Y_ABORT_UNLESS(count == FixtureData.Tree.Size());
    }
}

Y_CPU_BENCHMARK(TestIterateCowBTree, iface) {
    auto it = FixtureData.CowSnapshot.Iterator();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        size_t count = 0;
        for (it.SeekFirst(); it.IsValid(); it.Next()) {
            sum += it.GetValue();
            ++count;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
        Y_ABORT_UNLESS(count == FixtureData.CowTree.Size());
    }
}

Y_CPU_BENCHMARK(TestIterateSkipList, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        size_t count = 0;
        for (auto it = FixtureData.List.SeekToFirst(); it.IsValid(); it.Next()) {
            sum += it.GetValue().Value;
            ++count;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
        Y_ABORT_UNLESS(count == FixtureData.List.GetSize());
    }
}

Y_CPU_BENCHMARK(TestIterateHashMap, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        size_t count = 0;
        for (auto it = FixtureData.HashMap.begin(); it != FixtureData.HashMap.end(); ++it) {
            sum += it->second;
            ++count;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
        Y_ABORT_UNLESS(count == FixtureData.HashMap.size());
    }
}

Y_CPU_BENCHMARK(TestIterateMap, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        ui64 sum = 0;
        size_t count = 0;
        for (auto it = FixtureData.Map.begin(); it != FixtureData.Map.end(); ++it) {
            sum += it->second;
            ++count;
        }
        Y_DO_NOT_OPTIMIZE_AWAY(sum);
        Y_ABORT_UNLESS(count == FixtureData.Map.size());
    }
}
