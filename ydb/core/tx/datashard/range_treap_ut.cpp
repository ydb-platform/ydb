#include "range_treap.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/list.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NDataShard {

namespace {

    TVector<NScheme::TTypeInfo> CreateSchema(size_t n) {
        TVector<NScheme::TTypeInfo> schema;
        auto type = NScheme::TTypeInfo(NScheme::NTypeIds::Uint64);
        for (size_t i = 0; i < n; ++i) {
            schema.emplace_back(type);
        }
        return schema;
    }

#if 0
    TVector<TCell> CreateKey(std::initializer_list<ui64> keys) {
        TVector<TCell> cells(Reserve(keys.size()));
        for (ui64 key : keys) {
            cells.emplace_back(TCell::Make(key));
        }
        return cells;
    }
#endif

    TVector<TCell> CreateKey(ui64 key) {
        TVector<TCell> cells(Reserve(1));
        cells.emplace_back(TCell::Make(key));
        return cells;
    }

    void PrintKey(TStringBuilder& builder, TConstArrayRef<TCell> key, size_t columns) {
        if (columns != 1) {
            builder << '{';
        }
        for (size_t i = 0; i < columns; ++i) {
            if (i > 0) {
                builder << ',';
            }
            if (i < key.size()) {
                builder << key[i].AsValue<ui64>();
            } else {
                builder << "any";
            }
        }
        if (columns != 1) {
            builder << '}';
        }
    }

    void PrintRange(TStringBuilder& builder, const TRangeTreeBase::TRange& range, ui64 value, size_t columns) {
        builder << (range.LeftInclusive ? '[' : '(');
        PrintKey(builder, range.LeftKey, columns);
        builder << ", ";
        PrintKey(builder, range.RightKey, columns);
        builder << (range.RightInclusive ? ']' : ')');
        builder << " -> ";
        builder << value;
    }

    class TRangesToString : public TStringBuilder {
    public:
        TRangesToString(TStringBuilder& builder, size_t columns)
            : Builder(builder)
            , Columns(columns)
        { }

        void operator()(const TRangeTreeBase::TRange& range, ui64 value) {
            if (Index++) {
                Builder << ',';
                Builder << ' ';
            }
            PrintRange(Builder, range, value, Columns);
        }

    private:
        TStringBuilder& Builder;
        const size_t Columns;
        size_t Index = 0;
    };

    TString TreapToString(const TRangeTreap<ui64>& treap) {
        TStringBuilder builder;
        treap.EachRange(TRangesToString(builder, treap.KeyColumns()));
        return builder;
    }

    template<class TNeedle>
    TString IntersectionToString(const TRangeTreap<ui64>& treap, const TNeedle& needle) {
        TStringBuilder builder;
        treap.EachIntersection(needle, TRangesToString(builder, treap.KeyColumns()));
        return builder;
    }

    struct TCheckValue {
        ui64 Left, Right, Value;

        TCheckValue(ui64 left, ui64 right, ui64 value)
            : Left(left)
            , Right(right)
            , Value(value)
        { }

        TString ToString() const {
            return TStringBuilder() << *this;
        }

        friend inline bool operator==(const TCheckValue& a, const TCheckValue& b) {
            return a.Left == b.Left && a.Right == b.Right && a.Value == b.Value;
        }

        friend inline IOutputStream& operator<<(IOutputStream& out, const TCheckValue& check) {
            out << '[';
            out << check.Left;
            out << ", ";
            out << check.Right;
            out << "] -> ";
            out << check.Value;
            return out;
        }
    };

} // namespace

Y_UNIT_TEST_SUITE(TRangeTreap) {

    Y_UNIT_TEST(Simple) {
        using TRange = TRangeTreeBase::TRange;
        TRangeTreap<ui64> treap;
        treap.SetKeyTypes(CreateSchema(1));

        treap.AddRange(TRange(CreateKey(1), true, CreateKey(10), true), 42);
        treap.AddRange(TRange(CreateKey(2), true, CreateKey(20), true), 43);
        treap.AddRange(TRange(CreateKey(3), true, CreateKey(30), true), 44);
        treap.Validate();
        UNIT_ASSERT_VALUES_EQUAL(TreapToString(treap), "[1, 10] -> 42, [2, 20] -> 43, [3, 30] -> 44");
        UNIT_ASSERT_VALUES_EQUAL(treap.Size(), 3u);

        treap.AddRange(TRange(CreateKey(2), true, CreateKey(40), true), 43);
        treap.Validate();
        UNIT_ASSERT_VALUES_EQUAL(TreapToString(treap), "[1, 10] -> 42, [2, 40] -> 43, [3, 30] -> 44");
        UNIT_ASSERT_VALUES_EQUAL(treap.Size(), 3u);

        UNIT_ASSERT_VALUES_EQUAL(
            IntersectionToString(treap, CreateKey(1)),
            "[1, 10] -> 42");

        UNIT_ASSERT_VALUES_EQUAL(
            IntersectionToString(treap, CreateKey(2)),
            "[1, 10] -> 42, [2, 40] -> 43");

        UNIT_ASSERT_VALUES_EQUAL(
            IntersectionToString(treap, CreateKey(3)),
            "[1, 10] -> 42, [2, 40] -> 43, [3, 30] -> 44");

        UNIT_ASSERT_VALUES_EQUAL(
            IntersectionToString(treap, CreateKey(15)),
            "[2, 40] -> 43, [3, 30] -> 44");

        UNIT_ASSERT_VALUES_EQUAL(
            IntersectionToString(treap, CreateKey(35)),
            "[2, 40] -> 43");

        UNIT_ASSERT_VALUES_EQUAL(
            IntersectionToString(treap, CreateKey(45)),
            "");

        treap.RemoveRanges(43);
        treap.Validate();
        UNIT_ASSERT_VALUES_EQUAL(TreapToString(treap), "[1, 10] -> 42, [3, 30] -> 44");
        UNIT_ASSERT_VALUES_EQUAL(treap.Size(), 2u);

        treap.RemoveRanges(42);
        treap.RemoveRanges(44);
        UNIT_ASSERT_VALUES_EQUAL(TreapToString(treap), "");
        UNIT_ASSERT_VALUES_EQUAL(treap.Size(), 0u);
    }

    Y_UNIT_TEST(Sequential) {
        using TRange = TRangeTreeBase::TRange;
        TRangeTreap<ui64> treap;
        treap.SetKeyTypes(CreateSchema(1));

        const size_t nRanges = 1000000;
        for (size_t i = 0; i < nRanges; ++i) {
            ui64 left = i + 1;
            ui64 right = i + 1;
            ui64 value = i + 1;
            treap.AddRange(TRange(CreateKey(left), true, CreateKey(right), true), value);
        }
        treap.Validate();

        auto buildStats = treap.Stats();
        Cerr << "NOTE: building treap of size " << treap.Size()
            << " got height " << treap.Height() << " and needed "
            << (buildStats.Inserts + buildStats.Updates + buildStats.Deletes) << " ops ("
            << buildStats.Inserts << " inserts "
            << buildStats.Updates << " updates "
            << buildStats.Deletes << " deletes) and "
            << buildStats.Comparisons << " comparisons ("
            << double(buildStats.Comparisons) / double(buildStats.Inserts + buildStats.Updates + buildStats.Deletes)
            << " per op)"
            << Endl;
    }

    Y_UNIT_TEST(Random) {
        using TRange = TRangeTreeBase::TRange;
        TRangeTreap<ui64> treap;
        treap.SetKeyTypes(CreateSchema(1));

        using TCheckMap = TMap<std::pair<ui64, ui64>, ui64>;
        TCheckMap map; // (left, value) -> right
        using TCheckValues = THashMap<ui64, TList<TCheckMap::iterator>>;
        TCheckValues values;

        const ui64 nValues = 20;
#if 1
        const size_t nRanges = 10000;
        const ui64 totalRangeSize = 10000;
        const ui64 singleRangeMinSize = 100;
        const ui64 singleRangeMaxSize = 5000;
#else
        const size_t nRanges = 1000000;
        const ui64 totalRangeSize = 1000000;
        const ui64 singleRangeMinSize = 1;
        const ui64 singleRangeMaxSize = 1;
#endif

        // Insert a bunch of values
        for (size_t i = 0; i < nRanges; ++i) {
            ui64 left = 1 + (RandomNumber<ui64>() % totalRangeSize);
            ui64 size = singleRangeMinSize + (RandomNumber<ui64>() % (singleRangeMaxSize - singleRangeMinSize + 1));
            ui64 right = Min(left + size - 1, ui64(totalRangeSize));
            ui64 value = 1 + (RandomNumber<ui64>() % nValues);
            treap.AddRange(TRange(CreateKey(left), true, CreateKey(right), true), value);
            // Add it to the classical map too
            auto key = std::make_pair(left, value);
            auto it = map.find(key);
            if (it == map.end()) {
                it = map.emplace(key, right).first;
                values[value].emplace_back(it);
            } else {
                it->second = Max(it->second, right);
            }
        }

        // Remove some values with 10% probability
        for (ui64 value = 1; value <= nValues; ++value) {
            ui64 prio = RandomNumber<ui64>() % 10;
            if (prio == 0) {
                treap.RemoveRanges(value);
                // Remove it from the classical map too
                for (auto it : values[value]) {
                    map.erase(it);
                }
                values.erase(value);
            }
        }

        auto buildStats = treap.Stats();
        Cerr << "NOTE: building treap of size " << treap.Size()
            << " got height " << treap.Height() << " and needed "
            << (buildStats.Inserts + buildStats.Updates + buildStats.Deletes) << " ops ("
            << buildStats.Inserts << " inserts "
            << buildStats.Updates << " updates "
            << buildStats.Deletes << " deletes) and "
            << buildStats.Comparisons << " comparisons ("
            << double(buildStats.Comparisons) / double(buildStats.Inserts + buildStats.Updates + buildStats.Deletes)
            << " per op)"
            << Endl;

        // The resulting treap must be valid
        treap.Validate();

        // The resulting treap and shadow map must have the same size
        UNIT_ASSERT_VALUES_EQUAL(treap.Size(), map.size());

        auto checkIt = map.begin();
        treap.EachRange([&](const TRange& range, ui64 value) {
            TCheckValue found{ range.LeftKey[0].AsValue<ui64>(), range.RightKey[0].AsValue<ui64>(), value };
            UNIT_ASSERT_C(checkIt != map.end(), "Treap has more values than the map, e.g.: " << found);
            TCheckValue expected{ checkIt->first.first, checkIt->second, checkIt->first.second };
            UNIT_ASSERT_VALUES_EQUAL(found, expected);
            ++checkIt;
        });
        UNIT_ASSERT_C(checkIt == map.end(), "Map has more values than the treap");

        // Let's find some intersections and verify them with brute force
        for (size_t i = 0; i < 10; ++i) {
            ui64 point = 1 + (RandomNumber<ui64>() % totalRangeSize);
            Cerr << "Checking point " << point << Endl;
            auto checkIt = map.begin();
            treap.ResetStats();
            size_t foundCount = 0;
            treap.EachIntersection(CreateKey(point), [&](const TRange& range, ui64 value) {
                TCheckValue found{ range.LeftKey[0].AsValue<ui64>(), range.RightKey[0].AsValue<ui64>(), value };
                // Skip all map values that don't intersect with point
                while (checkIt != map.end() && !(checkIt->first.first <= point && point <= checkIt->second)) {
                    ++checkIt;
                }
                UNIT_ASSERT_C(checkIt != map.end(), "Treap returned value that was not found in the map, e.g." << found);
                TCheckValue expected{ checkIt->first.first, checkIt->second, checkIt->first.second };
                UNIT_ASSERT_VALUES_EQUAL_C(found, expected, "Treap returned a value that does not match with the map");
                ++checkIt;
                ++foundCount;
            });
            auto foundStats = treap.Stats();
            // Check if there is any other matching value
            while (checkIt != map.end() && !(checkIt->first.first <= point && point <= checkIt->second)) {
                ++checkIt;
            }
            UNIT_ASSERT_C(checkIt == map.end(), "Map has a value that was not returned from the treap, e.g."
                    << TCheckValue(checkIt->first.first, checkIt->second, checkIt->first.second));
            Cerr << "... found " << foundCount << " ranges, needed "
                << foundStats.Comparisons << " comparisons ("
                << double(foundStats.Comparisons) / double(Max(foundCount, ui64(1)))
                << " per range)"
                << Endl;
        }
    }

}

} // namespace NDataShard
} // namespace NKikimr
