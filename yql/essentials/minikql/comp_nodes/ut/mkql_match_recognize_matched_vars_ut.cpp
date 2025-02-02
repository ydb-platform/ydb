#include "../mkql_match_recognize_matched_vars.h"
#include "../mkql_match_recognize_list.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

class TSimpleList {
public:
    using iterator = TUnboxedValueVector::const_iterator;

    [[nodiscard]] iterator Begin() const noexcept {
        return Rows.begin();
    }

    [[nodiscard]] iterator End() const noexcept {
        return Rows.end();
    }

    [[nodiscard]] size_t Size() const noexcept {
        return Rows.size();
    }

    [[nodiscard]] bool Empty() const noexcept {
        return Rows.empty();
    }

    [[nodiscard]] bool Contains(size_t i) const noexcept {
        return i < Size();
    }

    [[nodiscard]] NUdf::TUnboxedValue Get(size_t i) const {
        return Rows.at(i);
    }

    ///Range that includes starting and ending points
    ///Can not be empty
    class TRange {
    public:
        TRange()
        : FromIndex(Max())
        , ToIndex(Max())
        , NfaIndex_(Max())
        {
        }

        explicit TRange(ui64 index)
            : FromIndex(index)
            , ToIndex(index)
            , NfaIndex_(Max())
        {
        }

        TRange(ui64 from, ui64 to)
            : FromIndex(from)
            , ToIndex(to)
            , NfaIndex_(Max())
        {
            MKQL_ENSURE(FromIndex <= ToIndex, "Internal logic error");
        }

        bool IsValid() const {
            return FromIndex != Max<size_t>() && ToIndex != Max<size_t>();
        }

        size_t From() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return FromIndex;
        }

        size_t To() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return ToIndex;
        }

        [[nodiscard]] size_t NfaIndex() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return NfaIndex_;
        }

        size_t Size() const {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            return ToIndex - FromIndex + 1;
        }

        void Extend() {
            MKQL_ENSURE(IsValid(), "Internal logic error");
            ++ToIndex;
        }

    private:
        size_t FromIndex;
        size_t ToIndex;
        size_t NfaIndex_;
    };

    TRange Append(NUdf::TUnboxedValue&& value) {
        TRange result(Rows.size());
        Rows.push_back(std::move(value));
        return result;
    }

private:
    TUnboxedValueVector Rows;
};

Y_UNIT_TEST_SUITE(MatchRecognizeMatchedVarExtend) {
    using TRange = TSimpleList::TRange;
    using TMatchedVar = TMatchedVar<TRange>;
    using TMatchedVars = TMatchedVars<TRange>;

    Y_UNIT_TEST(MatchedRangeSingleton) {
        TScopedAlloc alloc(__LOCATION__);
        TRange r{10};
        UNIT_ASSERT_VALUES_EQUAL(10, r.From());
        UNIT_ASSERT_VALUES_EQUAL(10, r.To());
        r.Extend();
        UNIT_ASSERT_VALUES_EQUAL(10, r.From());
        UNIT_ASSERT_VALUES_EQUAL(11, r.To());
    }

    Y_UNIT_TEST(MatchedRange) {
        TScopedAlloc alloc(__LOCATION__);
        TRange r{10, 20};
        UNIT_ASSERT_VALUES_EQUAL(10, r.From());
        UNIT_ASSERT_VALUES_EQUAL(20, r.To());
        r.Extend();
        UNIT_ASSERT_VALUES_EQUAL(10, r.From());
        UNIT_ASSERT_VALUES_EQUAL(21, r.To());
    }

    Y_UNIT_TEST(MatchedVarEmpty) {
        TScopedAlloc alloc(__LOCATION__);
        TMatchedVar v{};
        Extend(v, TRange{10});
        UNIT_ASSERT_VALUES_EQUAL(1, v.size());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].From());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].To());
    }

    Y_UNIT_TEST(MatchedVarExtendSingletonContiguous) {
        TScopedAlloc alloc(__LOCATION__);
        TMatchedVar v{TRange{10}};
        Extend(v, TRange{11});
        UNIT_ASSERT_VALUES_EQUAL(1, v.size());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].From());
        UNIT_ASSERT_VALUES_EQUAL(11, v[0].To());
    }

    Y_UNIT_TEST(MatchedVarExtendSingletonWithGap) {
        TScopedAlloc alloc(__LOCATION__);
        TMatchedVar v{TRange{10}};
        Extend(v, TRange{20});
        UNIT_ASSERT_VALUES_EQUAL(2, v.size());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].From());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].To());
        UNIT_ASSERT_VALUES_EQUAL(20, v[1].From());
        UNIT_ASSERT_VALUES_EQUAL(20, v[1].To());
    }

    Y_UNIT_TEST(MatchedVarExtendContiguous) {
        TScopedAlloc alloc(__LOCATION__);
        TMatchedVar v{TRange{10, 20}, TRange{30, 40}};
        Extend(v, TRange{41});
        UNIT_ASSERT_VALUES_EQUAL(2, v.size());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].From());
        UNIT_ASSERT_VALUES_EQUAL(20, v[0].To());
        UNIT_ASSERT_VALUES_EQUAL(30, v[1].From());
        UNIT_ASSERT_VALUES_EQUAL(41, v[1].To());
    }

    Y_UNIT_TEST(MatchedVarExtendWithGap) {
        TScopedAlloc alloc(__LOCATION__);
        TMatchedVar v{TRange{10, 20}, TRange{30, 40}};
        Extend(v, TRange{50});
        UNIT_ASSERT_VALUES_EQUAL(3, v.size());
        UNIT_ASSERT_VALUES_EQUAL(10, v[0].From());
        UNIT_ASSERT_VALUES_EQUAL(20, v[0].To());
        UNIT_ASSERT_VALUES_EQUAL(30, v[1].From());
        UNIT_ASSERT_VALUES_EQUAL(40, v[1].To());
        UNIT_ASSERT_VALUES_EQUAL(50, v[2].From());
        UNIT_ASSERT_VALUES_EQUAL(50, v[2].To());
    }
}

Y_UNIT_TEST_SUITE(MatchRecognizeMatchedVarsToValue) {
    using TRange = TSimpleList::TRange;
    using TMatchedVar = TMatchedVar<TRange>;
    using TMatchedVars = TMatchedVars<TRange>;
    TMemoryUsageInfo memUsage("MatchedVars");

    Y_UNIT_TEST(MatchedRange) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        {
            TRange r{10, 20};
            const auto value = ToValue(holderFactory, r);
            const auto elems = value.GetElements();
            UNIT_ASSERT(elems);
            UNIT_ASSERT_VALUES_EQUAL(10, elems[0].Get<ui64>());
            UNIT_ASSERT_VALUES_EQUAL(20, elems[1].Get<ui64>());
        }
    }

    Y_UNIT_TEST(MatchedRangeListEmpty) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        {
            const auto value = ToValue(holderFactory, TMatchedVar{});
            UNIT_ASSERT(value);
            UNIT_ASSERT(!value.HasListItems());
            UNIT_ASSERT(value.HasFastListLength());
            UNIT_ASSERT_VALUES_EQUAL(0, value.GetListLength());
            const auto iter = value.GetListIterator();
            UNIT_ASSERT(iter);
            NUdf::TUnboxedValue noValue;
            UNIT_ASSERT(!iter.Next(noValue));
            UNIT_ASSERT(!noValue);
        }
    }

    Y_UNIT_TEST(MatchedRangeList) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        {
            const auto value = ToValue(holderFactory, TMatchedVar{
                TRange{10, 30},
                TRange{40, 45},

            });
            UNIT_ASSERT(value);
            UNIT_ASSERT(value.HasListItems());
            UNIT_ASSERT(value.HasFastListLength());
            UNIT_ASSERT_VALUES_EQUAL(2, value.GetListLength());
            const auto iter = value.GetListIterator();
            UNIT_ASSERT(iter);
            NUdf::TUnboxedValue elem;
            //[0]
            UNIT_ASSERT(iter.Next(elem));
            UNIT_ASSERT(elem);
            UNIT_ASSERT(elem.GetElements());
            UNIT_ASSERT_VALUES_EQUAL(30, elem.GetElements()[1].Get<ui64>());
            //[1]
            UNIT_ASSERT(iter.Next(elem));
            UNIT_ASSERT(elem);
            UNIT_ASSERT(elem.GetElements());
            UNIT_ASSERT_VALUES_EQUAL(40, elem.GetElements()[0].Get<ui64>());
            //
            UNIT_ASSERT(!iter.Next(elem));
        }
    }

    Y_UNIT_TEST(MatchedVars) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        {
            const auto value = ToValue(holderFactory, TMatchedVars {
                    {},
                    {TRange{20, 25}},
                    {TRange{10, 30}, TRange{40, 45}},
            });
            UNIT_ASSERT(value);
            const auto varElems = value.GetElements();
            UNIT_ASSERT(varElems);
            UNIT_ASSERT(varElems[0]);
            UNIT_ASSERT(varElems[1]);
            UNIT_ASSERT(varElems[2]);
            const auto lastVar = varElems[2];
            UNIT_ASSERT(lastVar.HasFastListLength());
            UNIT_ASSERT_VALUES_EQUAL(2, lastVar.GetListLength());
            const auto iter = lastVar.GetListIterator();
            UNIT_ASSERT(iter);
            NUdf::TUnboxedValue elem;
            //[0]
            UNIT_ASSERT(iter.Next(elem));
            UNIT_ASSERT(elem);
            UNIT_ASSERT(elem.GetElements());
            UNIT_ASSERT_VALUES_EQUAL(30, elem.GetElements()[1].Get<ui64>());
            //[1]
            UNIT_ASSERT(iter.Next(elem));
            UNIT_ASSERT(elem);
            UNIT_ASSERT(elem.GetElements());
            UNIT_ASSERT_VALUES_EQUAL(40, elem.GetElements()[0].Get<ui64>());
            //
            UNIT_ASSERT(!iter.Next(elem));
        }
    }
}

Y_UNIT_TEST_SUITE(MatchRecognizeMatchedVarsToValueByRef) {
    using TRange = TSimpleList::TRange;
    using TMatchedVar = TMatchedVar<TRange>;
    using TMatchedVars = TMatchedVars<TRange>;
    TMemoryUsageInfo memUsage("MatchedVarsByRef");

    Y_UNIT_TEST(MatchedVarsEmpty) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        {
            TMatchedVars vars{};
            NUdf::TUnboxedValue value = holderFactory.Create<TMatchedVarsValue<TRange>>(holderFactory, vars);
            UNIT_ASSERT(value.HasValue());
        }
    }

    Y_UNIT_TEST(MatchedVars) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        {
            TMatchedVar A{{1, 4}, {7, 9}, {100, 200}};
            TMatchedVar B{{1, 6}};
            TMatchedVars vars{A, B};
            NUdf::TUnboxedValue value = holderFactory.Create<TMatchedVarsValue<TRange>>(holderFactory, vars);
            Y_UNUSED(value);
            UNIT_ASSERT(value.HasValue());
            auto a = value.GetElement(0);
            UNIT_ASSERT(a.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(3, a.GetListLength());
            auto iter = a.GetListIterator();
            UNIT_ASSERT(iter.HasValue());
            NUdf::TUnboxedValue last;
            while (iter.Next(last))
                ;
            UNIT_ASSERT(last.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(100, last.GetElement(0).Get<ui64>());
            UNIT_ASSERT_VALUES_EQUAL(200, last.GetElement(1).Get<ui64>());
            auto b = value.GetElement(1);
            UNIT_ASSERT(b.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, b.GetListLength());
        }
    }
}
} // namespace NKikimr::NMiniKQL::NMatchRecognize
