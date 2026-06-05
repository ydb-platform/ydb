#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

#include <yql/essentials/public/udf/arrow/block_item.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NYql::NUdf {
namespace {

class TTestTuple: public TBoxedValueBase {
public:
    explicit TTestTuple(TVector<TUnboxedValue> elements)
        : Elements_(std::move(elements))
    {
    }

    TUnboxedValue GetElement(ui32 index) const override {
        return Elements_[index];
    }

    const TUnboxedValue* GetElements() const override {
        return Elements_.data();
    }

private:
    TVector<TUnboxedValue> Elements_;
};

class TTestVariant: public TBoxedValueBase {
public:
    TTestVariant(ui32 index, TUnboxedValue value)
        : Index_(index)
        , Value_(std::move(value))
    {
    }

    ui32 GetVariantIndex() const override {
        return Index_;
    }

    TUnboxedValue GetVariantItem() const override {
        return Value_;
    }

private:
    ui32 Index_;
    TUnboxedValue Value_;
};

class TTestList: public TBoxedValueBase {
public:
    explicit TTestList(TVector<TUnboxedValue> elements)
        : Elements_(std::move(elements))
    {
    }

    bool HasFastListLength() const override {
        return true;
    }

    ui64 GetListLength() const override {
        return Elements_.size();
    }

    bool HasListItems() const override {
        return !Elements_.empty();
    }

    TUnboxedValue GetListIterator() const override {
        return TUnboxedValuePod(IBoxedValuePtr(new TIterator(Elements_)));
    }

private:
    class TIterator: public TBoxedValueBase {
    public:
        explicit TIterator(const TVector<TUnboxedValue>& elements)
            : Elements_(elements)
            , Index_(0)
        {
        }

        bool Next(TUnboxedValue& value) override {
            if (Index_ >= Elements_.size()) {
                return false;
            }
            value = Elements_[Index_++];
            return true;
        }

        bool Skip() override {
            if (Index_ >= Elements_.size()) {
                return false;
            }
            ++Index_;
            return true;
        }

    private:
        const TVector<TUnboxedValue>& Elements_;
        size_t Index_;
    };

    TVector<TUnboxedValue> Elements_;
};

class TTestStream: public TBoxedValueBase {
public:
    explicit TTestStream(TVector<TUnboxedValue> elements)
        : Elements_(std::move(elements))
    {
    }

    EFetchStatus Fetch(TUnboxedValue& value) override {
        if (Index_ >= Elements_.size()) {
            return EFetchStatus::Finish;
        }
        value = Elements_[Index_++];
        return EFetchStatus::Ok;
    }

private:
    TVector<TUnboxedValue> Elements_;
    size_t Index_ = 0;
};

TUnboxedValue MakeStream(TVector<TUnboxedValue> elements) {
    return TUnboxedValuePod(IBoxedValuePtr(new TTestStream(std::move(elements))));
}

TUnboxedValue MakeTuple(TVector<TUnboxedValue> elements) {
    return TUnboxedValuePod(IBoxedValuePtr(new TTestTuple(std::move(elements))));
}

TUnboxedValue MakeVariant(ui32 index, TUnboxedValue value) {
    return TUnboxedValuePod(IBoxedValuePtr(new TTestVariant(index, std::move(value))));
}

TUnboxedValue MakeList(TVector<TUnboxedValue> elements) {
    return TUnboxedValuePod(IBoxedValuePtr(new TTestList(std::move(elements))));
}

Y_UNIT_TEST_SUITE(TUdfValueComparatorTest) {

Y_UNIT_TEST(PrimitivesEqual) {
    AssertUnboxedValueElementEqual(TUnboxedValuePod(i64(42)), i64(42));
    AssertUnboxedValueElementEqual(TUnboxedValuePod(i32(-7)), i32(-7));
    AssertUnboxedValueElementEqual(TUnboxedValuePod(ui32(100U)), ui32(100U));
    AssertUnboxedValueElementEqual(TUnboxedValuePod(ui64(0U)), ui64(0U));
    AssertUnboxedValueElementEqual(TUnboxedValuePod(double(3.14)), double(3.14));
    AssertUnboxedValueElementEqual(TUnboxedValuePod(float(1.5F)), float(1.5F));
    AssertUnboxedValueElementEqual(TUnboxedValuePod(true), true);
    AssertUnboxedValueElementEqual(TUnboxedValuePod(false), false);
}

Y_UNIT_TEST(PrimitivesNotEqual) {
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValuePod(i64(42)), i64(43)));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValuePod(i32(0)), i32(1)));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValuePod(true), false));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValuePod(double(1.0)), double(2.0)));
}

Y_UNIT_TEST(StringEqual) {
    AssertUnboxedValueElementEqual(TUnboxedValue::Embedded("hello"), TString("hello"));
    AssertUnboxedValueElementEqual(TUnboxedValue::Embedded(""), TString(""));
    AssertUnboxedValueElementEqual(TUnboxedValue::Embedded("abc"), TString("abc"));
}

Y_UNIT_TEST(StringNotEqual) {
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValue::Embedded("hello"), TString("world")));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValue::Embedded("abc"), TString("ab")));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TUnboxedValue::Embedded(""), TString("x")));
}

Y_UNIT_TEST(MaybeNothing) {
    TUnboxedValuePod nothing;
    AssertUnboxedValueElementEqual(nothing, TMaybe<i32>{});
    AssertUnboxedValueElementEqual(nothing, TMaybe<TString>{});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(nothing, TMaybe<i32>{42}));
}

Y_UNIT_TEST(MaybeDefined) {
    TUnboxedValuePod opt = TUnboxedValuePod(i32(42)).MakeOptional();
    AssertUnboxedValueElementEqual(opt, TMaybe<i32>{42});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(opt, TMaybe<i32>{}));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(opt, TMaybe<i32>{43}));
}

Y_UNIT_TEST(MaybeDefinedString) {
    TUnboxedValue str = TUnboxedValue::Embedded("hi");
    TUnboxedValuePod opt = str.MakeOptional();
    AssertUnboxedValueElementEqual(opt, TMaybe<TString>{"hi"});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(opt, TMaybe<TString>{"bye"}));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(opt, TMaybe<TString>{}));
}

Y_UNIT_TEST(MaybeNested) {
    TUnboxedValuePod inner = TUnboxedValuePod(i32(7)).MakeOptional();
    TUnboxedValuePod outer = inner.MakeOptional();
    AssertUnboxedValueElementEqual(outer, TMaybe<TMaybe<i32>>{TMaybe<i32>{7}});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(outer, TMaybe<TMaybe<i32>>{TMaybe<i32>{}}));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(outer, TMaybe<TMaybe<i32>>{}));
}

Y_UNIT_TEST(MaybeNestedSecondLevel) {
    TUnboxedValuePod value = TUnboxedValuePod().MakeOptional();
    AssertUnboxedValueElementEqual(value, TMaybe<TMaybe<i32>>{TMaybe<i32>{}});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(value, TMaybe<TMaybe<i32>>{TMaybe<i32>{7}}));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(value, TMaybe<TMaybe<i32>>{}));
}

Y_UNIT_TEST(TupleEqual) {
    auto tuple = MakeTuple({TUnboxedValuePod(i32(1)), TUnboxedValue::Embedded("x")});
    AssertUnboxedValueElementEqual(tuple, std::tuple{i32(1), TString("x")});
}

Y_UNIT_TEST(TupleNotEqual) {
    auto tuple = MakeTuple({TUnboxedValuePod(i32(1)), TUnboxedValue::Embedded("x")});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(tuple, std::tuple{i32(2), TString("x")}));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(tuple, std::tuple{i32(1), TString("y")}));
}

Y_UNIT_TEST(TupleNested) {
    auto inner = MakeTuple({TUnboxedValuePod(i32(5)), TUnboxedValuePod(true)});
    auto outer = MakeTuple({inner, TUnboxedValue::Embedded("z")});
    using TInner = std::tuple<i32, bool>;
    using TOuter = std::tuple<TInner, TString>;
    AssertUnboxedValueElementEqual(outer, TOuter{TInner{i32(5), true}, TString("z")});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(outer, TOuter{TInner{i32(5), false}, TString("z")}));
}

Y_UNIT_TEST(TupleWithMaybe) {
    TUnboxedValuePod opt = TUnboxedValuePod(i64(99)).MakeOptional();
    auto tuple = MakeTuple({opt, TUnboxedValuePod(i32(1))});
    AssertUnboxedValueElementEqual(tuple, std::tuple{TMaybe<i64>{99}, i32(1)});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(tuple, std::tuple{TMaybe<i64>{}, i32(1)}));
}

Y_UNIT_TEST(VariantFirstAlternative) {
    auto var = MakeVariant(0, TUnboxedValuePod(i32(42)));
    using TVar = std::variant<i32, TString>;
    AssertUnboxedValueElementEqual(var, TVar{i32(42)});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(var, TVar{TString("x")}));
}

Y_UNIT_TEST(VariantSecondAlternative) {
    auto var = MakeVariant(1, TUnboxedValue::Embedded("hello"));
    using TVar = std::variant<i32, TString>;
    AssertUnboxedValueElementEqual(var, TVar{TString("hello")});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(var, TVar{i32(0)}));
}

Y_UNIT_TEST(VariantWrongIndex) {
    auto var = MakeVariant(0, TUnboxedValuePod(i32(42)));
    using TVar = std::variant<i32, TString>;
    UNIT_ASSERT(!IsUnboxedValueElementEqual(var, TVar{TString("42")}));
}

Y_UNIT_TEST(VariantThreeAlternatives) {
    auto var = MakeVariant(1, TUnboxedValuePod(double(3.14)));
    using TVar = std::variant<i32, double, TString>;
    AssertUnboxedValueElementEqual(var, TVar{double(3.14)});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(var, TVar{i32(0)}));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(var, TVar{TString("3.14")}));
}

Y_UNIT_TEST(VectorEqual) {
    auto list = MakeList({TUnboxedValuePod(i32(1)), TUnboxedValuePod(i32(2)), TUnboxedValuePod(i32(3))});
    AssertUnboxedValueElementEqual(list, TVector<i32>{1, 2, 3});
}

Y_UNIT_TEST(VectorNotEqual) {
    auto list = MakeList({TUnboxedValuePod(i32(1)), TUnboxedValuePod(i32(2))});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(list, TVector<i32>{1, 3}));
}

Y_UNIT_TEST(VectorTooShort) {
    auto list = MakeList({TUnboxedValuePod(i32(1))});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(list, TVector<i32>{1, 2}));
}

Y_UNIT_TEST(VectorTooLong) {
    auto list = MakeList({TUnboxedValuePod(i32(1)), TUnboxedValuePod(i32(2))});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(list, TVector<i32>{1}));
}

Y_UNIT_TEST(VectorEmpty) {
    auto list = MakeList({});
    AssertUnboxedValueElementEqual(list, TVector<i32>{});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(list, TVector<i32>{0}));
}

Y_UNIT_TEST(VectorOfStrings) {
    auto list = MakeList({TUnboxedValue::Embedded("foo"), TUnboxedValue::Embedded("bar")});
    AssertUnboxedValueElementEqual(list, TVector<TString>{"foo", "bar"});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(list, TVector<TString>{"foo", "baz"}));
}

Y_UNIT_TEST(VectorOfMaybe) {
    auto list = MakeList({TUnboxedValuePod(i32(5)).MakeOptional(), TUnboxedValuePod()});
    AssertUnboxedValueElementEqual(list, TVector<TMaybe<i32>>{5, {}});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(list, TVector<TMaybe<i32>>{{}, 5}));
}

Y_UNIT_TEST(BlockItemPrimitives) {
    AssertUnboxedValueElementEqual(TBlockItem(i64(42)), i64(42));
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TBlockItem(i64(42)), i64(43)));
    AssertUnboxedValueElementEqual(TBlockItem(ui32(7U)), ui32(7U));
    AssertUnboxedValueElementEqual(TBlockItem(true), true);
    UNIT_ASSERT(!IsUnboxedValueElementEqual(TBlockItem(false), true));
}

Y_UNIT_TEST(StreamViewEqual) {
    auto stream = MakeStream({TUnboxedValuePod(i32(1)), TUnboxedValuePod(i32(2)), TUnboxedValuePod(i32(3))});
    AssertUnboxedValueElementEqual(stream, TUnboxedValueComparatorStreamView<i32>({1, 2, 3}));
}

Y_UNIT_TEST(StreamViewNotEqual) {
    auto stream = MakeStream({TUnboxedValuePod(i32(1)), TUnboxedValuePod(i32(2))});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(stream, TUnboxedValueComparatorStreamView<i32>({1, 99})));
}

Y_UNIT_TEST(StreamViewTooShort) {
    auto stream = MakeStream({TUnboxedValuePod(i32(1))});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(stream, TUnboxedValueComparatorStreamView<i32>({1, 2})));
}

Y_UNIT_TEST(StreamViewTooLong) {
    auto stream = MakeStream({TUnboxedValuePod(i32(1)), TUnboxedValuePod(i32(2))});
    UNIT_ASSERT(!IsUnboxedValueElementEqual(stream, TUnboxedValueComparatorStreamView<i32>({1})));
}

Y_UNIT_TEST(StreamViewEmpty) {
    auto stream = MakeStream({});
    AssertUnboxedValueElementEqual(stream, TUnboxedValueComparatorStreamView<i32>({}));
}

Y_UNIT_TEST(StreamViewOfTuples) {
    using TRow = std::tuple<i32, TString>;
    auto stream = MakeStream({
        MakeTuple({TUnboxedValuePod(i32(7)), TUnboxedValue::Embedded("A")}),
        MakeTuple({TUnboxedValuePod(i32(1)), TUnboxedValue::Embedded("D")}),
    });
    TVector<TRow> expected = {TRow{i32(7), TString("A")}, TRow{i32(1), TString("D")}};
    AssertUnboxedValueElementEqual(stream, TUnboxedValueComparatorStreamView<TRow>(expected));
}

} // Y_UNIT_TEST_SUITE(TUdfValueComparatorTest)

} // namespace
} // namespace NYql::NUdf
