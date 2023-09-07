#include "udf_value.h"
#include "udf_ut_helpers.h"

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TUdfValue) {

    Y_UNIT_TEST(TestOptional) {
        TUnboxedValuePod foo((ui32) 42);
        UNIT_ASSERT(foo);
        UNIT_ASSERT(42 == foo.Get<ui32>());

        auto optFoo = foo.MakeOptional();
        UNIT_ASSERT(optFoo);
        UNIT_ASSERT(optFoo.HasValue());

        auto bar = optFoo.GetOptionalValue();
        UNIT_ASSERT(42 == bar.Get<ui32>());
    }

    Y_UNIT_TEST(TestOptional2) {
        auto valueOpt = TUnboxedValuePod((ui32) 42);
        UNIT_ASSERT(valueOpt);
        UNIT_ASSERT(valueOpt.HasValue());

        auto value = valueOpt.GetOptionalValue();
        UNIT_ASSERT(42 == value.Get<ui32>());
    }

    Y_UNIT_TEST(TestEmptyOptional) {
        auto optEmpty = TUnboxedValuePod();
        UNIT_ASSERT(!optEmpty);
        UNIT_ASSERT(!optEmpty.HasValue());

        auto optOptEmpty = optEmpty.MakeOptional();
        UNIT_ASSERT(optOptEmpty);
        UNIT_ASSERT(!optOptEmpty.HasValue());

        auto optOptOptEmpty = optOptEmpty.MakeOptional();
        UNIT_ASSERT(optOptOptEmpty);
        UNIT_ASSERT(!optOptOptEmpty.HasValue());

        auto v = optOptEmpty.GetOptionalValue();
        UNIT_ASSERT(!v);
    }

    Y_UNIT_TEST(TestVariant) {
        TUnboxedValuePod foo((ui64) 42);
        UNIT_ASSERT(foo);

        UNIT_ASSERT(!foo.TryMakeVariant(63));

        UNIT_ASSERT(foo.TryMakeVariant(62));

        UNIT_ASSERT(!foo.TryMakeVariant(0));

        UNIT_ASSERT(62 == foo.GetVariantIndex());
        UNIT_ASSERT(42 == foo.Get<ui64>());
    }

    Y_UNIT_TEST(TestEmptyInVariant) {
        TUnboxedValuePod foo;
        UNIT_ASSERT(!foo);
        UNIT_ASSERT(!foo.HasValue());

        UNIT_ASSERT(foo.TryMakeVariant(0));
        UNIT_ASSERT(foo);
        UNIT_ASSERT(!foo.HasValue());

        UNIT_ASSERT(0 == foo.GetVariantIndex());

        const auto opt = foo.MakeOptional();
        UNIT_ASSERT(!std::memcmp(&opt, &foo, sizeof(opt)));

        const auto bar = opt.GetOptionalValue();
        UNIT_ASSERT(!std::memcmp(&opt, &bar, sizeof(bar)));
    }

    Y_UNIT_TEST(TestInvalid) {
        TUnboxedValuePod foo;
        UNIT_ASSERT(!foo.IsInvalid());

        UNIT_ASSERT(!TUnboxedValuePod::Void().IsInvalid());
        UNIT_ASSERT(!TUnboxedValuePod::Zero().IsInvalid());
        UNIT_ASSERT(!TUnboxedValuePod::Embedded(TStringRef("abc")).IsInvalid());

        auto bad = TUnboxedValuePod::Invalid();
        UNIT_ASSERT(bad.IsInvalid());
    }

    Y_UNIT_TEST(TestDump) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuilder() << TUnboxedValuePod(), "Empty, count: 0");
        UNIT_ASSERT_STRINGS_EQUAL((TStringBuilder() << TUnboxedValuePod().MakeOptional()), "Empty, count: 1");
        UNIT_ASSERT_STRINGS_EQUAL((TStringBuilder() << TUnboxedValuePod::Invalid()), "Empty, count: -1");
        UNIT_ASSERT_STRINGS_EQUAL((TStringBuilder() << TUnboxedValuePod::Void()),
            "Embedded, size: 0, buffer: \"\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\"");
        UNIT_ASSERT_STRINGS_EQUAL((TStringBuilder() << TUnboxedValuePod::Embedded("foo")),
            "Embedded, size: 3, buffer: \"foo\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\"");
        UNIT_ASSERT_STRINGS_EQUAL((TStringBuilder() << TUnboxedValuePod(ui32(258))),
            "Embedded, size: 0, buffer: \"\\2\\1\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\"");
        TString pattern = "VERY VERY LONG STRING";
        TStringValue str(pattern.size());
        memcpy(str.Data(), pattern.data(), pattern.size());
        TUnboxedValue strVal(TUnboxedValuePod(std::move(str)));
        UNIT_ASSERT_STRINGS_EQUAL((TStringBuilder() << strVal),
            "String, size: 21, offset: 0, buffer: \"VERY VERY LONG STRING\"");

        TString objStr = (TStringBuilder() << TUnboxedValue(TUnboxedValuePod(IBoxedValuePtr(new TBoxedValue()))));
        UNIT_ASSERT(objStr.StartsWith("Boxed, pointer:"));
    }

    Y_UNIT_TEST(LockMethodsTable) {
#define METHOD_INDEX(name) NYql::GetMethodPtrIndex(TBoxedValueAccessor::GetMethodPtr(TBoxedValueAccessor::EMethod::name))
        UNIT_ASSERT_VALUES_EQUAL(2, METHOD_INDEX(HasFastListLength));
        UNIT_ASSERT_VALUES_EQUAL(3, METHOD_INDEX(GetListLength));
        UNIT_ASSERT_VALUES_EQUAL(4, METHOD_INDEX(GetEstimatedListLength));
        UNIT_ASSERT_VALUES_EQUAL(5, METHOD_INDEX(GetListIterator));
        UNIT_ASSERT_VALUES_EQUAL(6, METHOD_INDEX(GetListRepresentation));
        UNIT_ASSERT_VALUES_EQUAL(7, METHOD_INDEX(ReverseListImpl));
        UNIT_ASSERT_VALUES_EQUAL(8, METHOD_INDEX(SkipListImpl));
        UNIT_ASSERT_VALUES_EQUAL(9, METHOD_INDEX(TakeListImpl));
        UNIT_ASSERT_VALUES_EQUAL(10, METHOD_INDEX(ToIndexDictImpl));
        UNIT_ASSERT_VALUES_EQUAL(11, METHOD_INDEX(GetDictLength));
        UNIT_ASSERT_VALUES_EQUAL(12, METHOD_INDEX(GetDictIterator));
        UNIT_ASSERT_VALUES_EQUAL(13, METHOD_INDEX(GetKeysIterator));
        UNIT_ASSERT_VALUES_EQUAL(14, METHOD_INDEX(GetPayloadsIterator));
        UNIT_ASSERT_VALUES_EQUAL(15, METHOD_INDEX(Contains));
        UNIT_ASSERT_VALUES_EQUAL(16, METHOD_INDEX(Lookup));
        UNIT_ASSERT_VALUES_EQUAL(17, METHOD_INDEX(GetElement));
        UNIT_ASSERT_VALUES_EQUAL(18, METHOD_INDEX(GetElements));
        UNIT_ASSERT_VALUES_EQUAL(19, METHOD_INDEX(Run));
        UNIT_ASSERT_VALUES_EQUAL(20, METHOD_INDEX(GetResourceTag));
        UNIT_ASSERT_VALUES_EQUAL(21, METHOD_INDEX(GetResource));
        UNIT_ASSERT_VALUES_EQUAL(22, METHOD_INDEX(HasListItems));
        UNIT_ASSERT_VALUES_EQUAL(23, METHOD_INDEX(HasDictItems));
        UNIT_ASSERT_VALUES_EQUAL(24, METHOD_INDEX(GetVariantIndex));
        UNIT_ASSERT_VALUES_EQUAL(25, METHOD_INDEX(GetVariantItem));
        UNIT_ASSERT_VALUES_EQUAL(26, METHOD_INDEX(Fetch));
        UNIT_ASSERT_VALUES_EQUAL(27, METHOD_INDEX(Skip));
        UNIT_ASSERT_VALUES_EQUAL(28, METHOD_INDEX(Next));
        UNIT_ASSERT_VALUES_EQUAL(29, METHOD_INDEX(NextPair));
        UNIT_ASSERT_VALUES_EQUAL(30, METHOD_INDEX(Apply));
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
        UNIT_ASSERT_VALUES_EQUAL(31, METHOD_INDEX(GetTraverseCount));
        UNIT_ASSERT_VALUES_EQUAL(32, METHOD_INDEX(GetTraverseItem));
        UNIT_ASSERT_VALUES_EQUAL(33, METHOD_INDEX(Save));
        UNIT_ASSERT_VALUES_EQUAL(34, METHOD_INDEX(Load));
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
        UNIT_ASSERT_VALUES_EQUAL(35, METHOD_INDEX(Push));
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
        UNIT_ASSERT_VALUES_EQUAL(36, METHOD_INDEX(IsSortedDict));
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
        UNIT_ASSERT_VALUES_EQUAL(37, METHOD_INDEX(Unused1));
        UNIT_ASSERT_VALUES_EQUAL(38, METHOD_INDEX(Unused2));
        UNIT_ASSERT_VALUES_EQUAL(39, METHOD_INDEX(Unused3));
        UNIT_ASSERT_VALUES_EQUAL(40, METHOD_INDEX(Unused4));
        UNIT_ASSERT_VALUES_EQUAL(41, METHOD_INDEX(Unused5));
        UNIT_ASSERT_VALUES_EQUAL(42, METHOD_INDEX(Unused6));
#endif

#undef METHOD_INDEX
    }
}
