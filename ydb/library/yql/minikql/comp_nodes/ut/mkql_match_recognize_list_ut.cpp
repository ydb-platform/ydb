#include "../mkql_match_recognize_list.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

template<class L>
void CommonForSimpleAndSparse(const THolderFactory& holderFactory) {
    using TList = L;
    using TRange = typename L::TRange;
    TList list;
    TRange r;
    for (ui64 i = 0; i != 10; ++i) {
        r = list.Append(NUdf::TUnboxedValuePod{i});
        UNIT_ASSERT_VALUES_EQUAL(1, r.Size());
        NUdf::TUnboxedValue v = list.Get(i);
        UNIT_ASSERT_VALUES_EQUAL(i, v.Get<ui64>());
    }
    UNIT_ASSERT_VALUES_EQUAL(10, list.Size());
    {
        auto r2 = list.Append(NUdf::TUnboxedValuePod{10});
        Y_UNUSED(r2);
        r.Extend();
    }
    UNIT_ASSERT_VALUES_EQUAL(11, list.Size());
    {
        const NUdf::TUnboxedValue& v = list.Get(10);
        UNIT_ASSERT_VALUES_EQUAL(10, v.Get<ui64>());
    }
    //Test access via value
    const NUdf::TUnboxedValue& listValue = holderFactory.Create<TListValue<L>>(list);
    UNIT_ASSERT(listValue);
    UNIT_ASSERT(listValue.HasValue());
    UNIT_ASSERT(listValue.HasListItems());
    UNIT_ASSERT(listValue.HasFastListLength());
    UNIT_ASSERT_VALUES_EQUAL(11, listValue.GetListLength());
    TDefaultValueBuilder valueBuilder(holderFactory);
    auto listValueAsDict = NUdf::TBoxedValueAccessor::ToIndexDictImpl(*listValue.AsBoxed(), TDefaultValueBuilder(holderFactory));
    {
        const NUdf::TUnboxedValue &v = NUdf::TBoxedValueAccessor::Lookup(*listValueAsDict, NUdf::TUnboxedValuePod{9});
        UNIT_ASSERT_VALUES_EQUAL(9, v.Get<ui64>());
    }
    {
        const NUdf::TUnboxedValue &v = NUdf::TBoxedValueAccessor::Lookup(*listValueAsDict, NUdf::TUnboxedValuePod{10});
        UNIT_ASSERT_VALUES_EQUAL(10, v.Get<ui64>());
    }
}

Y_UNIT_TEST_SUITE(MatchRecognizeList) {
    TMemoryUsageInfo memUsage("MatchRecognizeListTest");
    Y_UNIT_TEST(SimpleListCommon) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        CommonForSimpleAndSparse<TSimpleList>(holderFactory);
    }
    Y_UNIT_TEST(SparseListCommon) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        CommonForSimpleAndSparse<TSparseList>(holderFactory);
    }
    Y_UNIT_TEST(SimpleListSpecific) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        TSimpleList list;
        for (ui64 i = 0; i != 10; ++i) {
            list.Append(NUdf::TUnboxedValuePod{i});
        }
        //All added items are accessible regardless of held ranges(locks)
        for (ui64 i = 0; i != 10; ++i) {
            NUdf::TUnboxedValue v = list.Get(i);
            UNIT_ASSERT_VALUES_EQUAL(i, v.Get<ui64>());
        }
    }
    Y_UNIT_TEST(SparseListSpecific) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        TSparseList list;
        //Add 10 items
        for (ui64 i = 0; i != 10; ++i) {
            list.Append(NUdf::TUnboxedValuePod{i});
        }
        //Check no one is stored
        UNIT_ASSERT_VALUES_EQUAL(0, list.Filled());
        for (ui64 i = 0; i != 10; ++i) {
            NUdf::TUnboxedValue v = list.Get(i);
            UNIT_ASSERT(!v);
        }
        //Add another 10 items and lock the last item added at every iteration
        TSparseList::TRange r;
        for (ui64 i = 10; i != 20; ++i) {
            r = list.Append(NUdf::TUnboxedValuePod{i});
        }
        //Check that only the last is stored
        UNIT_ASSERT_VALUES_EQUAL(1, list.Filled());
        for (ui64 i = 0; i != 19; ++i) {
            NUdf::TUnboxedValue v = list.Get(i);
            UNIT_ASSERT(!v);
        }
        {
            NUdf::TUnboxedValue v = list.Get(19);
            UNIT_ASSERT_VALUES_EQUAL(19, v.Get<ui64>());
        }

        //Test copy and assignment for locks
        TSparseList::TRange copiedRange{r};
        TSparseList::TRange assignedRange{r};
        assignedRange = copiedRange;
        UNIT_ASSERT_VALUES_EQUAL(1, list.Filled());
        {
            NUdf::TUnboxedValue v = list.Get(19);
            UNIT_ASSERT_VALUES_EQUAL(19, v.Get<ui64>());
        }
        r.Release();
        UNIT_ASSERT_VALUES_EQUAL(1, list.Filled());
        {
            NUdf::TUnboxedValue v = list.Get(19);
            UNIT_ASSERT_VALUES_EQUAL(19, v.Get<ui64>());
        }
        UNIT_ASSERT_VALUES_EQUAL(1, list.Filled());
        copiedRange.Release();
        UNIT_ASSERT_VALUES_EQUAL(1, list.Filled());
        {
            NUdf::TUnboxedValue v = list.Get(19);
            UNIT_ASSERT_VALUES_EQUAL(19, v.Get<ui64>());
        }
        assignedRange.Release();
        UNIT_ASSERT_VALUES_EQUAL(0, list.Filled());
        {
            NUdf::TUnboxedValue v = list.Get(19);
            UNIT_ASSERT(!v);
        }
    }
}

}//namespace NKikimr::NMiniKQL::TMatchRecognize
