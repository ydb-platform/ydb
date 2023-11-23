#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller_adapter.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>
#include <utility>
#include <algorithm>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TestWideSpillerAdapter) {
    constexpr size_t itemWidth = 3;
    constexpr size_t chunkSize = 100;
    Y_UNIT_TEST(TestWriteExtractZeroItems) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        const auto spiller = MakeSpiller();
        std::vector<TType*> itemTypes(itemWidth, TDataType::Create(NUdf::TDataType<char*>::Id, env));
        TWideUnboxedValuesSpillerAdapter wideUVSpiller(spiller, TMultiType::Create(itemWidth, itemTypes.data(), env), chunkSize);
        auto r = wideUVSpiller.FinishWriting();
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT(wideUVSpiller.Empty());
    }

    Y_UNIT_TEST(TestWriteExtract) {
        TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("test");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        TTypeEnvironment env(alloc);
        const auto spiller = MakeSpiller();
        std::vector<TType*> itemTypes(itemWidth, TDataType::Create(NUdf::TDataType<char*>::Id, env));
        TWideUnboxedValuesSpillerAdapter wideUVSpiller(spiller, TMultiType::Create(itemWidth, itemTypes.data(), env), chunkSize);
        std::vector<NUdf::TUnboxedValue> wideValue(itemWidth);
        constexpr size_t rowCount = chunkSize*10+3;
        for (size_t row = 0; row != rowCount; ++row) {
            for(size_t i = 0; i != itemWidth; ++i) {
                wideValue[i] = NUdf::TUnboxedValuePod(NUdf::TStringValue(TStringBuilder() << "Long enough string: " << row * 10 + i));
            }
            if (auto r = wideUVSpiller.WriteWideItem(wideValue)) {
                wideUVSpiller.AsyncWriteCompleted(r->GetValue());
            }
        }
        auto r = wideUVSpiller.FinishWriting();
        if (r) {
            wideUVSpiller.AsyncWriteCompleted(r->GetValue());
        }

        wideUVSpiller.AsyncWriteCompleted(r->GetValue());
        for (size_t row = 0; row != rowCount; ++row) {
            UNIT_ASSERT(!wideUVSpiller.Empty());
            if (auto r = wideUVSpiller.ExtractWideItem(wideValue)) {
                wideUVSpiller.AsyncReadCompleted(r->ExtractValue(), holderFactory);
                r = wideUVSpiller.ExtractWideItem(wideValue);
                UNIT_ASSERT(!r.has_value());
            }
            for (size_t i = 0; i != itemWidth; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(TStringBuf(wideValue[i].AsStringRef()) , TStringBuilder() << "Long enough string: " << row * 10 + i);
            }
        }
        UNIT_ASSERT(!wideUVSpiller.Empty());
    }
}

} //namespace NKikimr::NMiniKQL
