#include "block_layout_converter.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

using namespace NYql::NUdf;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

struct TBlockLayoutConverterTestData {
    TBlockLayoutConverterTestData()
        : FunctionRegistry(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , PgmBuilder(Env, *FunctionRegistry)
        , MemInfo("Memory")
        , ArrowPool(GetYqlMemoryPool())
    {
    }

    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment Env;
    NMiniKQL::TProgramBuilder PgmBuilder;
    NMiniKQL::TMemoryUsageInfo MemInfo;
    arrow::MemoryPool* const ArrowPool;
};

Y_UNIT_TEST_SUITE(TBlockLayoutConverterTest) {
    Y_UNIT_TEST(TestFixedSize) {
        TBlockLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector< NKikimr::NMiniKQL::TType*> types{int64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(int64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = NMiniKQL::MaxBlockSizeInBytes / sizeof(i64);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            builder->Add(TBlockItem(i));
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        IBlockLayoutConverter::PackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), 1, "Expected the one column after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        const auto& columnBefore = columns.front().array();
        const auto& columnAfter = columnsAfterConversion.front().array();
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), int64Type);

        for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
            TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
            TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after conversion");
        }
    }

    Y_UNIT_TEST(TestString) {
        TBlockLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector< NKikimr::NMiniKQL::TType*> types{stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(stringType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        // To fit all strings into single block
        constexpr auto testSize = 512;

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), stringType, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        std::string testString;
        testString.resize(testSize);
        for (size_t i = 0; i < testSize; i++) {
            testString[i] = static_cast<char>(i);
            if (i % 2) {
                builder->Add(TBlockItem(TStringRef(testString.data(), i + 1)));
            } else {
                // Empty string
                builder->Add(TBlockItem(TStringRef()));
            }
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        IBlockLayoutConverter::PackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), 1, "Expected the one column after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        const auto& columnBefore = columns.front().array();
        const auto& columnAfter = columnsAfterConversion.front().array();
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), stringType);

        for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
            TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
            TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.AsStringRef(), rhs.AsStringRef(), "Expected the same data after conversion");
        }
    }
}
