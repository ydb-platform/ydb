#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>

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

constexpr int TestSize = 128 * 3 * sizeof(i64);

Y_UNIT_TEST_SUITE(TBlockLayoutConverterTest) {
    Y_UNIT_TEST(TestFixedSize) {
        TBlockLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector< NKikimr::NMiniKQL::TType*> types{int64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(int64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = TestSize / sizeof(i64);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            builder->Add(TBlockItem(i));
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        TPackResult packRes;
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

    Y_UNIT_TEST(TestMultipleFixedSize) {
        TBlockLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector< NKikimr::NMiniKQL::TType*> types{int64Type, int64Type, int64Type, int64Type};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Payload};

        size_t itemSize = 4 * NMiniKQL::CalcMaxBlockItemSize(int64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = TestSize / 4;

        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);
        TVector<arrow::Datum> columns;

        for (size_t i = 0; i < types.size(); ++i) {
            auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, blockLen, nullptr);
            for (size_t j = 0; j < testSize; j++) {
                builder->Add(TBlockItem(i + j));
            }
            auto datum = builder->Build(true);
            columns.emplace_back(std::move(datum));
            Y_ENSURE(datum.is_array());
        }

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), columns.size(), "Expected same columns count after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
            const auto& columnBefore = columns[colIdx].array();
            const auto& columnAfter = columnsAfterConversion[colIdx].array();
            auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), int64Type);

            for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
                TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after conversion");
            }
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

        TPackResult packRes;
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

    Y_UNIT_TEST(TestMultipleStrings) {
        TBlockLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector< NKikimr::NMiniKQL::TType*> types{stringType, stringType, stringType, stringType};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Payload};

        size_t itemSize = 4 * NMiniKQL::CalcMaxBlockItemSize(stringType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        // To fit all strings into single block
        constexpr auto testSize = 128;

        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);
        TVector<arrow::Datum> columns;

        for (size_t i = 0; i < types.size(); ++i) {
            auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), stringType, *data.ArrowPool, blockLen, nullptr);
            std::string testString;
            testString.resize(testSize);
            for (size_t j = 0; j < testSize; j++) {
                testString[j] = static_cast<char>(j % 256);
                if (j % 2) {
                    builder->Add(TBlockItem(TStringRef(testString.data(), i * 2 + j / 2 + 1)));
                } else {
                    // Empty string
                    builder->Add(TBlockItem(TStringRef()));
                }
            }
            auto datum = builder->Build(true);
            columns.emplace_back(std::move(datum));
            Y_ENSURE(datum.is_array());
        }

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), columns.size(), "Expected same columns count after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
            const auto& columnBefore = columns[colIdx].array();
            const auto& columnAfter = columnsAfterConversion[colIdx].array();
            auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), stringType);

            for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
                TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.AsStringRef(), rhs.AsStringRef(), "Expected the same data after conversion");
            }
        }
    }

    Y_UNIT_TEST(TestMultipleVariousTypes) {
        TBlockLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);

        TVector< NKikimr::NMiniKQL::TType*> types{int64Type, stringType, int64Type, stringType};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload};

        size_t itemSize = 2 * NMiniKQL::CalcMaxBlockItemSize(stringType) + 2 * NMiniKQL::CalcMaxBlockItemSize(int64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = 128;

        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);
        TVector<arrow::Datum> columns;

        for (size_t i = 0; i < types.size(); ++i) {
            if (i % 2 == 0) {
                auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, blockLen, nullptr);
                for (size_t j = 0; j < testSize; j++) {
                    builder->Add(TBlockItem(i + j));
                }
                auto datum = builder->Build(true);
                columns.emplace_back(std::move(datum));
            } else {
                auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), stringType, *data.ArrowPool, blockLen, nullptr);
                std::string testString;
                testString.resize(testSize);
                for (size_t j = 0; j < testSize; j++) {
                    testString[j] = static_cast<char>(j % 256);
                    if (j % 2) {
                        builder->Add(TBlockItem(TStringRef(testString.data(), i * 2 + j / 2 + 1)));
                    } else {
                        // Empty string
                        builder->Add(TBlockItem(TStringRef()));
                    }
                }
                auto datum = builder->Build(true);
                columns.emplace_back(std::move(datum));
            }
        }

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), columns.size(), "Expected same columns count after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
            const auto& columnBefore = columns[colIdx].array();
            const auto& columnAfter = columnsAfterConversion[colIdx].array();

            if (colIdx % 2 == 0) {
                auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), int64Type);
                for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
                    TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
                    TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
                    UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after conversion");
                }
            } else {
                auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), stringType);
                for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
                    TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
                    TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
                    UNIT_ASSERT_VALUES_EQUAL_C(lhs.AsStringRef(), rhs.AsStringRef(), "Expected the same data after conversion");
                }
            }
        }
    }

    Y_UNIT_TEST(TestOptional) {
        TBlockLayoutConverterTestData data;

        const auto optionalInt64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true);
        TVector< NKikimr::NMiniKQL::TType*> types{optionalInt64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(optionalInt64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = TestSize / sizeof(i64);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), optionalInt64Type, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            if (i % 2) {
                builder->Add(TBlockItem());
            } else {
                builder->Add(TBlockItem(i));
            }
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), 1, "Expected the one column after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        const auto& columnBefore = columns.front().array();
        const auto& columnAfter = columnsAfterConversion.front().array();
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), optionalInt64Type);

        for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
            TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
            TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);
            UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs), bool(rhs), "Expected the same optionality after conversion");

            if (lhs) {
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after conversion");
            }
        }
    }

    Y_UNIT_TEST(TestTuple) {
        TBlockLayoutConverterTestData data;

        std::vector<NMiniKQL::TType*> t;
        t.push_back(data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64));
        t.push_back(data.PgmBuilder.NewDataType(NUdf::EDataSlot::String));
        t.push_back(data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true));
        const auto tupleType = data.PgmBuilder.NewTupleType(t);
        TVector< NKikimr::NMiniKQL::TType*> types{tupleType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(tupleType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = 512;

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), tupleType, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        std::string testString;
        testString.resize(testSize);
        std::vector<TBlockItem*> testTuples(testSize);
        for (size_t i = 0; i < testSize; i++) {
            testString[i] = static_cast<char>(i);

            TBlockItem* tupleItems = new TBlockItem[3];
            testTuples.push_back(tupleItems);
            tupleItems[0] = TBlockItem(i);
            tupleItems[1] = TBlockItem(TStringRef(testString.data(), i + 1));
            tupleItems[2] = i % 2 ? TBlockItem(i) : TBlockItem();

            builder->Add(TBlockItem(tupleItems));
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), columns.size(), "Expected the one column after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        const auto& columnBefore = columns.front().array();
        const auto& columnAfter = columnsAfterConversion.front().array();
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), tupleType);

        for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
            TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
            TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);

            UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetElement(0).Get<i64>(), rhs.GetElement(0).Get<i64>(), "Expected the same data after conversion");
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetElement(1).AsStringRef(), rhs.GetElement(1).AsStringRef(), "Expected the same data after conversion");
            UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs.GetElement(2)), bool(rhs.GetElement(2)), "Expected the same optionality after conversion");
            if (bool(lhs.GetElement(2))) {
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetElement(2).Get<i64>(), rhs.GetElement(2).Get<i64>(), "Expected the same data after conversion");
            }
        }

        for (auto tupleItems : testTuples) {
            delete[] tupleItems;
        }
    }

    Y_UNIT_TEST(TestTzDate) {
        TBlockLayoutConverterTestData data;
        using TDtLayout = NUdf::TDataType<TTzDatetime>::TLayout;

        const auto tzDatetimeType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::TzDatetime, false);
        TVector< NKikimr::NMiniKQL::TType*> types{tzDatetimeType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(tzDatetimeType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = TestSize / (sizeof(TDtLayout) + sizeof(ui16));

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), tzDatetimeType, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            TBlockItem dt = TBlockItem(i);
            dt.SetTimezoneId(i * 2);
            builder->Add(dt);
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), columns.size(), "Expected the one column after conversion");
        Y_ENSURE(columnsAfterConversion.front().is_array());

        const auto& columnBefore = columns.front().array();
        const auto& columnAfter = columnsAfterConversion.front().array();
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), tzDatetimeType);

        for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
            TBlockItem lhs = reader->GetItem(*columnBefore, elemIdx);
            TBlockItem rhs = reader->GetItem(*columnAfter, elemIdx);

            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<TDtLayout>(), rhs.Get<TDtLayout>(), "Expected the same data after conversion");
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetTimezoneId(), rhs.GetTimezoneId(), "Expected the same data after conversion");
        }
    }

    Y_UNIT_TEST(TestNullBitmapPreservationAcrossMultiplePacks) {
        TBlockLayoutConverterTestData data;

        const auto optStringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, true);
        TVector<NKikimr::NMiniKQL::TType*> types{optStringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Payload};

        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        // array has a non-null validity bitmap
        TPackResult packWithNulls;
        {
            auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), optStringType,
                                            *data.ArrowPool, 1024, nullptr);
            builder->Add(TBlockItem());                             // NULL
            builder->Add(TBlockItem(TStringRef("hello", 5)));      // valid
            builder->Add(TBlockItem());                             // NULL
            auto datum = builder->Build(true);

            TVector<arrow::Datum> columns{datum};
            converter->Pack(columns, packWithNulls);
        }

        // arrow omits the bitmap for fully-valid blocks
        {
            auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), optStringType,
                                            *data.ArrowPool, 1024, nullptr);
            builder->Add(TBlockItem(TStringRef("world", 5)));
            builder->Add(TBlockItem(TStringRef("test", 4)));
            auto datum = builder->Build(true);

            auto arrayData = datum.array()->Copy();
            arrayData->buffers[0] = nullptr;
            arrayData->null_count = 0;

            TVector<arrow::Datum> columns{arrow::Datum(arrayData)};
            TPackResult packRes;
            converter->Pack(columns, packRes);
        }

        TVector<arrow::Datum> unpacked;
        converter->Unpack(packWithNulls, unpacked);
        UNIT_ASSERT_VALUES_EQUAL(unpacked.size(), 1u);

        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), optStringType);
        const auto& col = *unpacked[0].array();

        TBlockItem item0 = reader->GetItem(col, 0);
        UNIT_ASSERT_C(!item0, "Row 0 must be NULL");

        TBlockItem item1 = reader->GetItem(col, 1);
        UNIT_ASSERT_C(item1, "Row 1 must be non-NULL");
        UNIT_ASSERT_VALUES_EQUAL(item1.AsStringRef(), TStringRef("hello", 5));

        TBlockItem item2 = reader->GetItem(col, 2);
        UNIT_ASSERT_C(!item2, "Row 2 must be NULL");
    }

    Y_UNIT_TEST(TestSingularColumns) {
        TBlockLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        auto* const voidType = data.Env.GetTypeOfVoidLazy();
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, voidType, voidType};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload};

        constexpr size_t testSize = 137;

        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, testSize, nullptr);
        for (size_t i = 0; i < testSize; i++) {
            builder->Add(TBlockItem(i));
        }
        auto keyDatum = builder->Build(true);
        Y_ENSURE(keyDatum.is_array());

        TVector<arrow::Datum> columns{
            keyDatum,
            arrow::Datum(NYql::NUdf::MakeSingularArray(/*isNull=*/false, testSize)),
            arrow::Datum(NYql::NUdf::MakeSingularArray(/*isNull=*/false, testSize))};

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        TVector<arrow::Datum> columnsAfterConversion;
        converter->Unpack(packRes, columnsAfterConversion);
        UNIT_ASSERT_VALUES_EQUAL_C(columnsAfterConversion.size(), columns.size(), "Expected same columns count after conversion");

        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), int64Type);
        const auto& keyBefore = columns.front().array();
        const auto& keyAfter = columnsAfterConversion.front().array();
        for (size_t elemIdx = 0; elemIdx < testSize; elemIdx++) {
            TBlockItem lhs = reader->GetItem(*keyBefore, elemIdx);
            TBlockItem rhs = reader->GetItem(*keyAfter, elemIdx);
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after conversion");
        }

        for (size_t colIdx = 1; colIdx < columns.size(); ++colIdx) {
            const auto& before = columns[colIdx].array();
            const auto& after = columnsAfterConversion[colIdx].array();
            UNIT_ASSERT_VALUES_EQUAL_C(after->length, before->length, "Expected the same length after conversion");
            UNIT_ASSERT_C(after->type->Equals(*before->type), "Expected the same arrow type after conversion");
        }
    }

    // singular null has DataSize == 0
    // without packing its allnull validity bitmap
    // two Null keys compare equal and would match in the join.
    Y_UNIT_TEST(TestSingularNullKeyPackedAsNull) {
        TBlockLayoutConverterTestData data;

        auto* const nullType = data.Env.GetTypeOfNullLazy();
        TVector<NKikimr::NMiniKQL::TType*> types{nullType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        constexpr size_t testSize = 17;

        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);
        TVector<arrow::Datum> columns{
            arrow::Datum(NYql::NUdf::MakeSingularArray(/*isNull=*/true, testSize))};

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL(packRes.NTuples, testSize);

        const auto* layout = converter->GetTupleLayout();
        UNIT_ASSERT_VALUES_EQUAL(layout->KeyColumnsNum, 1u);
        UNIT_ASSERT_VALUES_EQUAL(layout->Columns[0].DataSize, 0u);

        for (size_t row = 0; row < testSize; ++row) {
            const ui8* tuple = packRes.PackedTuples.data() + row * layout->TotalRowSize;
            const ui8 bit = (tuple[layout->BitmaskOffset] >> 0) & 1u;
            UNIT_ASSERT_VALUES_EQUAL_C(bit, 0u, "Null key column must be packed as NULL");
        }

        const ui8* row0 = packRes.PackedTuples.data();
        const ui8* row1 = packRes.PackedTuples.data() + layout->TotalRowSize;
        UNIT_ASSERT_C(
            !layout->KeysEqual(row0, packRes.Overflow.data(), row1, packRes.Overflow.data()),
            "Null keys must not compare equal");
    }

    Y_UNIT_TEST(TestBuckets) {
        TBlockLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector< NKikimr::NMiniKQL::TType*> types{int64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(int64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = TestSize / sizeof(i64);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, blockLen, nullptr);
        auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            builder->Add(TBlockItem(i));
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        TVector<arrow::Datum> columns{datum};

        TPackResult packRes;
        converter->Pack(columns, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testSize, "Expected the same dataset sizes after conversion");

        static constexpr ui32 bucketsLogNum = 5;
        auto packReses = std::array<TPackResult, 1u << bucketsLogNum>{};
        converter->BucketPack(columns, packReses.data(), bucketsLogNum);
        
        const ui32 bucketedTuplesNum = std::accumulate(packReses.begin(), packReses.end(), 0, [](size_t lhs, const TPackResult& rhs) {
            return lhs + rhs.NTuples;
        });
        UNIT_ASSERT_EQUAL(testSize, bucketedTuplesNum);

        const size_t totalRowSize = converter->GetTupleLayout()->TotalRowSize;

        ui32 hashsum = 0;
        for (size_t i = 0; i < packRes.PackedTuples.size(); i += totalRowSize) {
            hashsum += ReadUnaligned<ui32>(packRes.PackedTuples.data() + i);
        }
        ui32 bhashsum = 0;
        for (const auto& packRes : packReses) {
            for (size_t i = 0; i < packRes.PackedTuples.size(); i += totalRowSize) {
                bhashsum += ReadUnaligned<ui32>(packRes.PackedTuples.data() + i);
            }
        }
        UNIT_ASSERT_EQUAL(hashsum, bhashsum);
    }

    struct TBucketPages {
        static constexpr ui32 NBuckets = 64;
        static constexpr ui32 PageSizeBytes = 1 << 16;

        std::array<TPackResult, NBuckets> Building;
        std::array<std::vector<TPackResult>, NBuckets> Closed;

        void OnFull(ui32 bucket) {
            Closed[bucket].push_back(std::move(Building[bucket]));
            Building[bucket] = TPackResult{};
        }

        TPaddedPtr<TPackResult> Pages() {
            return TPaddedPtr<TPackResult>(Building.data());
        }

        void AddRowBaseline(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout) {
            const ui32 bucket = NPackedTuple::Hash(tuple.PackedData) & (NBuckets - 1);
            Building[bucket].AppendTuple(tuple, layout);
            if (Building[bucket].AllocatedBytes() > PageSizeBytes) {
                OnFull(bucket);
            }
        }
    };

    void AssertBucketPagesEqual(TBucketPages& lhs, TBucketPages& rhs) {
        for (ui32 b = 0; b < TBucketPages::NBuckets; ++b) {
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Closed[b].size(), rhs.Closed[b].size(), "bucket " << b);
            for (size_t p = 0; p < lhs.Closed[b].size(); ++p) {
                UNIT_ASSERT_VALUES_EQUAL(lhs.Closed[b][p].NTuples, rhs.Closed[b][p].NTuples);
                UNIT_ASSERT_EQUAL(lhs.Closed[b][p].PackedTuples, rhs.Closed[b][p].PackedTuples);
                UNIT_ASSERT_EQUAL(lhs.Closed[b][p].Overflow, rhs.Closed[b][p].Overflow);
            }
            UNIT_ASSERT_VALUES_EQUAL(lhs.Building[b].NTuples, rhs.Building[b].NTuples);
            UNIT_ASSERT_EQUAL(lhs.Building[b].PackedTuples, rhs.Building[b].PackedTuples);
            UNIT_ASSERT_EQUAL(lhs.Building[b].Overflow, rhs.Building[b].Overflow);
        }
    }

    TVector<arrow::Datum> MakeInt32Columns(TBlockLayoutConverterTestData& data, ui32 nCols, ui32 nRows, bool withNulls) {
        const auto type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int32, true);
        size_t itemSize = nCols * NMiniKQL::CalcMaxBlockItemSize(type);
        size_t blockLen = std::max<size_t>(NMiniKQL::CalcBlockLen(itemSize), nRows);

        TVector<arrow::Datum> columns;
        columns.reserve(nCols);
        for (ui32 c = 0; c < nCols; ++c) {
            auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), type, *data.ArrowPool, blockLen, nullptr);
            for (ui32 r = 0; r < nRows; ++r) {
                if (withNulls && ((r + c) % 17 == 0)) {
                    builder->Add(TBlockItem());
                } else {
                    builder->Add(TBlockItem(i32(c * 100000 + r)));
                }
            }
            columns.emplace_back(builder->Build(true));
        }
        return columns;
    }

    IBlockLayoutConverter::TPtr MakeInt32Converter(TBlockLayoutConverterTestData& data, ui32 nCols) {
        const auto type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int32, true);
        TVector<NKikimr::NMiniKQL::TType*> types(nCols, type);
        TVector<NPackedTuple::EColumnRole> roles(nCols, NPackedTuple::EColumnRole::Payload);
        roles[0] = NPackedTuple::EColumnRole::Key;
        return MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.ArrowPool);
    }

    Y_UNIT_TEST(TestPackSelectedMatchesPackInt32) {
        TBlockLayoutConverterTestData data;
        constexpr ui32 nRows = 384;
        auto converter = MakeInt32Converter(data, 1);
        auto columns = MakeInt32Columns(data, 1, nRows, true);

        TPackResult packed;
        converter->Pack(columns, packed);
        UNIT_ASSERT_VALUES_EQUAL(packed.NTuples, nRows);
        UNIT_ASSERT(converter->GetTupleLayout()->SupportsDirectBucketPack());

        TPackResult gathered;
        UNIT_ASSERT(converter->PackIntoBucketPages(
            columns, TPaddedPtr<TPackResult>(&gathered), 1,
            std::numeric_limits<ui32>::max(), [](ui32) {}));
        UNIT_ASSERT_VALUES_EQUAL(gathered.NTuples, nRows);
        UNIT_ASSERT_EQUAL(gathered.PackedTuples, packed.PackedTuples);
        UNIT_ASSERT_EQUAL(gathered.Overflow, packed.Overflow);
    }

    void RunDirectVsAddRow(ui32 nCols, ui32 nRows, bool withNulls, bool bench) {
        TBlockLayoutConverterTestData data;
        auto converter = MakeInt32Converter(data, nCols);
        auto columns = MakeInt32Columns(data, nCols, nRows, withNulls);
        const auto* layout = converter->GetTupleLayout();
        UNIT_ASSERT(layout->SupportsDirectBucketPack());

        auto baseline = [&] {
            TBucketPages pages;
            TPackResult packed;
            converter->Pack(columns, packed);
            for (TSingleTuple tuple : packed) {
                pages.AddRowBaseline(tuple, layout);
            }
            return pages;
        };

        auto direct = [&] {
            TBucketPages pages;
            UNIT_ASSERT(converter->PackIntoBucketPages(
                columns, pages.Pages(), TBucketPages::NBuckets, TBucketPages::PageSizeBytes,
                [&](ui32 b) { pages.OnFull(b); }));
            return pages;
        };

        if (bench) {
            auto expected = baseline();
            auto got = direct();
            AssertBucketPagesEqual(expected, got);

            constexpr int kIters = 7;
            i64 baseUs = std::numeric_limits<i64>::max();
            i64 directUs = std::numeric_limits<i64>::max();
            for (int i = 0; i < kIters; ++i) {
                auto t0 = std::chrono::steady_clock::now();
                Y_UNUSED(baseline());
                auto t1 = std::chrono::steady_clock::now();
                Y_UNUSED(direct());
                auto t2 = std::chrono::steady_clock::now();
                baseUs = std::min(baseUs, (i64)std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
                directUs = std::min(directUs, (i64)std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
            }
            i64 packedBytes = 0;
            for (ui32 b = 0; b < TBucketPages::NBuckets; ++b) {
                packedBytes += got.Building[b].AllocatedBytes();
                for (const auto& page : got.Closed[b]) {
                    packedBytes += page.AllocatedBytes();
                }
            }
            Cerr << "DirectArrowBucketPack cols=" << nCols << " rows=" << nRows
                 << " packedBytes=" << packedBytes
                 << " baselineUs=" << baseUs
                 << " directUs=" << directUs
                 << Endl;
        } else {
            auto expected = baseline();
            auto got = direct();
            AssertBucketPagesEqual(expected, got);
        }
    }

    Y_UNIT_TEST(TestDirectBucketPackInt32) {
        RunDirectVsAddRow(1, 384, true, false);
        RunDirectVsAddRow(1, 65536, false, true);
    }

    Y_UNIT_TEST(TestDirectBucketPackWide64) {
        RunDirectVsAddRow(64, 1024, true, false);
        RunDirectVsAddRow(64, 65536, false, true);
    }
}
