#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

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
}
