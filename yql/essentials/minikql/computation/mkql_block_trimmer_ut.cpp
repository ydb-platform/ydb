#include "mkql_block_trimmer.h"

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

namespace {

struct TBlockTrimmerTestData {
    TBlockTrimmerTestData()
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

void CheckTrimmedSlice(std::shared_ptr<arrow::ArrayData> array) {
    UNIT_ASSERT_VALUES_EQUAL(array->offset, 0);
    for (const auto& buffer : array->buffers) {
        if (buffer) {
            UNIT_ASSERT_GE(buffer->size(), 1);
        }
    }
    for (const auto& childData : array->child_data) {
        if (childData) {
            CheckTrimmedSlice(childData);
        }
    }
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TBlockTrimmerTest) {
    Y_UNIT_TEST(TestFixedSize) {
        TBlockTrimmerTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(int64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = NMiniKQL::MaxBlockSizeInBytes / sizeof(i64);
        constexpr auto sliceSize = 1024;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), int64Type, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), int64Type);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), int64Type, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            builder->Add(TBlockItem(i));
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        auto array = datum.array();

        for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
            auto slice = Chop(array, sliceSize);
            auto trimmedSlice = trimmer->Trim(slice);
            CheckTrimmedSlice(trimmedSlice);

            for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after trim");
            }
        }
    }

    Y_UNIT_TEST(TestString) {
        TBlockTrimmerTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(stringType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        // To fit all strings into single block
        constexpr auto testSize = 512;
        constexpr auto sliceSize = 128;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), stringType, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), stringType);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), stringType, data.ArrowPool);

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
        auto array = datum.array();

        for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
            auto slice = Chop(array, sliceSize);
            auto trimmedSlice = trimmer->Trim(slice);
            CheckTrimmedSlice(trimmedSlice);

            for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.AsStringRef(), rhs.AsStringRef(), "Expected the same data after trim");
            }
        }
    }

    Y_UNIT_TEST(TestOptional) {
        TBlockTrimmerTestData data;

        const auto optionalInt64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true);

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(optionalInt64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = NMiniKQL::MaxBlockSizeInBytes / sizeof(i64);
        constexpr auto sliceSize = 1024;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), optionalInt64Type, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), optionalInt64Type);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), optionalInt64Type, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            if (i % 2) {
                builder->Add(TBlockItem());
            } else {
                builder->Add(TBlockItem(i));
            }
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        auto array = datum.array();

        for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
            auto slice = Chop(array, sliceSize);
            auto trimmedSlice = trimmer->Trim(slice);
            CheckTrimmedSlice(trimmedSlice);

            for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);
                UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs), bool(rhs), "Expected the same optionality after trim");

                if (lhs) {
                    UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after trim");
                }
            }
        }
    }

    Y_UNIT_TEST(TestExternalOptional) {
        TBlockTrimmerTestData data;

        const auto doubleOptInt64Type = data.PgmBuilder.NewOptionalType(data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true));

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(doubleOptInt64Type);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = NMiniKQL::MaxBlockSizeInBytes / sizeof(i64);
        constexpr auto sliceSize = 1024;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), doubleOptInt64Type, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), doubleOptInt64Type);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), doubleOptInt64Type, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            if (i % 2) {
                builder->Add(TBlockItem(i).MakeOptional());
            } else if (i % 4) {
                builder->Add(TBlockItem());
            } else {
                builder->Add(TBlockItem().MakeOptional());
            }
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        auto array = datum.array();

        for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
            auto slice = Chop(array, sliceSize);
            auto trimmedSlice = trimmer->Trim(slice);
            CheckTrimmedSlice(trimmedSlice);

            for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);

                for (size_t i = 0; i < 2; i++) {
                    UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs), bool(rhs), "Expected the same optionality after trim");
                    if (!lhs) {
                        break;
                    }

                    lhs = lhs.GetOptionalValue();
                    rhs = rhs.GetOptionalValue();
                }

                if (lhs) {
                    UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<i64>(), rhs.Get<i64>(), "Expected the same data after trim");
                }
            }
        }
    }

    Y_UNIT_TEST(TestTuple) {
        TBlockTrimmerTestData data;

        std::vector<NMiniKQL::TType*> types;
        types.push_back(data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64));
        types.push_back(data.PgmBuilder.NewDataType(NUdf::EDataSlot::String));
        types.push_back(data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true));
        const auto tupleType = data.PgmBuilder.NewTupleType(types);

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(tupleType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        // To fit all strings into single block
        constexpr auto testSize = 512;
        constexpr auto sliceSize = 128;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), tupleType, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), tupleType);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), tupleType, data.ArrowPool);

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
        auto array = datum.array();

        for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
            auto slice = Chop(array, sliceSize);
            auto trimmedSlice = trimmer->Trim(slice);
            CheckTrimmedSlice(trimmedSlice);

            for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);

                UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetElement(0).Get<i64>(), rhs.GetElement(0).Get<i64>(), "Expected the same data after trim");
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetElement(1).AsStringRef(), rhs.GetElement(1).AsStringRef(), "Expected the same data after trim");
                UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs.GetElement(2)), bool(rhs.GetElement(2)), "Expected the same optionality after trim");
                if (bool(lhs.GetElement(2))) {
                    UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetElement(2).Get<i64>(), rhs.GetElement(2).Get<i64>(), "Expected the same data after trim");
                }
            }
        }

        for (auto tupleItems : testTuples) {
            delete[] tupleItems;
        }
    }

    Y_UNIT_TEST(TestTzDate) {
        TBlockTrimmerTestData data;
        using TDtLayout = TDataType<TTzDatetime>::TLayout;

        const auto tzDatetimeType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::TzDatetime, false);

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(tzDatetimeType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = NMiniKQL::MaxBlockSizeInBytes / (sizeof(TDtLayout) + sizeof(ui16));
        constexpr auto sliceSize = 1024;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), tzDatetimeType, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), tzDatetimeType);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), tzDatetimeType, data.ArrowPool);

        for (size_t i = 0; i < testSize; i++) {
            TBlockItem dt = TBlockItem(i);
            dt.SetTimezoneId(i * 2);
            builder->Add(dt);
        }
        auto datum = builder->Build(true);
        Y_ENSURE(datum.is_array());
        auto array = datum.array();

        for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
            auto slice = Chop(array, sliceSize);
            auto trimmedSlice = trimmer->Trim(slice);
            CheckTrimmedSlice(trimmedSlice);

            for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<TDtLayout>(), rhs.Get<TDtLayout>(), "Expected the same data after trim");
                UNIT_ASSERT_VALUES_EQUAL_C(lhs.GetTimezoneId(), rhs.GetTimezoneId(), "Expected the same data after trim");
            }
        }
    }

    extern const char ResourceName[] = "Resource.Name";
    Y_UNIT_TEST(TestResource) {
        TBlockTrimmerTestData data;

        const auto resourceType = data.PgmBuilder.NewResourceType(ResourceName);

        size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(resourceType);
        size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
        Y_ENSURE(blockLen > 8);

        constexpr auto testSize = NMiniKQL::MaxBlockSizeInBytes / sizeof(TUnboxedValue);
        constexpr auto sliceSize = 1024;
        static_assert(testSize % sliceSize == 0);

        auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), resourceType, *data.ArrowPool, blockLen, nullptr);
        auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), resourceType);
        auto trimmer = MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), resourceType, data.ArrowPool);

        struct TWithDtor {
            int Payload;
            std::shared_ptr<int> DestructorCallsCnt;
            TWithDtor(int payload, std::shared_ptr<int> destructorCallsCnt):
                Payload(payload), DestructorCallsCnt(std::move(destructorCallsCnt)) {
            }
            ~TWithDtor() {
                *DestructorCallsCnt = *DestructorCallsCnt + 1;
            }
        };
        using TTestResource = TBoxedResource<TWithDtor, ResourceName>;

        auto destructorCallsCnt = std::make_shared<int>(0);
        {
            for (size_t i = 0; i < testSize; i++) {
                builder->Add(TUnboxedValuePod(new TTestResource(i, destructorCallsCnt)));
            }
            auto datum = builder->Build(true);
            Y_ENSURE(datum.is_array());
            auto array = datum.array();

            for (size_t sliceIdx = 0; sliceIdx < testSize / sliceSize; sliceIdx++) {
                auto slice = Chop(array, sliceSize);
                auto trimmedSlice = trimmer->Trim(slice);
                CheckTrimmedSlice(trimmedSlice);

                for (size_t elemIdx = 0; elemIdx < sliceSize; elemIdx++) {
                    TBlockItem lhs = reader->GetItem(*slice, elemIdx);
                    TBlockItem rhs = reader->GetItem(*trimmedSlice, elemIdx);

                    auto lhsResource = reinterpret_cast<TTestResource*>(lhs.GetBoxed().Get());
                    auto rhsResource = reinterpret_cast<TTestResource*>(rhs.GetBoxed().Get());
                    UNIT_ASSERT_VALUES_EQUAL_C(lhsResource->Get()->Payload, rhsResource->Get()->Payload, "Expected the same data after trim");
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(*destructorCallsCnt, testSize, "Expected 1 call to resource destructor");
    }
}
