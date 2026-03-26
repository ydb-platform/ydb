#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>

namespace NYql::NUdf {

namespace {

using namespace NKikimr;

class TBlockReaderFixture: public NUnitTest::TBaseFixture {
    class TArrayHelpers: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TArrayHelpers>;

        explicit TArrayHelpers(const NMiniKQL::TType* type, arrow::MemoryPool* const arrowPool)
            : Builder(MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), type, *arrowPool, NMiniKQL::CalcBlockLen(CalcMaxBlockItemSize(type)), nullptr))
            , Reader(MakeBlockReader(NMiniKQL::TTypeInfoHelper(), type))
        {
        }

    public:
        const std::unique_ptr<IArrayBuilder> Builder;
        const std::unique_ptr<IBlockReader> Reader;
    };

public:
    TBlockReaderFixture()
        : FunctionRegistry(CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , PgmBuilder(Env, *FunctionRegistry)
        , ArrowPool(GetYqlMemoryPool())
    {
    }

    NMiniKQL::TType* OptionaType(NMiniKQL::TType* type) const {
        return PgmBuilder.NewOptionalType(type);
    }

    template <typename T>
    NMiniKQL::TType* DataType() const {
        return PgmBuilder.NewDataType(NUdf::TDataType<T>::Id);
    }

    NMiniKQL::TType* DataType(NUdf::EDataSlot dataSlot) const {
        return PgmBuilder.NewDataType(dataSlot);
    }

    template <typename... TArgs>
    NMiniKQL::TType* TupleType(TArgs&&... args) const {
        return PgmBuilder.NewTupleType({std::forward<TArgs>(args)...});
    }

    TArrayHelpers::TPtr GetArrayHelpers(const NMiniKQL::TType* type) const {
        return MakeIntrusive<TArrayHelpers>(type, ArrowPool);
    }

public:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment Env;
    NMiniKQL::TProgramBuilder PgmBuilder;
    arrow::MemoryPool* const ArrowPool;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlockReaderTest) {
Y_UNIT_TEST_F(TestLogicalDataSize, TBlockReaderFixture) {
    const std::vector arrayHelpers = {
        GetArrayHelpers(DataType<ui32>()),
        GetArrayHelpers(OptionaType(DataType<char*>())),
        GetArrayHelpers(OptionaType(TupleType(OptionaType(DataType<ui32>()), DataType<char*>()))),
        GetArrayHelpers(DataType(NUdf::EDataSlot::TzDate)),
        GetArrayHelpers(PgmBuilder.NewNullType())};

    constexpr ui32 size = 1000;
    constexpr ui32 stringSize = 37;
    for (ui32 i = 0; i < size; ++i) {
        arrayHelpers[0]->Builder->Add(TBlockItem(i));

        const auto str = NUnitTest::RandomString(stringSize, i);
        arrayHelpers[1]->Builder->Add((i % 2) ? TBlockItem(str) : TBlockItem());

        auto tuple = std::to_array<TBlockItem>({((i / 2) % 2) ? TBlockItem(i) : TBlockItem(), TBlockItem(str)});
        arrayHelpers[2]->Builder->Add((i % 2) ? TBlockItem(tuple.data()) : TBlockItem());

        TBlockItem tzDate(i);
        tzDate.SetTimezoneId(i % 100);
        arrayHelpers[3]->Builder->Add(tzDate);
        arrayHelpers[4]->Builder->Add(TBlockItem());
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
    arrays.reserve(arrayHelpers.size());
    for (const auto& helper : arrayHelpers) {
        arrays.emplace_back(helper->Builder->Build(true).array());
    }

    constexpr ui32 offset = 133;
    constexpr ui32 len = 533;
    static_assert(offset + len < size);

    constexpr ui64 offsetSize = sizeof(arrow::BinaryType::offset_type) * len;
    constexpr ui64 bitmaskSize = (len - 1) / 8 + 1;
    constexpr ui64 nonEmptyStrings = (len - offset % 2) / 2 + offset % 2;
    const std::vector<ui64> expectedLogicalSize = {
        sizeof(ui32) * len,
        bitmaskSize + offsetSize + stringSize * nonEmptyStrings,
        2 * bitmaskSize + offsetSize + sizeof(ui32) * len + stringSize * nonEmptyStrings,
        (sizeof(ui16) + sizeof(ui16)) * len,
        0};

    // Test GetDataWeight with offset and length
    for (ui32 i = 0; i < arrayHelpers.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(arrayHelpers[i]->Reader->GetSliceDataWeight(*arrays[i], offset, len), expectedLogicalSize[i], "array: " << i);
    }

    // Test GetDataWeight after slize
    for (ui32 i = 0; i < arrayHelpers.size(); ++i) {
        const auto slice = DeepSlice(arrays[i], offset, len);
        UNIT_ASSERT_VALUES_EQUAL_C(arrayHelpers[i]->Reader->GetDataWeight(*slice), expectedLogicalSize[i], "sliced array: " << i);
    }
}
} // Y_UNIT_TEST_SUITE(BlockReaderTest)

} // namespace NYql::NUdf
