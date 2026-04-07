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

constexpr size_t MAX_BLOCK_SIZE = 240_KB;

struct TArrayBuilderTestData {
    TArrayBuilderTestData()
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

std::unique_ptr<IArrayBuilder> MakeResourceArrayBuilder(TType* resourceType, TArrayBuilderTestData& data) {
    auto arrayBuilder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), resourceType,
                                         *data.ArrowPool, MAX_BLOCK_SIZE, /* pgBuilder */ nullptr);
    UNIT_ASSERT_C(arrayBuilder, "Failed to make resource arrow array builder");
    return arrayBuilder;
}

Y_UNIT_TEST_SUITE(TArrayBuilderTest) {
Y_UNIT_TEST(TestEmbeddedResourceBuilder) {
    TArrayBuilderTestData data;
    const auto resourceType = data.PgmBuilder.NewResourceType("Test.Resource");
    const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);
    auto resource = TUnboxedValuePod::Embedded("testtest");
    arrayBuilder->Add(resource);
    auto datum = arrayBuilder->Build(true);
    UNIT_ASSERT(datum.is_array());
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), 1);

    auto value = datum.array()->GetValues<TUnboxedValue>(1)[0];
    UNIT_ASSERT(value.IsEmbedded());
    UNIT_ASSERT_VALUES_EQUAL_C(TStringRef(value.AsStringRef()), TStringRef(resource.AsStringRef()),
                               "Expected equal values after building array");
}

Y_UNIT_TEST(TestTaggedTypeBuilder) {
    TArrayBuilderTestData data;
    const auto intType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int32, false);
    const auto taggedType = data.PgmBuilder.NewTaggedType(intType, "tag");

    const auto arrayBuilder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), taggedType, *data.ArrowPool, MAX_BLOCK_SIZE, /*pgBuilder=*/nullptr);

    TUnboxedValue testData = TUnboxedValuePod(123);

    arrayBuilder->Add(testData);

    auto datum = arrayBuilder->Build(true);

    UNIT_ASSERT(datum.is_array());
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), 1);

    auto value = datum.array()->buffers[1];

    UNIT_ASSERT_VALUES_EQUAL(*reinterpret_cast<int32_t*>(value->address()), 123);
}

Y_UNIT_TEST(TestTaggedTypeReader) {
    TArrayBuilderTestData data;
    const auto intType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int32, false);
    const auto taggedType = data.PgmBuilder.NewTaggedType(intType, "tag");

    const auto arrayBuilder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), taggedType, *data.ArrowPool, MAX_BLOCK_SIZE, /*pgBuilder=*/nullptr);

    TUnboxedValue first = TUnboxedValuePod(123);
    TUnboxedValue second = TUnboxedValuePod(456);

    arrayBuilder->Add(first);
    arrayBuilder->Add(second);

    auto datum = arrayBuilder->Build(true);

    UNIT_ASSERT(datum.is_array());
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), 2);

    const auto blockReader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), taggedType);

    const auto item1AfterRead = blockReader->GetItem(*datum.array(), 0);
    const auto item2AfterRead = blockReader->GetItem(*datum.array(), 1);

    UNIT_ASSERT_C(item1AfterRead.HasValue(), "Expected not null");
    UNIT_ASSERT_C(item2AfterRead.HasValue(), "Expected not null");

    UNIT_ASSERT_VALUES_EQUAL(item1AfterRead.Get<int>(), 123);
    UNIT_ASSERT_VALUES_EQUAL(item2AfterRead.Get<int>(), 456);
}

extern const char ResourceName[] = "Resource.Name";
Y_UNIT_TEST(TestDtorCall) {
    TArrayBuilderTestData data;
    const auto resourceType = data.PgmBuilder.NewResourceType("Test.Resource");
    const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);

    auto destructorCallsCnt = std::make_shared<int>(0);

    struct TWithDtor {
        int Payload;
        std::shared_ptr<int> DestructorCallsCnt;
        TWithDtor(int payload, std::shared_ptr<int> destructorCallsCnt)
            : Payload(payload)
            , DestructorCallsCnt(std::move(destructorCallsCnt))
        {
        }
        ~TWithDtor() {
            *DestructorCallsCnt = *DestructorCallsCnt + 1;
        }
    };
    int payload = 123;

    using TTestResource = TBoxedResource<std::shared_ptr<TWithDtor>, ResourceName>;
    auto resourcePtr = std::make_shared<TWithDtor>(payload, destructorCallsCnt);
    TUnboxedValuePod resource(new TTestResource(std::move(resourcePtr)));

    {
        arrayBuilder->Add(resource);
        auto datum = arrayBuilder->Build(true);
        UNIT_ASSERT(datum.is_array());
        UNIT_ASSERT_VALUES_EQUAL(datum.length(), 1);

        const auto value = datum.array()->GetValues<TUnboxedValuePod>(1)[0];
        auto boxed = value.AsBoxed().Get();
        const auto resource = reinterpret_cast<TTestResource*>(boxed);
        UNIT_ASSERT_VALUES_EQUAL(resource->Get()->get()->Payload, payload);
    }

    UNIT_ASSERT_VALUES_EQUAL_C(*destructorCallsCnt, 1, "Expected 1 call to resource destructor");
}

Y_UNIT_TEST(TestBoxedResourceNullable) {
    TArrayBuilderTestData data;
    const auto resourceType = data.PgmBuilder.NewOptionalType(data.PgmBuilder.NewResourceType("Test.Resource"));
    const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);

    struct TResourceItem {
        int Payload;
    };
    using TTestResource = TBoxedResource<TResourceItem, ResourceName>;
    for (int i = 0; i < 4; i++) {
        if ((i % 2) == 0) {
            TUnboxedValuePod resource(new TTestResource(TResourceItem{i}));
            arrayBuilder->Add(resource);
        } else {
            arrayBuilder->Add(TUnboxedValuePod{});
        }
    }
    auto datum = arrayBuilder->Build(true);
    const auto blockReader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), resourceType);
    for (int i = 0; i < 4; i++) {
        if ((i % 2) == 0) {
            auto item = blockReader->GetItem(*datum.array(), i);
            UNIT_ASSERT_C(item.HasValue(), "Expected not null");
            auto* resourcePtr = reinterpret_cast<TTestResource*>(item.GetBoxed().Get());
            UNIT_ASSERT_EQUAL(i, resourcePtr->Get()->Payload);
        } else {
            auto item = blockReader->GetItem(*datum.array(), i);
            UNIT_ASSERT(!item.HasValue());
        }
    }
}

Y_UNIT_TEST(TestBuilderWithReader) {
    TArrayBuilderTestData data;
    const auto resourceType = data.PgmBuilder.NewResourceType("Test.Resource");
    const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);

    const auto item1 = TUnboxedValuePod::Embedded("1");
    arrayBuilder->Add(item1);
    const auto item2 = TUnboxedValuePod::Embedded("22");
    arrayBuilder->Add(item2);

    auto datum = arrayBuilder->Build(true);
    UNIT_ASSERT(datum.is_array());
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), 2);

    const auto blockReader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), resourceType);
    const auto item1AfterRead = blockReader->GetItem(*datum.array(), 0);
    const auto item2AfterRead = blockReader->GetItem(*datum.array(), 1);

    UNIT_ASSERT_C(std::memcmp(item1.GetRawPtr(), item1AfterRead.GetRawPtr(), sizeof(TBlockItem)) == 0, "Expected UnboxedValue to equal to BlockItem");
    UNIT_ASSERT_C(std::memcmp(item2.GetRawPtr(), item2AfterRead.GetRawPtr(), sizeof(TBlockItem)) == 0, "Expected UnboxedValue to equal to BlockItem");
}

Y_UNIT_TEST(TestBoxedResourceReader) {
    TArrayBuilderTestData data;
    const auto resourceType = data.PgmBuilder.NewResourceType(ResourceName);
    const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);

    using TTestResource = TBoxedResource<int, ResourceName>;

    arrayBuilder->Add(TUnboxedValuePod(new TTestResource(11111111)));
    arrayBuilder->Add(TUnboxedValuePod(new TTestResource(22222222)));
    const auto datum = arrayBuilder->Build(true);
    UNIT_ASSERT(datum.is_array());
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), 2);

    const auto blockReader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), resourceType);
    const auto item1AfterRead = blockReader->GetItem(*datum.array(), 0);
    const auto item2AfterRead = blockReader->GetItem(*datum.array(), 1);

    auto boxed1 = item1AfterRead.GetBoxed().Get();
    const auto resource1 = reinterpret_cast<TTestResource*>(boxed1);
    UNIT_ASSERT_VALUES_EQUAL(*resource1->Get(), 11111111);
    UNIT_ASSERT_VALUES_EQUAL(resource1->GetResourceTag(), ResourceName);

    auto boxed2 = item2AfterRead.GetBoxed().Get();
    const auto resource2 = reinterpret_cast<TTestResource*>(boxed2);
    UNIT_ASSERT_VALUES_EQUAL(*resource2->Get(), 22222222);
    UNIT_ASSERT_VALUES_EQUAL(resource2->GetResourceTag(), ResourceName);
}

Y_UNIT_TEST(TestTzDateBuilder_Layout) {
    TArrayBuilderTestData data;
    const auto tzDateType = data.PgmBuilder.NewDataType(EDataSlot::TzDate);
    const NMiniKQL::TTypeInfoHelper typeInfoHelper;
    const auto arrayBuilder = MakeArrayBuilder(typeInfoHelper, tzDateType,
                                               *data.ArrowPool, MAX_BLOCK_SIZE, /* pgBuilder */ nullptr);

    auto makeTzDate = [](ui16 val, ui16 tz) {
        TUnboxedValuePod tzDate{val};
        tzDate.SetTimezoneId(tz);
        return tzDate;
    };

    TVector<TUnboxedValuePod> dates{makeTzDate(1234, 1), makeTzDate(1234, 2), makeTzDate(45678, 333)};
    for (auto date : dates) {
        arrayBuilder->Add(date);
    }

    const auto datum = arrayBuilder->Build(true);
    UNIT_ASSERT(datum.is_array());
    const auto array = datum.array();
    const auto expectedType = GetArrowType(typeInfoHelper, tzDateType);
    UNIT_ASSERT(array->type->Equals(expectedType));
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), dates.size());
    const auto childData = array->child_data;
    UNIT_ASSERT_VALUES_EQUAL_C(childData.size(), 2, "Expected date and timezone children");
    UNIT_ASSERT(childData[0]->type->Equals(expectedType->field(0)->type()));
    UNIT_ASSERT(childData[1]->type->Equals(expectedType->field(1)->type()));
}

Y_UNIT_TEST(TestResourceStringValueBuilderReader) {
    TArrayBuilderTestData data;
    const auto resourceType = data.PgmBuilder.NewResourceType(ResourceName);
    const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);

    arrayBuilder->Add(TUnboxedValuePod(TStringValue("test")));
    arrayBuilder->Add(TUnboxedValuePod(TStringValue("1234"), /* size */ 3, /* offset */ 1));
    const auto datum = arrayBuilder->Build(true);
    UNIT_ASSERT(datum.is_array());
    UNIT_ASSERT_VALUES_EQUAL(datum.length(), 2);

    const auto blockReader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), resourceType);
    const auto item1AfterRead = blockReader->GetItem(*datum.array(), 0);
    const auto item2AfterRead = blockReader->GetItem(*datum.array(), 1);

    UNIT_ASSERT_VALUES_EQUAL(item1AfterRead.GetStringRefFromValue(), "test");
    UNIT_ASSERT_VALUES_EQUAL(item2AfterRead.GetStringRefFromValue(), "234");
}

Y_UNIT_TEST(TestSingularTypeValueBuilderReader) {
    TArrayBuilderTestData data;
    const auto nullType = data.PgmBuilder.NewNullType();

    std::shared_ptr<arrow::ArrayData> arrayData = NYql::NUdf::MakeSingularArray(/*isNull=*/true, 42);
    IArrayBuilder::TArrayDataItem arrayDataItem = {.Data = arrayData.get(), .StartOffset = 0};
    {
        const auto arrayBuilder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), nullType, *data.ArrowPool, MAX_BLOCK_SIZE, /*pgBuilder=*/nullptr);
        // Check builder.
        arrayBuilder->Add(TUnboxedValuePod());
        arrayBuilder->Add(TBlockItem());
        arrayBuilder->Add(TBlockItem(), 4);
        TInputBuffer inputBuffer("Just arbitrary string");
        arrayBuilder->Add(inputBuffer);
        UNIT_ASSERT_VALUES_EQUAL_C(inputBuffer.PopChar(), 'J', "The input buffer must not be consumed.");
        arrayBuilder->AddMany(*arrayData, /*popCount=*/3u, /*sparseBitmap=*/nullptr, /*bitmapSize=*/arrayData->length);
        arrayBuilder->AddMany(&arrayDataItem, /*arrayCount=*/1, /*beginIndex=*/1, /*count=*/3u);
        std::vector<ui64> indexes = {1, 5, 7, 10};
        arrayBuilder->AddMany(&arrayDataItem, /*arrayCount=*/1, /*indexes=*/indexes.data(), /*count=*/4u);
        UNIT_ASSERT_VALUES_EQUAL(arrayBuilder->Build(true).array()->length, 1 + 1 + 4 + 1 + 3 + 3 + 4);
    }

    {
        // Check reader.
        const auto blockReader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), nullType);

        UNIT_ASSERT(!blockReader->GetItem(*arrayData, 0));
        UNIT_ASSERT(!blockReader->GetScalarItem(*NYql::NUdf::MakeSingularScalar(/*isNull=*/true)));
        UNIT_ASSERT_EQUAL(blockReader->GetDataWeight(*arrayData), 0);
        UNIT_ASSERT_EQUAL(blockReader->GetDataWeight(TBlockItem()), 0);
        UNIT_ASSERT_EQUAL(blockReader->GetDefaultValueWeight(), 0);
        UNIT_ASSERT_EQUAL(blockReader->GetDefaultValueWeight(), 0);

        TOutputBuffer outputBuffer;
        blockReader->SaveItem(*arrayData, 1, outputBuffer);
        UNIT_ASSERT(outputBuffer.Finish().empty());
        blockReader->SaveScalarItem(*NYql::NUdf::MakeSingularScalar(/*isNull=*/true), outputBuffer);
        UNIT_ASSERT(outputBuffer.Finish().empty());
    }
}

Y_UNIT_TEST(TestBuilderAllocatedSize) {
    TArrayBuilderTestData data;
    const auto optStringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, true);
    const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
    const auto structType = data.PgmBuilder.NewStructType({{"a", optStringType}, {"b", int64Type}});
    const auto optStructType = data.PgmBuilder.NewOptionalType(structType);
    const auto doubleOptStructType = data.PgmBuilder.NewOptionalType(optStructType);

    size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(doubleOptStructType);
    size_t blockLen = NMiniKQL::CalcBlockLen(itemSize);
    Y_ENSURE(blockLen > 8);

    size_t bigStringSize = NMiniKQL::MaxBlockSizeInBytes / 8;
    size_t hugeStringSize = NMiniKQL::MaxBlockSizeInBytes * 2;

    const TString bString(bigStringSize, 'a');
    TBlockItem strItem1(bString);
    TBlockItem intItem1(1);
    std::array<TBlockItem, 2> sItems1 = {strItem1, intItem1};
    TBlockItem sItem1(sItems1.data());

    const TBlockItem bigItem = sItem1.MakeOptional();

    const TString hString(hugeStringSize, 'b');
    TBlockItem strItem2(hString);
    TBlockItem intItem2(2);
    std::array<TBlockItem, 2> sItems2 = {strItem2, intItem2};
    TBlockItem sItem2(sItems2.data());

    const TBlockItem hugeItem = sItem2.MakeOptional();

    const size_t stringAllocStep =
        arrow::BitUtil::RoundUpToMultipleOf64(blockLen + 1) +       // String NullMask
        arrow::BitUtil::RoundUpToMultipleOf64((blockLen + 1) * 4) + // String Offsets
        NMiniKQL::MaxBlockSizeInBytes;                              // String Data
    const size_t initialAllocated =
        stringAllocStep +
        arrow::BitUtil::RoundUpToMultipleOf64((blockLen + 1) * 8) + // Int64 Data
        2 * arrow::BitUtil::RoundUpToMultipleOf64(blockLen + 1);    // Double Optional

    size_t totalAllocated = 0;
    auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), doubleOptStructType, *data.ArrowPool, blockLen, nullptr, &totalAllocated);
    UNIT_ASSERT_VALUES_EQUAL(totalAllocated, initialAllocated);

    for (ui32 i = 0; i < 8; ++i) {
        builder->Add(bigItem);
    }
    UNIT_ASSERT_VALUES_EQUAL(totalAllocated, initialAllocated);
    // string data block is fully used here

    size_t beforeBlockBoundary = totalAllocated;
    builder->Add(bigItem);
    UNIT_ASSERT_VALUES_EQUAL(totalAllocated, beforeBlockBoundary + stringAllocStep);

    // string data block is partially used
    size_t beforeHugeString = totalAllocated;
    builder->Add(hugeItem);
    UNIT_ASSERT_VALUES_EQUAL(totalAllocated, beforeHugeString + stringAllocStep + hugeStringSize - NMiniKQL::MaxBlockSizeInBytes);

    totalAllocated = 0;
    builder->Build(false);
    UNIT_ASSERT_VALUES_EQUAL(totalAllocated, initialAllocated);
}
} // Y_UNIT_TEST_SUITE(TArrayBuilderTest)
