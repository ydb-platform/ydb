#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

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
        , ArrowPool(arrow::default_memory_pool())
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
        *data.ArrowPool, MAX_BLOCK_SIZE, /* pgBuilder */nullptr);
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

    extern const char ResourceName[] = "Resource.Name";
    Y_UNIT_TEST(TestDtorCall) {
        TArrayBuilderTestData data;
        const auto resourceType = data.PgmBuilder.NewResourceType("Test.Resource");
        const auto arrayBuilder = MakeResourceArrayBuilder(resourceType, data);

        auto destructorCallsCnt = std::make_shared<int>(0);

        struct TWithDtor {
            int Payload;
            std::shared_ptr<int> DestructorCallsCnt;
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
}