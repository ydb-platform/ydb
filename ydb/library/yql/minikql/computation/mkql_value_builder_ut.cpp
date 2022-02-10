#include "mkql_value_builder.h" 
#include "mkql_computation_node_holders.h" 
 
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <library/cpp/testing/unittest/registar.h>
 
namespace NKikimr { 
 
using namespace NUdf; 
 
namespace NMiniKQL { 
 
class TMiniKQLValueBuilderTest: public TTestBase { 
public: 
    TMiniKQLValueBuilderTest() 
        : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry())) 
        , Env(Alloc)
        , MemInfo("Memory") 
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get()) 
        , Builder(HolderFactory) 
    { 
    } 
 
private: 
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry; 
    TScopedAlloc Alloc; 
    TTypeEnvironment Env; 
    TMemoryUsageInfo MemInfo; 
    THolderFactory HolderFactory; 
    TDefaultValueBuilder Builder; 
 
    UNIT_TEST_SUITE(TMiniKQLValueBuilderTest); 
        UNIT_TEST(TestEmbeddedVariant); 
        UNIT_TEST(TestBoxedVariant); 
        UNIT_TEST(TestSubstring); 
    UNIT_TEST_SUITE_END(); 
 
 
    void TestEmbeddedVariant() { 
        const auto v = Builder.NewVariant(62, TUnboxedValuePod((ui64) 42)); 
        UNIT_ASSERT(v); 
        UNIT_ASSERT(!v.IsBoxed()); 
        UNIT_ASSERT_VALUES_EQUAL(62, v.GetVariantIndex()); 
        UNIT_ASSERT_VALUES_EQUAL(42, v.GetVariantItem().Get<ui64>()); 
    } 
 
    void TestBoxedVariant() { 
        const auto v = Builder.NewVariant(63, TUnboxedValuePod((ui64) 42)); 
        UNIT_ASSERT(v); 
        UNIT_ASSERT(v.IsBoxed()); 
        UNIT_ASSERT_VALUES_EQUAL(63, v.GetVariantIndex()); 
        UNIT_ASSERT_VALUES_EQUAL(42, v.GetVariantItem().Get<ui64>()); 
    } 
 
    void TestSubstring() { 
        const auto string = Builder.NewString("0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM"); 
        UNIT_ASSERT(string); 
 
        const auto zero = Builder.SubString(string, 7, 0); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(""), TStringBuf(zero.AsStringRef())); 
 
        const auto tail = Builder.SubString(string, 60, 8); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("NM"), TStringBuf(tail.AsStringRef())); 
 
        const auto small = Builder.SubString(string, 2, 14); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("23456789qwerty"), TStringBuf(small.AsStringRef())); 
 
        const auto one = Builder.SubString(string, 3, 15); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("3456789qwertyui"), TStringBuf(one.AsStringRef())); 
        UNIT_ASSERT_VALUES_EQUAL(string.AsStringValue().Data(), one.AsStringValue().Data()); 
 
        const auto two = Builder.SubString(string, 10, 30); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("qwertyuiopasdfghjklzxcvbnmQWER"), TStringBuf(two.AsStringRef())); 
        UNIT_ASSERT_VALUES_EQUAL(string.AsStringValue().Data(), two.AsStringValue().Data()); 
    } 
 
}; 
 
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLValueBuilderTest); 
 
} 
} 
