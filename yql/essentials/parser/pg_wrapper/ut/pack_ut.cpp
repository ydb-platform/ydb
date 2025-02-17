#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TPGPackTests) {
    Y_UNIT_TEST(UnknownTypeAsString) {
        using namespace NKikimr::NMiniKQL;
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TIntrusivePtr<IFunctionRegistry> functionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()));
        TProgramBuilder pgmBuilder(env, *functionRegistry);
        TMemoryUsageInfo memInfo("Memory");
        THolderFactory holderFactory(alloc.Ref(), memInfo, functionRegistry.Get());

        auto pgType = pgmBuilder.NewPgType(0xffffffff);
        TValuePacker pgPacker(false, pgType);

        NUdf::TUnboxedValue s = MakeString(NUdf::TStringRef::Of("foo"));
        auto p = pgPacker.Pack(s);
        auto u = pgPacker.Unpack(p, holderFactory);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(u.AsStringRef()), "foo");
    }
}

}
