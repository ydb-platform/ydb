#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

using namespace NYql;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TestLocalGateway) {

std::pair<TIntrusivePtr<IDqGateway>,TIntrusivePtr<NMiniKQL::IFunctionRegistry>> CreateGateway()
{
    auto funcRegistry = CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());

    auto dqTaskTransformFactory = CreateCommonDqTaskTransformFactory();

    auto dqCompFactory = GetCommonDqFactory();

    TDqTaskPreprocessorFactoryCollection dqTaskPreprocessorFactories;

    Y_UNUSED(dqTaskTransformFactory);

    return {CreateLocalDqGateway(
        funcRegistry.Get(), 
        dqCompFactory, 
        dqTaskTransformFactory, 
        dqTaskPreprocessorFactories, 
        /*enableSpilling = */ false,
        MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>()), funcRegistry};
}

Y_UNIT_TEST(Create) {
    auto [gateway, _] = CreateGateway();
}

Y_UNIT_TEST(OpenSession) {
    auto [gateway, _] = CreateGateway();
    gateway->OpenSession("SessionId", "Username");
}

Y_UNIT_TEST(OpenSessionInLoop) {
    for (int i = 0; i < 10; i++) {
        Cerr << "Iteration: " << i << "\n";
        auto [gateway, _] = CreateGateway();
        gateway->OpenSession("SessionId", "Username");
    }
}

} // Y_UNIT_TEST_SUITE(TestLocalGateway)

