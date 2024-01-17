#include "yql_common_dq_transform.h"

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/actors/core/actor.h>
#include <util/stream/file.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TCommonDqTaskTransform {
public:
    TCommonDqTaskTransform(const IFunctionRegistry& functionRegistry)
        : FunctionRegistry(functionRegistry)
    {
    }

    TCallableVisitFunc operator()(TInternName name) {
        if (name == "CurrentActorId") {
            return [this](TCallable& callable, const TTypeEnvironment& env) {
                Y_UNUSED(callable);
                auto selfId = NActors::TActivationContext::AsActorContext().SelfID;
                TProgramBuilder pgmBuilder(env, FunctionRegistry);
                return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(selfId.ToString());
            };
        }
        if (name == "FileContentJob") {
            return [](TCallable& callable, const TTypeEnvironment& env) {
                YQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 argument");
                const TString path(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                auto content = TFileInput(path).ReadAll();
                return TRuntimeNode(BuildDataLiteral(content, NUdf::TDataType<char*>::Id, env), true);
            };
        }

        return TCallableVisitFunc();
    }

private:
    const IFunctionRegistry& FunctionRegistry;
};

TTaskTransformFactory CreateCommonDqTaskTransformFactory() {
    return [] (const THashMap<TString, TString>& taskParams, const IFunctionRegistry* funcRegistry) -> TCallableVisitFuncProvider {
        Y_UNUSED(taskParams);
        return TCommonDqTaskTransform(*funcRegistry);
    };
}

} // NYql

