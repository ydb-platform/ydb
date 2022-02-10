#include "yql_ydb_dq_transform.h"

#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/ydb/proto/range.pb.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TYdbDqTaskTransform {
public:
    TYdbDqTaskTransform(const THashMap<TString, TString>& taskParams, const IFunctionRegistry& functionRegistry)
        : TaskParams(std::move(taskParams))
        , FunctionRegistry(functionRegistry)
    {
    }

    TCallableVisitFunc operator()(TInternName name) {
        if (TaskParams.contains("ydb") && name.Str().starts_with("KikScan")) {
            return [this](TCallable& callable, const TTypeEnvironment& env) {
                const auto part = TaskParams.Value("ydb", TString());
                TStringInput in(part);
                NYdb::TKeyRange range;
                range.Load(&in);

                TProgramBuilder pgmBuilder(env, FunctionRegistry);
                TCallableBuilder callableBuilder(env, callable.GetType()->GetName(), callable.GetType()->GetReturnType(), false);
                callableBuilder.Add(callable.GetInput(0));
                callableBuilder.Add(callable.GetInput(1));
                callableBuilder.Add(callable.GetInput(2));
                callableBuilder.Add(callable.GetInput(3));
                callableBuilder.Add(callable.GetInput(4));
                callableBuilder.Add(callable.GetInput(5));
                callableBuilder.Add(callable.GetInput(6));
                callableBuilder.Add(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(range.from_key()));
                callableBuilder.Add(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(range.to_key()));
                callableBuilder.Add(callable.GetInput(9));

                return TRuntimeNode(callableBuilder.Build(), false);
            };
        }

        return TCallableVisitFunc();
    }

private:
    const THashMap<TString, TString> TaskParams;
    const IFunctionRegistry& FunctionRegistry;
};

TTaskTransformFactory CreateYdbDqTaskTransformFactory() {
    return [] (const THashMap<TString, TString>& taskParams, const IFunctionRegistry* funcRegistry) -> TCallableVisitFuncProvider {
        return TYdbDqTaskTransform(taskParams, *funcRegistry);
    };
}

} // NYql

