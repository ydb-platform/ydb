#include "yql_common_dq_transform.h" 
 
#include <ydb/library/yql/minikql/mkql_function_registry.h> 
#include <ydb/library/yql/minikql/mkql_program_builder.h> 
#include <ydb/library/yql/minikql/mkql_node_cast.h> 
 
#include <library/cpp/actors/core/actor.h> 
 
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
 
