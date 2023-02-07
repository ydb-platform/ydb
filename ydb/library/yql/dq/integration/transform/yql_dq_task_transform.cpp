#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>

namespace NYql {

TTaskTransformFactory CreateCompositeTaskTransformFactory(TVector<TTaskTransformFactory> factories) {
    return [factories = std::move(factories)] (const THashMap<TString, TString>& taskParams, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry) -> NKikimr::NMiniKQL::TCallableVisitFuncProvider {
        TVector<NKikimr::NMiniKQL::TCallableVisitFuncProvider> funcProviders;
        for (auto& factory: factories) {
            funcProviders.push_back(factory(taskParams, funcRegistry));
        }
        return [funcProviders = std::move(funcProviders)] (const NKikimr::NMiniKQL::TInternName& name) -> NKikimr::NMiniKQL::TCallableVisitFunc {
            for (auto& provider: funcProviders) {
                if (auto res = provider(name)) {
                    return res;
                }
            }
            return {};
        };
    };
}

} // NYql
