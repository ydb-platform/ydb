#include "dq_function_gateway.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql {

    void TDqFunctionGatewayFactory::Register(const TDqFunctionType& type, TGatewayCreator creator) {
        auto [_, registered] = CreatorsByType.emplace(type, std::move(creator));
        Y_ABORT_UNLESS(registered);
    }

    bool TDqFunctionGatewayFactory::IsKnownFunctionType(const TDqFunctionType& type) {
        return !type.empty() && CreatorsByType.contains(type);
    }

    void TDqFunctionGatewayFactory::RegisterServiceFunction(
        const TString& module, const TString& method, TServiceFunctionSignature signature) {
        YQL_ENSURE(CreatorsByType.contains(module), "Service module gateway is not registered");
        YQL_ENSURE(!module.empty() && !method.empty(), "Empty service function name");
        YQL_ENSURE(ServiceFunctions.emplace(module + "." + method, std::move(signature)).second,
                   "Duplicate service function signature");
    }

    const TServiceFunctionSignature* TDqFunctionGatewayFactory::FindServiceFunction(const TString& qualifiedName) const {
        const auto it = ServiceFunctions.find(qualifiedName);
        return it == ServiceFunctions.end() ? nullptr : &it->second;
    }

    IDqFunctionGateway::TPtr TDqFunctionGatewayFactory::CreateDqFunctionGateway(
        const TDqFunctionType& type, const THashMap<TString, TString>& secureParams,
        const TString& connection) const {
        auto creator = CreatorsByType.find(type);
        if (creator == CreatorsByType.end()) {
            YQL_ENSURE(false, "Unregistered external function gateway type " << type);
        }
        return (creator->second)(secureParams, connection);
    }

} // namespace NYql
