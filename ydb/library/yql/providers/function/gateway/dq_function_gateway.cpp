#include "dq_function_gateway.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

void TDqFunctionGatewayFactory::Register(const TDqFunctionType& type, TGatewayCreator creator) {
    auto [_, registered] = CreatorsByType.emplace(type, std::move(creator));
    Y_ABORT_UNLESS(registered);
}

bool TDqFunctionGatewayFactory::IsKnownFunctionType(const TDqFunctionType& type) {
    return !type.empty() && CreatorsByType.contains(type);
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

}