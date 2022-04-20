#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/function/common/dq_function_types.h>
#include <library/cpp/threading/future/future.h>

#include <unordered_map>

namespace NYql {

using namespace NDqFunction;

class IDqFunctionGateway {
public:
    using TPtr = std::shared_ptr<IDqFunctionGateway>;

    virtual NThreading::TFuture<TDqFunctionDescription> ResolveFunction(const TString& folderId, const TString& functionName) = 0;
    virtual ~IDqFunctionGateway() = default;
};

class TDqFunctionGatewayFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDqFunctionGatewayFactory>;

    TDqFunctionGatewayFactory() {}

    using TGatewayCreator = std::function<IDqFunctionGateway::TPtr(
            const THashMap<TString, TString>& secureParams, const TString& connection)>;

    void Register(const TDqFunctionType& type, TGatewayCreator creator);
    bool IsKnownFunctionType(const TDqFunctionType& type);
    IDqFunctionGateway::TPtr CreateDqFunctionGateway(const TDqFunctionType& type,
                                                     const THashMap<TString, TString>& secureParams,
                                                     const TString& connection) const;
private:
    std::unordered_map<TDqFunctionType, TGatewayCreator> CreatorsByType;
};


}


