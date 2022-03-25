#pragma once

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

namespace NYql {
class IHTTPMockGateway : public IHTTPGateway {
public:
    using TPtr = std::shared_ptr<IHTTPMockGateway>;

    virtual ~IHTTPMockGateway() = default;

    static TPtr Make();

    using TDataDefaultResponse = std::function<IHTTPGateway::TResult(TString, IHTTPGateway::THeaders, TString)>;
    using TDataResponse = std::function<IHTTPGateway::TResult()>;

    virtual void AddDefaultResponse(TDataDefaultResponse responseCallback) = 0;

    virtual void AddDownloadResponse(
            TString url,
            IHTTPGateway::THeaders headers,
            TString data,
            TDataResponse responseCallback) = 0;
};
}