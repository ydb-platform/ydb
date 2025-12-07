#pragma once

#include "model_interface.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/public/lib/ydb_cli//commands/interactive/common/interactive_log.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

class TModelBase : public IModel {
public:
    TModelBase(const TString& apiUrl, const TString& authToken, const TInteractiveLogger& log);

    TResponse HandleMessages(const std::vector<TMessage>& messages) final;

protected:
    virtual void AdvanceConversation(const std::vector<TMessage>& messages) = 0;

    virtual TResponse HandleModelResponse(const NJson::TJsonValue& response) = 0;

    virtual TString HandleErrorResponse(ui64 httpCode, const TString& response);

protected:
    NJson::TJsonValue ChatCompletionRequest;

private:
    const TInteractiveLogger Log;
    const TString ApiUrl;
    const NYql::THttpHeader ApiHeaders;
    const NYql::IHTTPGateway::TPtr HttpGateway;
};

} // namespace NYdb::NConsoleClient::NAi
