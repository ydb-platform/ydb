#pragma once

#include "model_interface.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

class TModelBase : public IModel {
protected:
    enum EVerboseLevel {
        VERB_INFO = 1,
        VERB_TRACE = 2,
    };

public:
    TModelBase(const TString& apiUrl, const std::optional<TString>& authToken, const TClientCommand::TConfig& config);

    TResponse HandleMessages(const std::vector<TMessage>& messages) final;

protected:
    virtual void AdvanceConversation(const std::vector<TMessage>& messages) = 0;

    virtual TResponse HandleModelResponse(const NJson::TJsonValue& response) = 0;

    virtual TString HandleErrorResponse(ui64 httpCode, const TString& response);

    static TString CreateApiUrl(const TString& baseUrl, const TString& uri);

protected:
    NJson::TJsonValue ChatCompletionRequest;

private:
    const ui64 Verbosity = 0;
    const TString ApiUrl;
    const NYql::THttpHeader ApiHeaders;
    const NYql::IHTTPGateway::TPtr HttpGateway;
};

} // namespace NYdb::NConsoleClient::NAi
