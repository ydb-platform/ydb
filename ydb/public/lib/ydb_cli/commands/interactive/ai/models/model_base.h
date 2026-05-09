#pragma once

#include "model_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>

namespace NYdb::NConsoleClient::NAi {

class TModelBase : public IModel {
public:
    TModelBase(const TString& apiUrl, const TString& authToken);

    TResponse HandleMessages(const std::vector<TMessage>& messages, IResponseProcessor& responseProcessor) final;

    void AddMessages(const std::vector<TMessage>& messages) final;

protected:
    virtual void AdvanceConversation(const std::vector<TMessage>& messages) = 0;

    virtual TResponse HandleModelResponse(const NJson::TJsonValue& response) = 0;

    virtual bool UseStreaming() = 0;

    virtual void ConsumeStreamEvent(TStringBuf eventJson, NJson::TJsonValue& assembled, IResponseProcessor& responseProcessor) = 0;

    virtual NJson::TJsonValue FinalizeStreamingResponse(NJson::TJsonValue&& assembled) = 0;

protected:
    const TString CacheKey;
    NJson::TJsonValue ChatCompletionRequest;

private:
    NJson::TJsonValue PerformNonStreamingRequest(TString&& request);

    NJson::TJsonValue PerformStreamingRequest(TString&& request, IResponseProcessor& responseProcessor);

    THttpExecutor HttpExecutor;
};

} // namespace NYdb::NConsoleClient::NAi
