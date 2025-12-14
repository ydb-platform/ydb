#pragma once

#include "model_interface.h"

#include <ydb/public/lib/ydb_cli//commands/interactive/common/api_utils.h>
#include <ydb/public/lib/ydb_cli//commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient::NAi {

class TModelBase : public IModel {
public:
    TModelBase(const TString& apiUrl, const TString& authToken, const TInteractiveLogger& log);

    TResponse HandleMessages(const std::vector<TMessage>& messages, std::function<void()> onStartWaiting = {}, std::function<void()> onFinishWaiting = {}) final;

    void AddMessages(const std::vector<TMessage>& messages) final;

protected:
    virtual void AdvanceConversation(const std::vector<TMessage>& messages) = 0;

    virtual TResponse HandleModelResponse(const NJson::TJsonValue& response) = 0;

protected:
    NJson::TJsonValue ChatCompletionRequest;

private:
    const TInteractiveLogger Log;
    THttpExecutor HttpExecutor;
};

} // namespace NYdb::NConsoleClient::NAi
