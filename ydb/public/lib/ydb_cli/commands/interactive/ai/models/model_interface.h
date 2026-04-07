#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class IModel {
public:
    using TPtr = std::shared_ptr<IModel>;

    virtual ~IModel() = default;

    struct TUserMessage {
        TString Text;
    };

    struct TToolResponse {
        TString Text;
        TString ToolCallId;
        bool IsSuccess = true;
    };

    using TMessage = std::variant<TUserMessage, TToolResponse>;

    struct TResponse {
        struct TToolCall {
            TString Id;
            TString Name;
            NJson::TJsonValue Parameters;
        };

        TString Text;
        std::vector<TToolCall> ToolCalls;
    };

    virtual TResponse HandleMessages(const std::vector<TMessage>& messages, std::function<void()> onStartWaiting = {}, std::function<void()> onFinishWaiting = {}) = 0;

    virtual void AddMessages(const std::vector<TMessage>& messages) = 0;

    virtual void RegisterTool(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description) = 0;

    virtual void ClearContext() = 0;
};

} // namespace NYdb::NConsoleClient::NAi
