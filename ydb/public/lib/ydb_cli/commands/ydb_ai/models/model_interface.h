#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class IModel {
public:
    using TPtr = std::shared_ptr<IModel>;

    virtual ~IModel() = default;

    struct TRequest {
        TString Text;
        std::optional<TString> ToolCallId;
    };

    struct TResponse {
        struct TToolCall {
            TString Id;
            TString Name;
            NJson::TJsonValue Parameters;
        };

        std::optional<TString> Text;
        std::vector<TToolCall> ToolCalls;
    };

    virtual TResponse HandleMessages(const std::vector<NAi::IModel::TRequest>& requests) = 0;

    virtual void RegisterTool(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
