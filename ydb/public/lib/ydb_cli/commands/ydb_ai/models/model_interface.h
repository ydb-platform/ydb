#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class IModel {
public:
    using TPtr = std::shared_ptr<IModel>;

    virtual ~IModel() = default;

    struct TResponse {
        struct TToolCall {
            TString Id;
            TString Name;
            NJson::TJsonValue Parameters;
        };

        std::optional<TString> Text;
        std::optional<TToolCall> ToolCall;
    };

    virtual TResponse HandleMessage(const TString& input, std::optional<TString> toolCallId = std::nullopt) = 0;

    virtual void RegisterTool(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
