#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class ITool {
public:
    using TPtr = std::shared_ptr<ITool>;

    virtual ~ITool() = default;

    struct TResponse {
        // Response will be sent into API in order ToolResult -> UserMessage
        TString UserMessage;
        TString ToolResult;
        bool IsSuccess = true;

        explicit TResponse(const TString& error, const TString& userMessage = "");

        explicit TResponse(const NJson::TJsonValue& result, const TString& userMessage = "");
    };

    virtual const NJson::TJsonValue& GetParametersSchema() const = 0;

    virtual const TString& GetDescription() const = 0;

    virtual TResponse Execute(const NJson::TJsonValue& parameters) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
