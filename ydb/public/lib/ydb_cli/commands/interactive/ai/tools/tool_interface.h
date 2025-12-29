#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class ITool {
public:
    using TPtr = std::shared_ptr<ITool>;

    virtual ~ITool() = default;

    class TResponse {
    public:
        // Response will be sent into API in order ToolResult -> UserMessage
        TString UserMessage;
        TString ToolResult;
        bool IsSuccess = true;

        static TResponse Error(const TString& error, const TString& userMessage = "");

        static TResponse Success(const TString& result, const TString& userMessage = "");

        static TResponse Success(const NJson::TJsonValue& result, const TString& userMessage = "");

    private:
        TResponse(const TString& result, const TString& userMessage, bool isSuccess);
    };

    virtual const NJson::TJsonValue& GetParametersSchema() const = 0;

    virtual const TString& GetDescription() const = 0;

    virtual TResponse Execute(const NJson::TJsonValue& parameters) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
