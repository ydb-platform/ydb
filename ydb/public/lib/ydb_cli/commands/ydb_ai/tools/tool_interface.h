#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class ITool {
public:
    using TPtr = std::shared_ptr<ITool>;

    virtual ~ITool() = default;

    struct TResponse {
        TString Text;
        bool IsSuccess = true;

        explicit TResponse(const TString& error);

        explicit TResponse(const NJson::TJsonValue& result);
    };

    virtual const NJson::TJsonValue& GetParametersSchema() const = 0;

    virtual const TString& GetDescription() const = 0;

    virtual TResponse Execute(const NJson::TJsonValue& parameters) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
