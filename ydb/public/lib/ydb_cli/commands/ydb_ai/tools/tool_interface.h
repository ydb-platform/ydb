#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/fwd.h>

#include <memory>

namespace NYdb::NConsoleClient::NAi {

class ITool {
public:
    using TPtr = std::shared_ptr<ITool>;

    virtual ~ITool() = default;

    virtual TString GetName() const = 0;

    virtual NJson::TJsonValue GetParametersSchema() const = 0;

    virtual TString GetDescription() const = 0;

    virtual TString Execute(const NJson::TJsonValue& parameters) = 0;
};

} // namespace NYdb::NConsoleClient::NAi
