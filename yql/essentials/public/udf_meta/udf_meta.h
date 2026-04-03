#pragma once

#include <library/cpp/json/json_reader.h>

namespace NYql {

class IUdfMeta {
public:
    virtual ~IUdfMeta() = default;

    // module & function should be lowercased
    virtual bool HasModule(TStringBuf module) const = 0;
    virtual bool HasFunction(TStringBuf module, TStringBuf function) const = 0;
};

std::unique_ptr<IUdfMeta> ParseUdfMeta(const NJson::TJsonValue& json);

} // namespace NYql
