#pragma once

#include <library/cpp/json/json_reader.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NYql {

class IUdfMeta {
public:
    virtual ~IUdfMeta() = default;

    struct TMeta {
        bool IsTypeAwareness = false;
        TString CallableType;
        TString RunConfigType;
        ui32 ArgCount = 0;
        ui32 OptionalArgCount = 0;
        bool IsStrict = false;
        bool SupportsBlocks = false;
        TLangVersion MinLangVer = UnknownLangVersion;
        TLangVersion MaxLangVer = UnknownLangVersion;
    };

    // module & function should be lowercased
    virtual bool HasModule(TStringBuf module) const = 0;
    virtual bool HasFunction(TStringBuf module, TStringBuf function) const = 0;
    virtual const TMeta* GetMetadata(TStringBuf module, TStringBuf function) const = 0;
};

std::unique_ptr<IUdfMeta> ParseUdfMeta(const NJson::TJsonValue& json);

} // namespace NYql
