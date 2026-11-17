#pragma once

#include <yql/essentials/public/udf_meta/abi/udf_meta.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

#include <memory>

namespace NJson {
class TJsonValue;
} // namespace NJson

namespace NYql {

class TUdfMeta {
public:
    struct TMeta {
        bool IsTypeAwareness = false;
        TStringBuf CallableType;
        TStringBuf RunConfigType;
        ui32 ArgCount = 0;
        ui32 OptionalArgCount = 0;
        bool IsStrict = false;
        bool SupportsBlocks = false;
        TLangVersion MinLangVer = UnknownLangVersion;
        TLangVersion MaxLangVer = UnknownLangVersion;
        TStringBuf PolyArgs;
        TStringBuf ResolvedCallableTypes;
    };

    // Adopts an ABI cookie, or borrows it for a synchronous call when owns is false.
    explicit TUdfMeta(TYqlUdfMeta abi, bool owns = true);
    ~TUdfMeta();
    TUdfMeta(TUdfMeta&& other) noexcept;
    TUdfMeta(const TUdfMeta&) = delete;
    TUdfMeta& operator=(const TUdfMeta&) = delete;

    // Module and function names must be lowercase. Views returned by GetMetadata
    // remain valid until the owning metadata cookie is destroyed.
    bool HasModule(TStringBuf module) const;
    bool HasFunction(TStringBuf module, TStringBuf function) const;
    TMaybe<TMeta> GetMetadata(TStringBuf module, TStringBuf function) const;

    const TYqlUdfMeta& GetAbi() const;

private:
    TYqlUdfMeta Abi_;
    bool Owns_;
};

std::unique_ptr<TUdfMeta> ParseUdfMeta(const NJson::TJsonValue& json);

} // namespace NYql
