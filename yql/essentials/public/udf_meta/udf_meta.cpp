#include "udf_meta.h"

#include <util/generic/yexception.h>

#include <utility>

namespace NYql {

TUdfMeta::TUdfMeta(TYqlUdfMeta abi, bool owns)
    : Abi_(abi)
    , Owns_(owns)
{
    Y_ENSURE(Abi_.Context && Abi_.Free && Abi_.HasModule && Abi_.HasFunction && Abi_.GetMetadata,
             "Incomplete UDF metadata ABI cookie");
}

TUdfMeta::~TUdfMeta() {
    if (Owns_) {
        Abi_.Free(Abi_.Context);
    }
}

TUdfMeta::TUdfMeta(TUdfMeta&& other) noexcept
    : Abi_(other.Abi_)
    , Owns_(std::exchange(other.Owns_, false))
{
}

bool TUdfMeta::HasModule(TStringBuf module) const {
    const int result = Abi_.HasModule(Abi_.Context, {module.data(), module.size()});
    Y_ENSURE(result >= 0, "UDF module lookup failed");
    return result != 0;
}

bool TUdfMeta::HasFunction(TStringBuf module, TStringBuf function) const {
    const int result = Abi_.HasFunction(Abi_.Context, {module.data(), module.size()}, {function.data(), function.size()});
    Y_ENSURE(result >= 0, "UDF function lookup failed");
    return result != 0;
}

TMaybe<TUdfMeta::TMeta> TUdfMeta::GetMetadata(TStringBuf module, TStringBuf function) const {
    TYqlUdfMetaFunction value{};
    const int result = Abi_.GetMetadata(Abi_.Context, {module.data(), module.size()}, {function.data(), function.size()}, &value);
    Y_ENSURE(result >= 0, "UDF metadata lookup failed");
    if (!result) {
        return Nothing();
    }
    return TMeta{
        .IsTypeAwareness = value.IsTypeAwareness != 0,
        .CallableType = {value.CallableType.Data, value.CallableType.Size},
        .RunConfigType = {value.RunConfigType.Data, value.RunConfigType.Size},
        .ArgCount = value.ArgCount,
        .OptionalArgCount = value.OptionalArgCount,
        .IsStrict = value.IsStrict != 0,
        .SupportsBlocks = value.SupportsBlocks != 0,
        .MinLangVer = value.MinLangVer,
        .MaxLangVer = value.MaxLangVer,
        .PolyArgs = {value.PolyArgs.Data, value.PolyArgs.Size},
        .ResolvedCallableTypes = {value.ResolvedCallableTypes.Data, value.ResolvedCallableTypes.Size},
    };
}

const TYqlUdfMeta& TUdfMeta::GetAbi() const {
    return Abi_;
}

} // namespace NYql
