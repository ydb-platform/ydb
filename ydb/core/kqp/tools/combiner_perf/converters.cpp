#include "converters.h"

namespace NKikimr {
namespace NMiniKQL {

template<>
std::string UnboxedToNative(const NUdf::TUnboxedValue& result)
{
    const NUdf::TStringRef val = result.AsStringRef();
    return std::string(val.data(), val.size());
}

}
}