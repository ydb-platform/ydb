#include "mkql_unboxed_value_stream.h"

#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

TUnboxedValueStream::TUnboxedValueStream()
    : Value_(NUdf::TUnboxedValuePod::Zero())
{
}

NUdf::TUnboxedValuePod TUnboxedValueStream::Value() {
    return Value_.Release();
}

void TUnboxedValueStream::DoWrite(const void* buf, size_t len) {
    Value_ = AppendString(Value_.Release(), NUdf::TStringRef(static_cast<const char*>(buf), len));
}

} // namespace NKikimr::NMiniKQL
