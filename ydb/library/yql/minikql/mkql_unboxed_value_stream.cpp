#include "mkql_unboxed_value_stream.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

TUnboxedValueStream::TUnboxedValueStream()
    : Value_(NUdf::TUnboxedValuePod::Zero())
{}

NUdf::TUnboxedValuePod TUnboxedValueStream::Value() {
    return Value_.Release();
}

void TUnboxedValueStream::DoWrite(const void* buf, size_t len) {
    Value_ = AppendString(Value_.Release(), NUdf::TStringRef(static_cast<const char*>(buf), len));
}

}
}
