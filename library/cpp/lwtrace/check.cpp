#include "check.h"

#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NLWTrace {
    int TCheck::ObjCount = 0;
}

template <>
NLWTrace::TCheck FromStringImpl(const char* data, size_t len) {
    return NLWTrace::TCheck(FromString<int, char>(data, len));
}

template <>
void Out<NLWTrace::TCheck>(IOutputStream& o, TTypeTraits<NLWTrace::TCheck>::TFuncParam t) {
    Out<int>(o, t.Value);
}
