#pragma once

#include <library/cpp/scheme/scheme.h>

#include <util/generic/strbuf.h>
#include <util/ysaveload.h>

namespace NScUtils {

void CopyField(const NSc::TValue& from, NSc::TValue& to);

template <typename... Args>
void CopyField(const NSc::TValue& from, NSc::TValue& to, TStringBuf path, Args... args) {
    CopyField(from[path], to[path], args...);
}

} // namespace NScUtils

template<>
struct TSerializer<NSc::TValue> {
    static void Save(IOutputStream* out, const NSc::TValue& v);
    static void Load(IInputStream* in, NSc::TValue& v);
};
