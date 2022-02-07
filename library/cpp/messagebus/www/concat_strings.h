#pragma once

#include <util/generic/string.h>
#include <util/stream/str.h>

// ATTN: not equivalent to TString::Join - cat concat anything "outputable" to stream, not only TString convertable types.

inline void DoConcatStrings(TStringStream&) {
}

template <class T, class... R>
inline void DoConcatStrings(TStringStream& ss, const T& t, const R&... r) {
    ss << t;
    DoConcatStrings(ss, r...);
}

template <class... R>
inline TString ConcatStrings(const R&... r) {
    TStringStream ss;
    DoConcatStrings(ss, r...);
    return ss.Str();
}
