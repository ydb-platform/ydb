#pragma once

#include <util/generic/vector.h>
#include <util/generic/strbuf.h>
#include <util/string/subst.h>

/// Заменяет в строке одни подстроки на другие.
template <class TBuf, class TPool>
size_t SubstGlobal(TBuf& s, const TBuf& from, const TBuf& to, TPool& pool) {
    if (from.empty())
        return 0;

    TVector<size_t> offs;
    for (size_t off = 0; (off = s.find(from, off)) != TBuf::npos; off += from.length())
        offs.push_back(off);
    if (offs.empty())
        return 0;

    size_t dstSize = s.size() + ssize_t(offs.size()) * ssize_t(to.size() - from.size());
    const size_t charTypeSz = sizeof(typename TBuf::char_type);
    typename TBuf::char_type* dst = (typename TBuf::char_type*)pool.Allocate((dstSize + 1) * charTypeSz);
    dst[dstSize] = 0;

    typename TBuf::char_type* p = dst;
    size_t lastSrc = 0;
    for (auto off : offs) {
        memcpy(p, s.data() + lastSrc, (off - lastSrc) * charTypeSz);
        p += off - lastSrc;
        lastSrc = off + from.size();
        memcpy(p, to.data(), to.size() * charTypeSz);
        p += to.size();
    }
    memcpy(p, s.data() + lastSrc, (s.size() - lastSrc) * charTypeSz);
    p += s.size() - lastSrc;
    Y_ASSERT(p - dst == (ssize_t)dstSize);

    s = TBuf(dst, dstSize);
    return offs.size();
}

template <class TPool>
size_t SubstGlobal(TStringBuf& s, const TStringBuf& from, const TStringBuf& to, TPool& pool) {
    return SubstGlobal<TStringBuf, TPool>(s, from, to, pool);
}

/// Заменяет в строке одни подстроки на другие.
template <class TBuf, class TPool>
inline size_t SubstGlobal(TBuf& s, typename TBuf::char_type from, typename TBuf::char_type to, TPool& pool) {
    size_t result = 0;
    size_t off = s.find(from);
    if (off == TBuf::npos)
        return 0;

    s = TBuf(pool.Append(s), s.size());

    for (typename TBuf::char_type* it = const_cast<typename TBuf::char_type*>(s.begin()) + off; it != s.end(); ++it) {
        if (*it == from) {
            *it = to;
            ++result;
        }
    }
    return result;
}
