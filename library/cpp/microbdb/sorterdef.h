#pragma once

#define MAKESORTERTMPL(TRecord, MemberFunc)                       \
    template <typename T>                                         \
    struct MemberFunc;                                            \
    template <>                                                   \
    struct MemberFunc<TRecord> {                                  \
        bool operator()(const TRecord* l, const TRecord* r) {     \
            return TRecord ::MemberFunc(l, r) < 0;                \
        }                                                         \
        int operator()(const TRecord* l, const TRecord* r, int) { \
            return TRecord ::MemberFunc(l, r);                    \
        }                                                         \
    }

template <typename T>
static inline int compare(const T& a, const T& b) {
    return (a < b) ? -1 : (a > b);
}
