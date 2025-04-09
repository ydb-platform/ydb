#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/string_utils/helpers/helpers.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <optional>

namespace NYdb {
inline namespace Dev {
namespace NUtils {

template <class TContainer, class T>
bool ContainerTransform(TContainer& str, T&& f, size_t pos = 0, size_t n = TContainer::npos) {
    size_t len = str.size();
    if (pos > len) {
        pos = len;
    }
    if (n > len - pos) {
        n = len - pos;
    }

    bool changed = false;

    for (size_t i = pos; i != pos + n; ++i) {
        auto c = f(i, str[i]);
        if (c != str[i]) {
            changed = true;
            str[i] = c;
        }
    }
    return changed;
}

template <class P, class D>
void GetNext(std::string_view& s, D delim, P& param) {
    std::string_view next;
    Y_ENSURE(NUtils::NextTok(s, next, delim), "Split: number of fields less than number of Split output arguments");
    param = FromString<P>(next);
}

template <class P, class D>
void GetNext(std::string_view& s, D delim, std::optional<P>& param) {
    std::string_view next;
    if (NUtils::NextTok(s, next, delim)) {
        param = FromString<P>(next);
    } else {
        param.reset();
    }
}

template <class D>
void GetNext(std::string_view& s, D delim, std::optional<std::string_view>& param) {
    std::string_view next;
    if (NUtils::NextTok(s, next, delim)) {
        param = next;
    } else {
        param.reset();
    }
}

}
}
}