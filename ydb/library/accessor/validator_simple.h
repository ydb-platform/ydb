#pragma once
#include <util/system/yassert.h>

class TSimpleValidator {
public:
    template <class T>
    static const T& CheckNotNull(const T& object) {
        Y_ABORT_UNLESS(!!object);
        return object;
    }
    template <class T>
    static T&& CheckNotNull(T&& object) {
        Y_ABORT_UNLESS(!!object);
        return std::forward<T>(object);
    }
};
