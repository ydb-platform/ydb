#pragma once

#include "thread_extra.h"

#include <util/generic/vector.h>
#include <util/system/yassert.h>

template <typename T, typename TTag = void, template <typename, class> class TVectorType = TVector>
class TTempTlsVector {
private:
    struct TTagForTls {};

    TVectorType<T, std::allocator<T>>* Vector;

public:
    TVectorType<T, std::allocator<T>>* GetVector() {
        return Vector;
    }

    TTempTlsVector() {
        Vector = FastTlsSingletonWithTag<TVectorType<T, std::allocator<T>>, TTagForTls>();
        Y_ASSERT(Vector->empty());
    }

    ~TTempTlsVector() {
        Clear();
    }

    void Clear() {
        Vector->clear();
    }

    size_t Capacity() const noexcept {
        return Vector->capacity();
    }

    void Shrink() {
        Vector->shrink_to_fit();
    }
};
