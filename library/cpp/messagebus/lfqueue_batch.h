#pragma once

#include <library/cpp/messagebus/actor/temp_tls_vector.h>

#include <util/generic/vector.h>
#include <util/thread/lfstack.h>

template <typename T, template <typename, class> class TVectorType = TVector>
class TLockFreeQueueBatch {
private:
    TLockFreeStack<TVectorType<T, std::allocator<T>>*> Stack;

public:
    bool IsEmpty() {
        return Stack.IsEmpty();
    }

    void EnqueueAll(TAutoPtr<TVectorType<T, std::allocator<T>>> vec) {
        Stack.Enqueue(vec.Release());
    }

    void DequeueAllSingleConsumer(TVectorType<T, std::allocator<T>>* r) {
        TTempTlsVector<TVectorType<T, std::allocator<T>>*> vs;
        Stack.DequeueAllSingleConsumer(vs.GetVector());

        for (typename TVector<TVectorType<T, std::allocator<T>>*>::reverse_iterator i = vs.GetVector()->rbegin();
             i != vs.GetVector()->rend(); ++i) {
            if (i == vs.GetVector()->rend()) {
                r->swap(**i);
            } else {
                r->insert(r->end(), (*i)->begin(), (*i)->end());
            }
            delete *i;
        }
    }
};
