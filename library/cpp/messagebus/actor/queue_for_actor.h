#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>
#include <util/thread/lfstack.h>
#include <util/thread/singleton.h>

// TODO: include from correct directory
#include "temp_tls_vector.h"

namespace NActor {
    namespace NPrivate {
        struct TTagForTl {};

    }

    template <typename T>
    class TQueueForActor {
    private:
        TLockFreeStack<T> Queue;

    public:
        ~TQueueForActor() {
            Y_ABORT_UNLESS(Queue.IsEmpty());
        }

        bool IsEmpty() {
            return Queue.IsEmpty();
        }

        void Enqueue(const T& value) {
            Queue.Enqueue(value);
        }

        template <typename TCollection>
        void EnqueueAll(const TCollection& all) {
            Queue.EnqueueAll(all);
        }

        void Clear() {
            TVector<T> tmp;
            Queue.DequeueAll(&tmp);
        }

        template <typename TFunc>
        void DequeueAll(const TFunc& func
                        // TODO: , std::enable_if_t<TFunctionParamCount<TFunc>::Value == 1>* = 0
        ) {
            TTempTlsVector<T> temp;

            Queue.DequeueAllSingleConsumer(temp.GetVector());

            for (typename TVector<T>::reverse_iterator i = temp.GetVector()->rbegin(); i != temp.GetVector()->rend(); ++i) {
                func(*i);
            }

            temp.Clear();

            if (temp.Capacity() * sizeof(T) > 64 * 1024) {
                temp.Shrink();
            }
        }

        template <typename TFunc>
        void DequeueAllLikelyEmpty(const TFunc& func) {
            if (Y_LIKELY(IsEmpty())) {
                return;
            }

            DequeueAll(func);
        }
    };

}
