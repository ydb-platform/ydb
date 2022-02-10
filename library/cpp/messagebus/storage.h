#pragma once

#include "message_ptr_and_header.h"
#include "moved.h"
#include "ybus.h"

#include <contrib/libs/sparsehash/src/sparsehash/dense_hash_map>

#include <util/generic/deque.h>
#include <util/generic/noncopyable.h>
#include <util/generic/utility.h>

namespace NBus {
    namespace NPrivate {
        typedef TVector<TBusMessage*> TMessagesPtrs;

        class TTimedMessages {
        public:
            TTimedMessages();
            ~TTimedMessages();

            struct TItem {
                THolder<TBusMessage> Message;

                void Swap(TItem& that) {
                    DoSwap(Message, that.Message);
                }
            };

            typedef TDeque<TMoved<TItem>> TItems;

            void PushBack(TNonDestroyingAutoPtr<TBusMessage> m);
            TNonDestroyingAutoPtr<TBusMessage> PopFront();
            bool Empty() const;
            size_t Size() const;

            void Timeout(TInstant before, TMessagesPtrs* r);
            void Clear(TMessagesPtrs* r);

        private:
            TItems Items;
        };

        class TSyncAckMessages : TNonCopyable {
        public:
            TSyncAckMessages();
            ~TSyncAckMessages();

            void Push(TBusMessagePtrAndHeader& m);
            TBusMessage* Pop(TBusKey id);

            void Timeout(TInstant before, TMessagesPtrs* r);

            void Clear(TMessagesPtrs* r);

            size_t Size() const {
                return KeyToMessage.size();
            }

            void RemoveAll(const TMessagesPtrs&);

            void Gc();

            void DumpState();

        private:
            struct TTimedItem {
                TBusKey Key;
                TBusInstant SendTime;
            };

            typedef TDeque<TTimedItem> TTimedItems;
            typedef TDeque<TTimedItem>::iterator TTimedIterator;

            TTimedItems TimedItems;

            struct TValue {
                TBusMessage* Message;
            };

            // keys are already random, no need to hash them further
            struct TIdHash {
                size_t operator()(TBusKey value) const {
                    return value;
                }
            };

            typedef google::dense_hash_map<TBusKey, TValue, TIdHash> TKeyToMessage;

            TKeyToMessage KeyToMessage;
        };

    }
}
