#pragma once

/*
  http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue

  Simple semi-wait-free queue. Many producers - one consumer.
  Tracking of allocated memory is not required.
  No CAS. Only atomic swap (exchange) operations.

  WARNING: a sleeping producer can stop progress for consumer.

  WARNING: there is no wait&notify mechanic for consumer,
  consumer receives nullptr if queue was empty.

  WARNING: the algorithm itself is lock-free
  but producers and consumer could be blocked by memory allocator

  Reference design: rtmapreduce/libs/threading/lfqueue.h
 */

#include <util/generic/noncopyable.h>
#include <util/system/types.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include "tune.h"

namespace NThreading {
    namespace NHTSwapPrivate {
        template <typename T, typename TTuneup>
        struct TNode
           : public TTuneup::TNodeBase,
              public TTuneup::template TNodeLayout<TNode<T, TTuneup>, T> {
            TNode(const T& item) {
                this->Next = nullptr;
                this->Item = item;
            }

            TNode(T&& item) {
                this->Next = nullptr;
                this->Item = std::move(item);
            }
        };

        struct TDefaultTuneup {
            struct TNodeBase: private TNonCopyable {
            };

            template <typename TNode, typename T>
            struct TNodeLayout {
                TNode* Next;
                T Item;
            };

            template <typename TNode>
            struct TQueueLayout {
                TNode* Head;
                TNode* Tail;
            };
        };

        template <typename T, typename TTuneup>
        class THTSwapQueueImpl
           : protected  TTuneup::template TQueueLayout<TNode<T, TTuneup>> {
        protected:
            using TTunedNode = TNode<T, TTuneup>;

        public:
            using TItem = T;

            THTSwapQueueImpl() {
                this->Head = new TTunedNode(T());
                this->Tail = this->Head;
            }

            ~THTSwapQueueImpl() {
                TTunedNode* node = this->Head;
                while (node != nullptr) {
                    TTunedNode* next = node->Next;
                    delete node;
                    node = next;
                }
            }

            template <typename TT>
            void Push(TT&& item) {
                Enqueue(new TTunedNode(std::forward<TT>(item)));
            }

            T Peek() {
                TTunedNode* next = AtomicGet(this->Head->Next);
                if (next == nullptr) {
                    return T();
                }
                return next->Item;
            }

            void Enqueue(TTunedNode* node) {
                // our goal is to avoid expensive CAS here,
                // but now consumer will be blocked until new tail linked.
                // fortunately 'window of inconsistency' is extremely small.
                TTunedNode* prev = AtomicSwap(&this->Tail, node);
                AtomicSet(prev->Next, node);
            }

            T Pop() {
                TTunedNode* next = AtomicGet(this->Head->Next);
                if (next == nullptr) {
                    return nullptr;
                }
                auto item = std::move(next->Item);
                std::swap(this->Head, next); // no need atomic here
                delete next;
                return item;
            }

            bool IsEmpty() const {
                TTunedNode* next = AtomicGet(this->Head->Next);
                return (next == nullptr);
            }
        };
    }

    DeclareTuneTypeParam(THTSwapNodeBase, TNodeBase);
    DeclareTuneTypeParam(THTSwapNodeLayout, TNodeLayout);
    DeclareTuneTypeParam(THTSwapQueueLayout, TQueueLayout);

    template <typename T = void*, typename... TParams>
    class THTSwapQueue
       : public NHTSwapPrivate::THTSwapQueueImpl<T,
                                                  TTune<NHTSwapPrivate::TDefaultTuneup, TParams...>> {
    };
}
