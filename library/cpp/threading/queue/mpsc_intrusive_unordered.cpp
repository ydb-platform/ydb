#include "mpsc_intrusive_unordered.h"
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NThreading {
    void TMPSCIntrusiveUnordered::Push(TIntrusiveNode* node) noexcept {
        auto head = AtomicGet(HeadForCaS);
        for (ui32 i = NUMBER_OF_TRIES_FOR_CAS; i-- > 0;) {
            // no ABA here, because Next is exactly head
            // it does not matter how many travels head was made/
            node->Next = head;
            auto prev = AtomicGetAndCas(&HeadForCaS, node, head);
            if (head == prev) {
                return;
            }
            head = prev;
        }
        // boring of trying to do cas, let's just swap

        // no need for atomic here, because the next is atomic swap
        node->Next = 0;

        head = AtomicSwap(&HeadForSwap, node);
        if (head != nullptr) {
            AtomicSet(node->Next, head);
        } else {
            // consumer must know if no other thread may access the memory,
            // setting Next to node is a way to notify consumer
            AtomicSet(node->Next, node);
        }
    }

    TIntrusiveNode* TMPSCIntrusiveUnordered::PopMany() noexcept {
        if (NotReadyChain == nullptr) {
            auto head = AtomicSwap(&HeadForSwap, nullptr);
            NotReadyChain = head;
        }

        if (NotReadyChain != nullptr) {
            auto next = AtomicGet(NotReadyChain->Next);
            if (next != nullptr) {
                auto ready = NotReadyChain;
                TIntrusiveNode* cut;
                do {
                    cut = NotReadyChain;
                    NotReadyChain = next;
                    next = AtomicGet(NotReadyChain->Next);
                    if (next == NotReadyChain) {
                        cut = NotReadyChain;
                        NotReadyChain = nullptr;
                        break;
                    }
                } while (next != nullptr);
                cut->Next = nullptr;
                return ready;
            }
        }

        if (AtomicGet(HeadForCaS) != nullptr) {
            return AtomicSwap(&HeadForCaS, nullptr);
        }
        return nullptr;
    }

    TIntrusiveNode* TMPSCIntrusiveUnordered::Pop() noexcept {
        if (PopOneQueue != nullptr) {
            auto head = PopOneQueue;
            PopOneQueue = PopOneQueue->Next;
            return head;
        }

        PopOneQueue = PopMany();
        if (PopOneQueue != nullptr) {
            auto head = PopOneQueue;
            PopOneQueue = PopOneQueue->Next;
            return head;
        }
        return nullptr;
    }
}
