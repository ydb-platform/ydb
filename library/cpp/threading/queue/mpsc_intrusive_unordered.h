#pragma once

/*
  Simple almost-wait-free unordered queue for low contention operations.

  It's wait-free for producers.
  Hanging producer can hide some items from consumer.
 */

#include <util/system/types.h>

namespace NThreading {
    struct TIntrusiveNode {
        TIntrusiveNode* Next;
    };

    class TMPSCIntrusiveUnordered {
    public:
        static constexpr ui32 NUMBER_OF_TRIES_FOR_CAS = 3;

        void Push(TIntrusiveNode* node) noexcept;
        TIntrusiveNode* PopMany() noexcept;
        TIntrusiveNode* Pop() noexcept;

        void Push(void* node) noexcept {
            Push(reinterpret_cast<TIntrusiveNode*>(node));
        }

    private:
        TIntrusiveNode* HeadForCaS = nullptr;
        TIntrusiveNode* HeadForSwap = nullptr;
        TIntrusiveNode* NotReadyChain = nullptr;
        TIntrusiveNode* PopOneQueue = nullptr;
    };
}
