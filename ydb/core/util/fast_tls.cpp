#include "fast_tls.h"

#include "lf_stack.h"

#include <util/generic/singleton.h>

namespace NKikimr {

    namespace NDetail {
        namespace {
            struct TFastSlot : public TLockFreeIntrusiveStackItem<TFastSlot> {
                size_t Key = -1;
            };

            struct TFastSlotState {
                TFastSlot Slots[TPerThreadStorage::FAST_COUNT];
                TLockFreeIntrusiveStack<TFastSlot> FreeList;
                std::atomic<size_t> LastKey{ 0 };

                static TFastSlotState& Instance() noexcept {
                    return *HugeSingleton<TFastSlotState>();
                }
            };
        }

        TPerThreadStorage::TPerThreadStorage() noexcept {
            // nothing
        }

        uintptr_t TPerThreadStorage::Set(size_t key, void* ptr) {
            Y_ABORT_UNLESS(ptr != nullptr, "Value cannot be null");

            if (key < FAST_COUNT) {
                void* old = Values[key].exchange(ptr, std::memory_order_acq_rel);
                Y_ABORT_UNLESS(old == nullptr, "Unexpected non-null previous value in a fast slot");
                return uintptr_t(key << 1) | uintptr_t(1);
            }

            const size_t garbage = SlowGarbage.load(std::memory_order_acquire);
            if (garbage > (Slow.size() >> 1)) {
                // More that half of slow hash map is garbage
                // We are going to allocate, may as well collect it
                auto it = Slow.begin();
                size_t collected = 0;
                while (it != Slow.end() && collected < garbage) {
                    if (it->second.load() == nullptr) {
                        Slow.erase(it++);
                        ++collected;
                    } else {
                        ++it;
                    }
                }

                // Slow keys are never reused and garbage counter is only
                // incremented after slot is marked as empty. We may collect
                // slots in different order, but we never collect more than
                // in the garbage counter, so there should always be at least
                // as much real garbage as in the counter. There must be a
                // bug somewhere if it's ever not so.
                SlowGarbage.fetch_sub(collected, std::memory_order_relaxed);
            }

            auto res = Slow.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(key),
                std::forward_as_tuple(ptr));
            Y_ABORT_UNLESS(res.second, "Unexpected existing value in a slow slot");

            TSlowSlot* slot = &res.first->second;
            uintptr_t token = uintptr_t(slot);
            Y_ABORT_UNLESS((token & 1) == 0, "Unexpected unaligned pointer");
            return token;
        }

        void TPerThreadStorage::Clear(uintptr_t token, void* ptr) {
            Y_ABORT_UNLESS(token != 0, "Cannot clear empty token");

            if (token & 1) {
                size_t key = token >> 1;
                void* old = Values[key].exchange(nullptr, std::memory_order_release);
                Y_ABORT_UNLESS(old == ptr, "Unexpected pointer mismatch when clearing a fast slot");
                return;
            }

            auto* slot = reinterpret_cast<TSlowSlot*>(token);

            // We may be running in another thread, so it's not safe to work
            // with the slow hash table. However, since *slot != nullptr it
            // cannot be collected until this atomic store succeeds.
            void* old = slot->exchange(nullptr);
            Y_ABORT_UNLESS(old == ptr, "Unexpected pointer mismatch when clearing a slow slot");
            SlowGarbage.fetch_add(1, std::memory_order_release);
        }

        size_t TPerThreadStorage::AcquireKey() noexcept {
            auto& state = TFastSlotState::Instance();
            if (auto* slot = state.FreeList.Pop()) {
                return slot->Key;
            }
            size_t key = state.LastKey++;
            if (key < FAST_COUNT) {
                // Initialize a new fast slot
                auto* slot = &state.Slots[key];
                Y_ABORT_UNLESS(slot->Key == size_t(-1));
                slot->Key = key;
            }
            return key;
        }

        void TPerThreadStorage::ReleaseKey(size_t key) noexcept {
            if (key < FAST_COUNT) {
                auto& state = TFastSlotState::Instance();
                state.FreeList.Push(&state.Slots[key]);
            }
        }
    }

}
