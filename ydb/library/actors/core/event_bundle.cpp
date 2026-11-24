#include "event_bundle.h"

namespace NActors {
    namespace NEventBundle {
        TAllocation* TAllocation::Allocate(size_t size, size_t alignment) {
            void* memory = alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__
                ? ::operator new(size, std::align_val_t(alignment))
                : ::operator new(size);
            return ::new (memory) TAllocation(alignment);
        }

        TAllocation* TAllocation::FromHandle(IEventHandle* handle) noexcept {
            return reinterpret_cast<TAllocation*>(reinterpret_cast<std::byte*>(handle) - HandleOffset);
        }

        void TAllocation::AddBeforePublish(ESlot slot) noexcept {
            const unsigned live = Live.load(std::memory_order_relaxed);
            Y_DEBUG_ABORT_UNLESS(!(live & slot));
            Live.store(live | slot, std::memory_order_relaxed);
        }

        void TAllocation::Release(ESlot slot) noexcept {
            const unsigned live = Live.fetch_and(~unsigned(slot), std::memory_order_acq_rel);
            Y_DEBUG_ABORT_UNLESS(live & slot, "Bundled event slot %u was released twice", unsigned(slot));
            if (live == slot) {
                const size_t alignment = Alignment;
                this->~TAllocation();
                if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
                    ::operator delete(this, std::align_val_t(alignment));
                } else {
                    ::operator delete(this);
                }
            }
        }

        IEventHandle* TAllocation::ConstructHandle(TBundledEventOptions&& options, THolder<IEventBase> event) {
            auto* storage = reinterpret_cast<std::byte*>(this) + HandleOffset;
            return ::new (storage) IEventHandle(options.Recipient, options.Sender, std::move(event),
                options.Flags, options.Cookie, options.ForwardOnNondelivery, std::move(options.TraceId), true);
        }

        void TCookie::Destroy() noexcept {
            auto* allocation = Allocation;
            this->~TCookie();
            allocation->Release(TAllocation::Cookie);
        }
    }

    void IEventHandle::DestroyBundled(IEventHandle* event) noexcept {
        auto* allocation = NEventBundle::TAllocation::FromHandle(event);
        event->~IEventHandle();
        allocation->Release(NEventBundle::TAllocation::Handle);
    }
}
