#pragma once

#include "event_local.h"
#include "scheduler_cookie.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace NActors {

    struct TBundledEventOptions {
        TActorId Recipient;
        TActorId Sender = {};
        IEventHandle::TEventFlags Flags = 0;
        ui64 Cookie = 0; // Message correlation cookie.
        const TActorId* ForwardOnNondelivery = nullptr;
        NWilson::TTraceId TraceId = {};
    };

    namespace NEventBundle {
        struct TAllocation {
            enum ESlot : unsigned {
                Handle = 1 << 0,
                Event = 1 << 1,
                Cookie = 1 << 2,
            };

            static TAllocation* Allocate(size_t size, size_t alignment);
            static TAllocation* FromHandle(IEventHandle* handle) noexcept;
            void AddBeforePublish(ESlot slot) noexcept;
            void Release(ESlot slot) noexcept;
            IEventHandle* ConstructHandle(TBundledEventOptions&& options, THolder<IEventBase> event);

        private:
            explicit TAllocation(size_t alignment) noexcept
                : Alignment(alignment)
            {}

            // The handle bit also pins storage while the factory constructs the objects.
            std::atomic<unsigned> Live{Handle};
            const size_t Alignment;
        };

        class TCookie final : public NSchedulerCookie::TTwoWayCookie {
            TAllocation* const Allocation;

            void Destroy() noexcept override;

        public:
            explicit TCookie(TAllocation* allocation) noexcept
                : Allocation(allocation)
            {}
        };

        constexpr size_t AlignOffset(size_t offset, size_t alignment) noexcept {
            return (offset + alignment - 1) & ~(alignment - 1);
        }

        inline constexpr size_t HandleOffset = AlignOffset(sizeof(TAllocation), alignof(IEventHandle));

        struct TFactory;
    }

    // An opt-in concrete event can also be allocated and deleted on its own.
    template <class TEv, ui32 EventType>
    class TEventLocalBundled : public TEventLocal<TEv, EventType> {
        NEventBundle::TAllocation* Allocation = nullptr;

        friend struct NEventBundle::TFactory;

    public:
        static void operator delete(TEventLocalBundled* event, std::destroying_delete_t) noexcept {
            static_assert(std::is_final_v<TEv>, "Bundled events must be final");
            auto* allocation = event->Allocation;
            auto* concrete = static_cast<TEv*>(event);
            concrete->~TEv();
            if (allocation) {
                allocation->Release(NEventBundle::TAllocation::Event);
            } else if constexpr (alignof(TEv) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
                ::operator delete(concrete, std::align_val_t(alignof(TEv)));
            } else {
                ::operator delete(concrete);
            }
        }

        static void operator delete(void* event) noexcept {
            ::operator delete(event);
        }

        static void operator delete(void* event, std::align_val_t alignment) noexcept {
            ::operator delete(event, alignment);
        }
    };

    struct TBundledScheduledEvent {
        std::unique_ptr<IEventHandle> Event;
        TSchedulerCookieHolder OwnerCookie;
        TSchedulerCookieHolder SchedulerCookie;
    };

    namespace NEventBundle {
        struct TFactory {
            template <class TEv, bool WithCookie, class... TArgs>
            static auto Make(TBundledEventOptions&& options, TArgs&&... args) {
                using TBase = TEventLocalBundled<TEv, TEv::EventType>;
                static_assert(std::is_base_of_v<TBase, TEv> && std::is_final_v<TEv>,
                    "MakeBundled requires a final TEventLocalBundled event");

                constexpr size_t eventOffset = AlignOffset(HandleOffset + sizeof(IEventHandle), alignof(TEv));
                constexpr size_t eventEnd = eventOffset + sizeof(TEv);
                constexpr size_t cookieOffset = AlignOffset(eventEnd, alignof(TCookie));
                constexpr size_t size = WithCookie ? cookieOffset + sizeof(TCookie) : eventEnd;
                constexpr size_t alignment = std::max({
                    alignof(TAllocation), alignof(IEventHandle), alignof(TEv),
                    WithCookie ? alignof(TCookie) : size_t(1)});

                auto release = [](TAllocation* allocation) { allocation->Release(TAllocation::Handle); };
                std::unique_ptr<TAllocation, decltype(release)> storage(TAllocation::Allocate(size, alignment));
                auto* bytes = reinterpret_cast<std::byte*>(storage.get());
                auto* event = ::new (bytes + eventOffset) TEv(std::forward<TArgs>(args)...);
                storage->AddBeforePublish(TAllocation::Event);
                static_cast<TBase*>(event)->Allocation = storage.get();
                auto* handle = storage->ConstructHandle(std::move(options), THolder<IEventBase>(event));
                auto* allocation = storage.release();

                if constexpr (WithCookie) {
                    auto* cookie = ::new (bytes + cookieOffset) TCookie(allocation);
                    allocation->AddBeforePublish(TAllocation::Cookie);
                    return TBundledScheduledEvent{std::unique_ptr<IEventHandle>(handle), cookie, cookie};
                } else {
                    return std::unique_ptr<IEventHandle>(handle);
                }
            }
        };
    }

    template <class TEv, class... TArgs>
    std::unique_ptr<IEventHandle> MakeBundled(TBundledEventOptions options, TArgs&&... args) {
        return NEventBundle::TFactory::Make<TEv, false>(std::move(options), std::forward<TArgs>(args)...);
    }

    // Move Event and Release() SchedulerCookie into Schedule. Detach OwnerCookie to cancel
    // or release the owner's reference after delivery. Dropping the bundle detaches both.
    template <class TEv, class... TArgs>
    TBundledScheduledEvent MakeBundledWithCookie(TBundledEventOptions options, TArgs&&... args) {
        return NEventBundle::TFactory::Make<TEv, true>(std::move(options), std::forward<TArgs>(args)...);
    }
}
