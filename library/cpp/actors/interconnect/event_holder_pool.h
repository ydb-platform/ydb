#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include "packet.h"

namespace NActors {
    struct TEvFreeItems : TEventLocal<TEvFreeItems, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        static constexpr size_t MaxEvents = 256;

        std::list<TEventHolder> FreeQueue;
        TStackVec<THolder<IEventBase>, MaxEvents> Events;
        TStackVec<THolder<TEventSerializedData>, MaxEvents> Buffers;
        std::shared_ptr<std::atomic<TAtomicBase>> Counter;
        ui64 NumBytes = 0;

        ~TEvFreeItems() {
            if (Counter) {
                TAtomicBase res = Counter->fetch_sub(NumBytes) - NumBytes;
                Y_VERIFY(res >= 0);
            }
        }

        bool GetInLineForDestruction(const TIntrusivePtr<TInterconnectProxyCommon>& common) {
            Y_VERIFY(!Counter);
            const auto& counter = common->DestructorQueueSize;
            const auto& max = common->MaxDestructorQueueSize;
            if (counter && (TAtomicBase)(counter->fetch_add(NumBytes) + NumBytes) > max) {
                counter->fetch_sub(NumBytes);
                return false;
            }
            Counter = counter;
            return true;
        }
    };

    class TEventHolderPool {
        using TDestroyCallback = std::function<void(THolder<IEventBase>)>;

        static constexpr size_t MaxFreeQueueItems = 32;
        static constexpr size_t FreeQueueTrimThreshold = MaxFreeQueueItems * 2;
        static constexpr ui64 MaxBytesPerMessage = 10 * 1024 * 1024;

        TIntrusivePtr<TInterconnectProxyCommon> Common;
        std::list<TEventHolder> Cache;
        THolder<TEvFreeItems> PendingFreeEvent;
        TDestroyCallback DestroyCallback;

    public:
        TEventHolderPool(TIntrusivePtr<TInterconnectProxyCommon> common,
                TDestroyCallback destroyCallback)
            : Common(std::move(common))
            , DestroyCallback(std::move(destroyCallback))
        {}

        TEventHolder& Allocate(std::list<TEventHolder>& queue) {
            if (Cache.empty()) {
                queue.emplace_back();
            } else {
                queue.splice(queue.end(), Cache, Cache.begin());
            }
            return queue.back();
        }

        void Release(std::list<TEventHolder>& queue) {
            for (auto it = queue.begin(); it != queue.end(); ) {
                Release(queue, it++);
            }
        }

        void Release(std::list<TEventHolder>& queue, std::list<TEventHolder>::iterator event) {
            bool trim = false;

            // release held event, if any
            if (THolder<IEventBase> ev = std::move(event->Event)) {
                auto p = GetPendingEvent();
                p->NumBytes += event->EventSerializedSize;
                auto& events = p->Events;
                events.push_back(std::move(ev));
                trim = trim || events.size() >= TEvFreeItems::MaxEvents || p->NumBytes >= MaxBytesPerMessage;
            }

            // release buffer, if any
            if (event->Buffer && event->Buffer.RefCount() == 1) {
                auto p = GetPendingEvent();
                p->NumBytes += event->EventSerializedSize;
                auto& buffers = p->Buffers;
                buffers.emplace_back(event->Buffer.Release());
                trim = trim || buffers.size() >= TEvFreeItems::MaxEvents || p->NumBytes >= MaxBytesPerMessage;
            }

            // free event and trim the cache if its size is exceeded
            event->Clear();
            Cache.splice(Cache.end(), queue, event);
            if (Cache.size() >= FreeQueueTrimThreshold) {
                auto& freeQueue = GetPendingEvent()->FreeQueue;
                auto it = Cache.begin();
                std::advance(it, Cache.size() - MaxFreeQueueItems);
                freeQueue.splice(freeQueue.end(), Cache, Cache.begin(), it);
                trim = true;
            }

            // release items if we have hit the limit
            if (trim) {
                Trim();
            }
        }

        void Trim() {
            if (auto ev = std::move(PendingFreeEvent); ev && ev->GetInLineForDestruction(Common)) {
                DestroyCallback(std::move(ev));
            }

            // ensure it is dropped
            PendingFreeEvent.Reset();
        }

    private:
        TEvFreeItems* GetPendingEvent() {
            if (!PendingFreeEvent) {
                PendingFreeEvent.Reset(new TEvFreeItems);
            }
            return PendingFreeEvent.Get();
        }
    };

}
