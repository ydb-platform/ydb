#include "interconnect_uring_engine_client.h"

#include "interconnect_common.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <util/datetime/cputimer.h>

#include <atomic>
#include <mutex>

namespace NActors {
    namespace {

        constexpr size_t MaxDestructionBatchObjects = 256;
        constexpr size_t MaxDestructionBatchBytes = 64 * 1024;

        struct TEvDestroyEvents : TEventLocal<TEvDestroyEvents, 0> {
            std::vector<std::unique_ptr<IEventBase>> Events;
            std::vector<TIntrusivePtr<TEventSerializedData>> Buffers;
            size_t Bytes = 0;
            std::shared_ptr<std::atomic<TAtomicBase>> Counter;

            ~TEvDestroyEvents() {
                if (Counter) {
                    Counter->fetch_sub(Bytes, std::memory_order_relaxed);
                }
            }
        };

        class TActorSystemUringEngineClient final
            : public IUringEngineClient
        {
            TActorSystem& ActorSystem;
            const TActorId DestructorActorId;
            const std::shared_ptr<std::atomic<TAtomicBase>> DestructorQueueSize;
            const size_t MaxDestructorQueueSize;
            std::mutex Mutex;
            std::unique_ptr<TEvDestroyEvents> PendingDestruction;
            std::atomic<ui64> DestroyEventsCycles{0};

        public:
            TActorSystemUringEngineClient(
                    TActorSystem& actorSystem,
                    const TInterconnectProxyCommon& common)
                : ActorSystem(actorSystem)
                , DestructorActorId(common.DestructorId)
                , DestructorQueueSize(common.DestructorQueueSize)
                , MaxDestructorQueueSize(common.MaxDestructorQueueSize)
            {
            }

            void Start(const TUringEnginePtr& engine) override {
                ActorSystem.DeferPreStop([engine] { engine->Stop(); });
            }

            void Stop() override {
                std::unique_ptr<TEvDestroyEvents> ev;
                {
                    std::lock_guard guard(Mutex);
                    ev = std::move(PendingDestruction);
                }
                Flush(std::move(ev));
            }

            ui64 GetAndResetDestroyEventsCycles() override {
                return DestroyEventsCycles.exchange(0, std::memory_order_relaxed);
            }

            void Send(std::unique_ptr<IEventHandle> ev) override {
                ActorSystem.Send(ev.release());
            }

            void DestroyEvent(
                    std::unique_ptr<IEventBase> event,
                    TIntrusivePtr<TEventSerializedData> buffer) override {
                const ui64 start = GetCycleCountFast();
                const size_t bytes =
                    (event ? event->CalculateSerializedSizeCached() : 0) +
                    (buffer ? buffer->GetSize() : 0);
                std::unique_ptr<TEvDestroyEvents> ev;
                {
                    std::lock_guard guard(Mutex);
                    if (!PendingDestruction) {
                        PendingDestruction = std::make_unique<TEvDestroyEvents>();
                    }
                    if (event) {
                        PendingDestruction->Events.push_back(std::move(event));
                    }
                    if (buffer) {
                        PendingDestruction->Buffers.push_back(std::move(buffer));
                    }
                    PendingDestruction->Bytes += bytes;
                    if (PendingDestruction->Bytes >= MaxDestructionBatchBytes ||
                            PendingDestruction->Events.size() + PendingDestruction->Buffers.size() >=
                                MaxDestructionBatchObjects) {
                        ev = std::move(PendingDestruction);
                    }
                }
                Flush(std::move(ev));
                DestroyEventsCycles.fetch_add(GetCycleCountFast() - start, std::memory_order_relaxed);
            }

        private:
            void Flush(std::unique_ptr<TEvDestroyEvents> ev) {
                if (!ev) {
                    return;
                }
                ev->Counter = DestructorQueueSize;
                if (Y_LIKELY(!DestructorQueueSize ||
                        DestructorQueueSize->fetch_add(ev->Bytes, std::memory_order_relaxed) + ev->Bytes <=
                            MaxDestructorQueueSize)) {
                    Send(std::make_unique<IEventHandle>(
                        DestructorActorId,
                        TActorId(),
                        ev.release()));
                }
            }
        };

    }

    IUringEngineClientPtr CreateUringEngineClient(
            const TInterconnectProxyCommon& common,
            TActorSystem* actorSystem) {
        Y_ABORT_UNLESS(actorSystem);
        return std::make_shared<TActorSystemUringEngineClient>(*actorSystem, common);
    }

}
