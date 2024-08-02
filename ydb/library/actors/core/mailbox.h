#pragma once

#include "defs.h"
#include "event.h"
#include "executor_pool.h"
#include "mailbox_queue_simple.h"
#include "mailbox_queue_revolving.h"
#include <functional>
#include <ydb/library/actors/util/unordered_cache.h>
#include <library/cpp/threading/queue/mpsc_htswap.h>
#include <library/cpp/threading/queue/mpsc_read_as_filled.h>
#include <util/generic/hash.h>
#include <util/system/hp_timer.h>
#include <util/generic/ptr.h>
// TODO: clean all broken arcadia atomic stuff and replace with intrinsics

namespace NActors {
    class IActor;
    class IExecutorPool;

    const ui64 ARRAY_CAPACITY = 8;

    // structure of hint:
    // 1 bit: is service or direct hint
    // 2 bits: pool index
    // 17 bits: line
    // 12 bits: index of mailbox inside of line

    struct TMailboxHeader;

    struct TMailboxStats {
        ui64 ElapsedCycles = 0;
    };

    template<bool>
    struct TMailboxUsageImpl {
        void Push(ui64 /*localId*/) {}
        void ProcessEvents(TMailboxHeader* /*mailbox*/) {}
    };

    template<>
    struct TMailboxUsageImpl<true> {
        struct TPendingEvent {
            ui64 LocalId;
            ui64 Timestamp;
        };
        NThreading::TReadAsFilledQueue<TPendingEvent> PendingEventQueue;

        ~TMailboxUsageImpl();
        void Push(ui64 localId);
        void ProcessEvents(TMailboxHeader *mailbox);
    };

    struct TMailboxHeader
        : TMailboxUsageImpl<ActorLibCollectUsageStats>
    {
        struct TMailboxActorPack {
            enum EType {
                Simple = 0,
                Array = 1,
                Map = 2,
                Complex = 3,
            };
        };

        using TActorMap = THashMap<ui64, IActor*>;

        struct TExecutionState {
            enum EState {
                // normal states
                Inactive = 0,
                Scheduled = 1,
                Leaving = 2,
                Executing = 3,
                LeavingMarked = 4,
                // states for free mailboxes (they can still be scheduled so we need duplicates)
                Free = 5,
                FreeScheduled = 6,
                FreeLeaving = 7,
                FreeExecuting = 8,
                FreeLeavingMarked = 9,
            };
        };

        volatile ui32 ExecutionState;
        ui32 Reserved : 4; // never changes, always zero
        ui32 Type : 4; // never changes
        TMailboxActorPack::EType ActorPack : 2;
        ui32 Knobs : 22;

        struct TActorPair {
            IActor *Actor;
            ui64 ActorId;
        };

        struct alignas(64) TActorArray {
            TActorPair Actors[ARRAY_CAPACITY];
        };

        struct alignas(64) TComplexActorInfo;

        union TActorsInfo {
            TActorPair Simple;
            struct {
                TActorArray* ActorsArray;
                ui64 ActorsCount;
            } Array;
            struct {
                TActorMap* ActorsMap;
            } Map;
            TComplexActorInfo* Complex;
        } ActorsInfo;

        struct alignas(64) TComplexActorInfo{
            TActorsInfo ActorsInfo;
            TMailboxActorPack::EType ActorPack;
            TMailboxStats Stats;
        };

        TMailboxHeader(TMailboxType::EType type);
        ~TMailboxHeader();

        static bool CleanupActors(TMailboxActorPack::EType &actorPack, TActorsInfo &ActorsInfo);
        bool CleanupActors();

        // this interface is used exclusively by executor thread, so implementation is there

        bool MarkForSchedule(); // we put something in queue, check should we schedule?

        bool LockForExecution(); // we got activation, try to lock mailbox
        bool LockFromFree();     // try to claim mailbox from recycled (could fail if other thread process garbage)

        void UnlockFromExecution1();                     // prepare for releasing lock
        bool UnlockFromExecution2(bool wouldReschedule); // proceed with releasing lock
        bool UnlockAsFree(bool wouldReschedule);         // preceed with releasing lock, but mark as free one

        bool IsEmpty() const noexcept {
            return (ActorPack == TMailboxActorPack::Simple && ActorsInfo.Simple.ActorId == 0) ||
               (ActorPack == TMailboxActorPack::Complex && ActorsInfo.Complex->ActorPack == TMailboxActorPack::Simple && ActorsInfo.Complex->ActorsInfo.Simple.ActorId == 0); 
        }

        template<typename T>
        static void ForEach(TMailboxActorPack::EType actorPack, TActorsInfo &ActorsInfo, T&& callback) noexcept {
            switch (actorPack) {
                case TMailboxActorPack::Simple:
                    if (ActorsInfo.Simple.ActorId) {
                        callback(ActorsInfo.Simple.ActorId, ActorsInfo.Simple.Actor);
                    }
                    break;

                case TMailboxActorPack::Map:
                    for (const auto& [actorId, actor] : *ActorsInfo.Map.ActorsMap) {
                        callback(actorId, actor);
                    }
                    break;

                case TMailboxActorPack::Array:
                    for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                        auto& row = ActorsInfo.Array.ActorsArray->Actors[i];
                        callback(row.ActorId, row.Actor);
                    }
                    break;

                case TMailboxActorPack::Complex:
                    Y_ABORT("Unexpected ActorPack type");
            }
        }

        template<typename T>
        void ForEach(T&& callback) noexcept {
            if (ActorPack != TMailboxActorPack::Complex) {
                ForEach(static_cast<TMailboxActorPack::EType>(ActorPack), ActorsInfo, std::move(callback));
            } else {
                ForEach(ActorsInfo.Complex->ActorPack, ActorsInfo.Complex->ActorsInfo, std::move(callback));
            }
        }

        static IActor* FindActor(TMailboxActorPack::EType ActorPack, TActorsInfo &ActorsInfo, ui64 localActorId) noexcept {
            switch (ActorPack) {
                case TMailboxActorPack::Simple: {
                    if (ActorsInfo.Simple.ActorId == localActorId)
                        return ActorsInfo.Simple.Actor;
                    break;
                }
                case TMailboxActorPack::Map: {
                    TActorMap::iterator it = ActorsInfo.Map.ActorsMap->find(localActorId);
                    if (it != ActorsInfo.Map.ActorsMap->end())
                        return it->second;
                    break;
                }
                case TMailboxActorPack::Array: {
                    for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                        if (ActorsInfo.Array.ActorsArray->Actors[i].ActorId == localActorId) {
                            return ActorsInfo.Array.ActorsArray->Actors[i].Actor;
                        }
                    }
                    break;
                }
                case TMailboxActorPack::Complex:
                    Y_ABORT("Unexpected ActorPack type");
            }
            return nullptr;
        }

        IActor* FindActor(ui64 localActorId) noexcept {
            if (ActorPack != TMailboxActorPack::Complex) {
                return FindActor(static_cast<TMailboxActorPack::EType>(ActorPack), ActorsInfo, localActorId);
            } else {
                return FindActor(ActorsInfo.Complex->ActorPack, ActorsInfo.Complex->ActorsInfo, localActorId);
            }
        }

        static void AttachActor(TMailboxActorPack::EType &actorPack, TActorsInfo &ActorsInfo, ui64 localActorId, IActor* actor) noexcept {
            switch (actorPack) {
                case TMailboxActorPack::Simple: {
                    if (ActorsInfo.Simple.ActorId == 0) {
                        ActorsInfo.Simple.ActorId = localActorId;
                        ActorsInfo.Simple.Actor = actor;
                        return;
                    } else {
                        auto ar = new TActorArray;
                        ar->Actors[0] = ActorsInfo.Simple;
                        ar->Actors[1] = TActorPair{actor, localActorId};
                        ActorsInfo.Array.ActorsCount = 2;
                        actorPack = TMailboxActorPack::Array;
                        ActorsInfo.Array.ActorsArray = ar;
                    }
                    break;
                }
                case TMailboxActorPack::Map: {
                    ActorsInfo.Map.ActorsMap->insert(TActorMap::value_type(localActorId, actor));
                    break;
                }
                case TMailboxActorPack::Array: {
                    if (ActorsInfo.Array.ActorsCount == ARRAY_CAPACITY) {
                        TActorMap* mp = new TActorMap();
                        for (ui64 i = 0; i < ARRAY_CAPACITY; ++i) {
                            mp->emplace(ActorsInfo.Array.ActorsArray->Actors[i].ActorId, ActorsInfo.Array.ActorsArray->Actors[i].Actor);
                        }
                        mp->emplace(localActorId, actor);
                        actorPack = TMailboxActorPack::Map;
                        ActorsInfo.Array.ActorsCount = 0;
                        delete ActorsInfo.Array.ActorsArray;
                        ActorsInfo.Map.ActorsMap = mp;
                    } else {
                        ActorsInfo.Array.ActorsArray->Actors[ActorsInfo.Array.ActorsCount++] = TActorPair{actor, localActorId};
                    }
                    break;
                }
                case TMailboxActorPack::Complex:
                    Y_ABORT("Unexpected ActorPack type");
            }
        }

        void AttachActor(ui64 localActorId, IActor* actor) noexcept {
            if (ActorPack != TMailboxActorPack::Complex) {
                TMailboxActorPack::EType pack = ActorPack;
                AttachActor(pack, ActorsInfo, localActorId, actor);
                ActorPack = pack;
            } else {
                AttachActor(ActorsInfo.Complex->ActorPack, ActorsInfo.Complex->ActorsInfo, localActorId, actor);
            }
        }

        static IActor* DetachActor(TMailboxActorPack::EType &actorPack, TActorsInfo &ActorsInfo, ui64 localActorId) noexcept {
            Y_DEBUG_ABORT_UNLESS(FindActor(actorPack, ActorsInfo, localActorId) != nullptr);

            IActor* actorToDestruct = nullptr;

            switch (actorPack) {
                case TMailboxActorPack::Simple: {
                    Y_ABORT_UNLESS(ActorsInfo.Simple.ActorId == localActorId);
                    actorToDestruct = ActorsInfo.Simple.Actor;

                    ActorsInfo.Simple.ActorId = 0;
                    ActorsInfo.Simple.Actor = nullptr;
                    break;
                }
                case TMailboxActorPack::Map: {
                    TActorMap::iterator it = ActorsInfo.Map.ActorsMap->find(localActorId);
                    Y_ABORT_UNLESS(it != ActorsInfo.Map.ActorsMap->end());

                    actorToDestruct = it->second;
                    ActorsInfo.Map.ActorsMap->erase(it);

                    if (ActorsInfo.Map.ActorsMap->size() == ARRAY_CAPACITY) {
                        auto ar = new TActorArray;
                        ui64 i = 0;
                        for (auto& [actorId, actor] : *ActorsInfo.Map.ActorsMap) {
                           ar->Actors[i++] = TActorPair{actor, actorId};
                        }
                        delete ActorsInfo.Map.ActorsMap;
                        actorPack = TMailboxActorPack::Array;
                        ActorsInfo.Array.ActorsArray = ar;
                        ActorsInfo.Array.ActorsCount = ARRAY_CAPACITY;
                    }
                    break;
                }
                case TMailboxActorPack::Array: {
                    bool found = false;
                    for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                        if (ActorsInfo.Array.ActorsArray->Actors[i].ActorId == localActorId) {
                            found = true;
                            actorToDestruct = ActorsInfo.Array.ActorsArray->Actors[i].Actor;
                            ActorsInfo.Array.ActorsArray->Actors[i] = ActorsInfo.Array.ActorsArray->Actors[ActorsInfo.Array.ActorsCount - 1];
                            ActorsInfo.Array.ActorsCount -= 1;
                            break;
                        }
                    }
                    Y_ABORT_UNLESS(found);

                    if (ActorsInfo.Array.ActorsCount == 1) {
                        const TActorPair Actor = ActorsInfo.Array.ActorsArray->Actors[0];
                        delete ActorsInfo.Array.ActorsArray;
                        actorPack = TMailboxActorPack::Simple;
                        ActorsInfo.Simple = Actor;
                    }
                    break;
                }
                case TMailboxActorPack::Complex:
                    Y_ABORT("Unexpected ActorPack type");
            }

            return actorToDestruct;
        }

        IActor* DetachActor(ui64 localActorId) noexcept {
            if (ActorPack != TMailboxActorPack::Complex) {
                TMailboxActorPack::EType pack = ActorPack;
                IActor* result = DetachActor(pack, ActorsInfo, localActorId);
                ActorPack = pack;
                return result;
            } else {
                return DetachActor(ActorsInfo.Complex->ActorPack, ActorsInfo.Complex->ActorsInfo, localActorId);
            }
        }

        void EnableStats() {
            TComplexActorInfo* complex = new TComplexActorInfo;
            complex->ActorPack = ActorPack;
            complex->ActorsInfo = std::move(ActorsInfo);
            ActorPack = TMailboxActorPack::Complex;
            ActorsInfo.Complex = complex;
        }

        void AddElapsedCycles(ui64 elapsed) {
            if (ActorPack == TMailboxActorPack::Complex) {
                ActorsInfo.Complex->Stats.ElapsedCycles += elapsed;
            }
        }

        std::optional<ui64> GetElapsedCycles() {
            if (ActorPack == TMailboxActorPack::Complex) {
                return ActorsInfo.Complex->Stats.ElapsedCycles;
            }
            return std::nullopt;
        }

        std::pair<ui32, ui32> CountMailboxEvents(ui64 localActorId, ui32 maxTraverse);
    };

    class TMailboxTable : TNonCopyable {
    private:
        struct TMailboxLineHeader {
            const TMailboxType::EType MailboxType;
            const ui32 Index;
            // some more stuff in first cache line, then goes mailboxes
            ui8 Padding[52];

            TMailboxLineHeader(TMailboxType::EType type, ui32 index)
                : MailboxType(type)
                , Index(index)
            {
            }
        };
        static_assert(sizeof(TMailboxLineHeader) <= 64, "expect sizeof(TMailboxLineHeader) <= 64");

        constexpr static ui64 MaxLines = 131000; // somewhat less then 2^17.
        constexpr static ui64 LineSize = 262144; // 64 * 2^12.

        TAtomic LastAllocatedLine;
        TAtomic AllocatedMailboxCount;

        typedef TUnorderedCache<ui32, 512, 4> TMailboxCache;
        TMailboxCache MailboxCacheSimple;
        TAtomic CachedSimpleMailboxes;
        TMailboxCache MailboxCacheRevolving;
        TAtomic CachedRevolvingMailboxes;
        TMailboxCache MailboxCacheHTSwap;
        TAtomic CachedHTSwapMailboxes;
        TMailboxCache MailboxCacheReadAsFilled;
        TAtomic CachedReadAsFilledMailboxes;
        TMailboxCache MailboxCacheTinyReadAsFilled;
        TAtomic CachedTinyReadAsFilledMailboxes;

        // and here goes large chunk of lines
        // presented as array of static size to avoid sync on access
        TMailboxLineHeader* volatile Lines[MaxLines];

        ui32 AllocateNewLine(TMailboxType::EType type);
        ui32 TryAllocateMailbox(TMailboxType::EType type, ui64 revolvingCounter);

    public:
        TMailboxTable();
        ~TMailboxTable();

        bool Cleanup(); // returns true if nothing found to destruct (so nothing new is possible to be created)

        static const ui32 LineIndexShift = 12;
        static const ui32 LineIndexMask = 0x1FFFFu << LineIndexShift;
        static const ui32 LineHintMask = 0xFFFu;
        static const ui32 PoolIndexShift = TActorId::PoolIndexShift;
        static const ui32 PoolIndexMask = TActorId::PoolIndexMask;

        static ui32 LineIndex(ui32 hint) {
            return ((hint & LineIndexMask) >> LineIndexShift);
        }
        static ui32 PoolIndex(ui32 hint) {
            return TActorId::PoolIndex(hint);
        }

        TMailboxHeader* Get(ui32 hint);
        ui32 AllocateMailbox(TMailboxType::EType type, ui64 revolvingCounter);
        void ReclaimMailbox(TMailboxType::EType type, ui32 hint, ui64 revolvingCounter);
        ui64 GetAllocatedMailboxCount() const {
            return RelaxedLoad(&AllocatedMailboxCount);
        }

    private:
        typedef void (IExecutorPool::*TEPScheduleActivationFunction)(ui32 activation);

        template <TEPScheduleActivationFunction EPSpecificScheduleActivation>
        bool GenericSendTo(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool);

    public:
        bool SendTo(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool);
        bool SpecificSendTo(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool);

        struct TSimpleMailbox: public TMailboxHeader {
            // 4 bytes - state
            // 4 bytes - knobs
            // 8 bytes - actorid
            // 8 bytes - actor*
            TSimpleMailboxQueue<IEventHandle*, 64> Queue; // 24 + 8 bytes (body, lock)
            NHPTimer::STime ScheduleMoment;

            TSimpleMailbox();
            ~TSimpleMailbox();

            IEventHandle* Pop() {
                return Queue.Pop();
            }
            IEventHandle* Head() {
                return Queue.Head();
            }

            static TSimpleMailbox* Get(ui32 hint, void* line) {
                return (TSimpleMailbox*)((ui8*)line + 64 + (hint - 1) * AlignedSize()); //
            }
            static const TMailboxType::EType MailboxType = TMailboxType::Simple;
            constexpr static ui32 AlignedSize() {
                return ((sizeof(TSimpleMailbox) + 63) / 64) * 64;
            }

            std::pair<ui32, ui32> CountSimpleMailboxEvents(ui64 localActorId, ui32 maxTraverse);
            bool CleanupEvents();
        };

        struct TRevolvingMailbox: public TMailboxHeader {
            // 4 bytes - state
            // 4 bytes - knobs
            // 8 bytes - actorid
            // 8 bytes - actor*
            TRevolvingMailboxQueue<IEventHandle*, 3, 128>::TReader QueueReader; // 8 * 3 + 4 * 3 + (padding): 40 bytes
            // here goes next cache-line, so less writers<-> reader interference
            TRevolvingMailboxQueue<IEventHandle*, 3, 128>::TWriter QueueWriter; // 8 * 3 + 4 * 3 + 8 : 48 bytes
            ui32 Reserved1;
            ui32 Reserved2;
            NHPTimer::STime ScheduleMoment;

            TRevolvingMailbox();
            ~TRevolvingMailbox();

            IEventHandle* Pop() {
                return QueueReader.Pop();
            }
            IEventHandle* Head() {
                return QueueReader.Head();
            }

            static TRevolvingMailbox* Get(ui32 hint, void* line) {
                return (TRevolvingMailbox*)((ui8*)line + 64 + (hint - 1) * AlignedSize());
            }

            constexpr static ui64 MaxMailboxesInLine() {
                return (LineSize - 64) / AlignedSize();
            }
            static const TMailboxType::EType MailboxType = TMailboxType::Revolving;
            constexpr static ui32 AlignedSize() {
                return ((sizeof(TRevolvingMailbox) + 63) / 64) * 64;
            }

            std::pair<ui32, ui32> CountRevolvingMailboxEvents(ui64 localActorId, ui32 maxTraverse);
            bool CleanupEvents();
        };

        struct THTSwapMailbox: public TMailboxHeader {
            using TQueueType = NThreading::THTSwapQueue<IEventHandle*>;

            TQueueType Queue;
            NHPTimer::STime ScheduleMoment;
            char Padding_[16];

            THTSwapMailbox()
                : TMailboxHeader(TMailboxType::HTSwap)
                , ScheduleMoment(0)
            {
            }

            ~THTSwapMailbox() {
                CleanupEvents();
            }

            IEventHandle* Pop() {
                return Queue.Pop();
            }

            IEventHandle* Head() {
                return Queue.Peek();
            }

            static THTSwapMailbox* Get(ui32 hint, void* line) {
                return (THTSwapMailbox*)((ui8*)line + 64 + (hint - 1) * AlignedSize());
            }

            constexpr static ui64 MaxMailboxesInLine() {
                return (LineSize - 64) / AlignedSize();
            }

            static const TMailboxType::EType MailboxType = TMailboxType::HTSwap;

            constexpr static ui32 AlignedSize() {
                return ((sizeof(THTSwapMailbox) + 63) / 64) * 64;
            }

            bool CleanupEvents() {
                const bool done = (Queue.Peek() == nullptr);
                while (IEventHandle* ev = Queue.Pop())
                    delete ev;
                return done;
            }
        };

        struct TReadAsFilledMailbox: public TMailboxHeader {
            using TQueueType = NThreading::TReadAsFilledQueue<IEventHandle>;

            TQueueType Queue;
            NHPTimer::STime ScheduleMoment;
            char Padding_[8];

            TReadAsFilledMailbox()
                : TMailboxHeader(TMailboxType::ReadAsFilled)
                , ScheduleMoment(0)
            {
            }

            ~TReadAsFilledMailbox() {
                CleanupEvents();
            }

            IEventHandle* Pop() {
                return Queue.Pop();
            }

            IEventHandle* Head() {
                return Queue.Peek();
            }

            static TReadAsFilledMailbox* Get(ui32 hint, void* line) {
                return (TReadAsFilledMailbox*)((ui8*)line + 64 + (hint - 1) * AlignedSize());
            }

            constexpr static ui64 MaxMailboxesInLine() {
                return (LineSize - 64) / AlignedSize();
            }

            static const TMailboxType::EType MailboxType =
                TMailboxType::ReadAsFilled;

            constexpr static ui32 AlignedSize() {
                return ((sizeof(TReadAsFilledMailbox) + 63) / 64) * 64;
            }

            bool CleanupEvents() {
                const bool done = (Queue.Peek() == nullptr);
                while (IEventHandle* ev = Queue.Pop())
                    delete ev;
                return done;
            }
        };

        struct TTinyReadAsFilledMailbox: public TMailboxHeader {
            using TQueueType = NThreading::TReadAsFilledQueue<
                IEventHandle,
                NThreading::TRaFQueueBunchSize<4>>;

            TQueueType Queue;
            NHPTimer::STime ScheduleMoment;
            char Padding_[8];

            TTinyReadAsFilledMailbox()
                : TMailboxHeader(TMailboxType::TinyReadAsFilled)
                , ScheduleMoment(0)
            {
            }

            ~TTinyReadAsFilledMailbox() {
                CleanupEvents();
            }

            IEventHandle* Pop() {
                return Queue.Pop();
            }

            IEventHandle* Head() {
                return Queue.Peek();
            }

            static TTinyReadAsFilledMailbox* Get(ui32 hint, void* line) {
                return (TTinyReadAsFilledMailbox*)((ui8*)line + 64 + (hint - 1) * AlignedSize());
            }

            constexpr static ui64 MaxMailboxesInLine() {
                return (LineSize - 64) / AlignedSize();
            }

            static const TMailboxType::EType MailboxType =
                TMailboxType::TinyReadAsFilled;

            constexpr static ui32 AlignedSize() {
                return ((sizeof(TTinyReadAsFilledMailbox) + 63) / 64) * 64;
            }

            bool CleanupEvents() {
                const bool done = (Queue.Peek() == nullptr);
                while (IEventHandle* ev = Queue.Pop())
                    delete ev;
                return done;
            }
        };
    };
}
