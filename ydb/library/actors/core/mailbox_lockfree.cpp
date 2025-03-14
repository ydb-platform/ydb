#include "mailbox_lockfree.h"
#include "actor.h"
#include "events.h"
#include "executor_pool.h"

const int DebugLevel = 0;
#define MY_DEBUG(x, y) if (x <= DebugLevel) { \
    Cerr << (TStringBuilder() << __PRETTY_FUNCTION__ << " " << y << Endl); \
}

namespace NActors {

    namespace {
        static inline IEventHandle* GetNextPtr(IEventHandle* ev) {
            return reinterpret_cast<IEventHandle*>(ev->NextLinkPtr.load(std::memory_order_relaxed));
        }

        static inline void SetNextPtr(IEventHandle* ev, IEventHandle* next) {
            ev->NextLinkPtr.store(reinterpret_cast<uintptr_t>(next), std::memory_order_relaxed);
        }

        static inline void SetNextPtr(IEventHandle* ev, uintptr_t next) {
            ev->NextLinkPtr.store(next, std::memory_order_relaxed);
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////
// TMailbox
////////////////////////////////////////////////////////////////////////////////////////////////////

    void TMailbox::Work() noexcept {
        Y_ASSERT(ActorSystemStarted); // not set for testactorsystem!
        ActorSystemStarted->wait(false);
        while (true) {
            auto vec = SimpleQueue->PopAll();
            for (auto evExt : vec) {
                MY_DEBUG(2, " Pop, mailbox# " << (void*)this << " event " << (evExt ? evExt->GetTypeName() : "nullptr") << " evPtr# " << (void*)evExt);
                if (!evExt) {
                    MY_DEBUG(1, " Pop returned nullptr, mailbox# " << (void*)this << " status# " << (int)Status.load());
                    if (Status.load() == 0) {
                        continue;
                    } else {
                        if (IsEmpty()) {
                            if (Status.exchange(MarkerFree) != MarkerFree) {
                                ExecutorPool->GetMailboxCache()->Free(this);
                            }
                        }
                        return;
                    }
                }
                {
                    std::unique_lock g(*ActorsMutex);

                    IActor* actor = nullptr;
                    TActorId recipient = evExt->GetRecipientRewrite();
                    actor = this->FindActor(recipient.LocalId());
                    if (!actor) {
                        actor = this->FindAlias(recipient.LocalId());
                        if (actor) {
                            // Work as if some alias actor rewrites events and delivers them to the real actor id
                            evExt->Rewrite(evExt->GetTypeRewrite(), actor->SelfId());
                            recipient = evExt->GetRecipientRewrite();
                        }
                    }
                    NHPTimer::STime eventStart = 0;// = NHPTimer::HPNow();
                    TActorContext ctx(*this, *ExecutorPool, eventStart, recipient);
                    TlsActivationContext = &ctx; // ensure dtor (if any) is called within actor system
                    // move for destruct before ctx;
                    auto ev = std::move(evExt);

                    if (actor) {
                        Y_ASSERT(ev);
                        TAutoPtr<IEventHandle> evHandle(ev);
                        actor->OnEnqueueEvent(0);
                        actor->Receive(evHandle);

                        actor->OnDequeueEvent();
                    } else {
                        TAutoPtr<IEventHandle> nonDelivered = IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown);
                        if (nonDelivered.Get()) {
                            ctx.Send(nonDelivered);
                        }
                    }
                }
            }
            TActorContext ctx(*this, *ExecutorPool, 0, {});
            TlsActivationContext = &ctx;
            DyingActors.clear();
            TlsActivationContext = nullptr;
            if (IsEmpty()) {
                // there are no more alive actors on that mailbox
                if (Status.exchange(MarkerFree) != MarkerFree) {
                    ExecutorPool->GetMailboxCache()->Free(this);
                }
            }
        }
    }

    void TMailbox::UnregisterActor(ui64 localActorId) noexcept {
        std::unique_lock g(*ActorsMutex);

        IActor* actor = DetachActor(localActorId);
        MY_DEBUG(1, " local_actor# " <<  localActorId << " mailbox " << (void*)this << " actor# " << TypeName(*actor));

        DyingActors.push_back(THolder(actor));
    }

    IActor* TMailbox::FindActor(ui64 localActorId) noexcept {
        std::unique_lock g(*ActorsMutex);
        switch (ActorPack) {
            case EActorPack::Empty:
                ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                ///    << " mailbox " << (void*)this << " empty " << Endl);
                return nullptr;

            case EActorPack::Simple:
                if (ActorsInfo.Simple.ActorId == localActorId) {
                    ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                    ///    << " mailbox " << (void*)this << " simple found " << Endl);
                    return ActorsInfo.Simple.Actor;
                }
                ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                ///    << " mailbox " << (void*)this << " simple not found " << Endl);
                return nullptr;

            case EActorPack::Array:
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    auto& entry = ActorsInfo.Array.ActorsArray->Actors[i];
                    if (entry.ActorId == localActorId) {
                        ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                        ///    << " mailbox " << (void*)this << " array found " << Endl);
                        return entry.Actor;
                    }
                }
                ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                ///    << " mailbox " << (void*)this << " array not found " << Endl);
                return nullptr;

            case EActorPack::Map: {
                auto it = ActorsInfo.Map.ActorsMap->find(localActorId);
                if (it != ActorsInfo.Map.ActorsMap->end()) {
                    ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                    ///    << " mailbox " << (void*)this << " map found " << Endl);
                    return it->second;
                }
                ///Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
                ///    << " mailbox " << (void*)this << " map not found " << Endl);
                return nullptr;
            }
        }

        Y_ABORT();
    }

    IActor* TMailbox::FindAlias(ui64 localActorId) noexcept {
        std::unique_lock g(*ActorsMutex);
        switch (ActorPack) {
            case EActorPack::Empty:
            case EActorPack::Simple:
            case EActorPack::Array:
                return nullptr;

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->Aliases.find(localActorId);
                if (it != m->Aliases.end()) {
                    return it->second;
                }
                return nullptr;
            }
        }

        Y_ABORT();
    }

    void TMailbox::AttachActor(ui64 localActorId, IActor* actor) noexcept {
        std::unique_lock g(*ActorsMutex);
        MY_DEBUG(1, "Status# " << Status.load() << " local_actor# " <<  localActorId
            << " mailbox " << (void*)this << " actor# " << TypeName(*actor));
        switch (ActorPack) {
            case EActorPack::Empty:
                ActorsInfo.Simple = { actor, localActorId };
                ActorPack = EActorPack::Simple;
                return;

            case EActorPack::Simple: {
                TActorArray* a = new TActorArray;
                a->Actors[0] = ActorsInfo.Simple;
                a->Actors[1] = TActorPair{ actor, localActorId };
                ActorsInfo.Array = { a, 2 };
                ActorPack = EActorPack::Array;
                return;
            }

            case EActorPack::Array: {
                if (ActorsInfo.Array.ActorsCount < ArrayCapacity) {
                    ActorsInfo.Array.ActorsArray->Actors[ActorsInfo.Array.ActorsCount++] = TActorPair{ actor, localActorId };
                    return;
                }

                TActorMap* m = new TActorMap();
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ArrayCapacity; ++i) {
                    m->emplace(a->Actors[i].ActorId, a->Actors[i].Actor);
                }
                m->emplace(localActorId, actor);

                ActorsInfo.Map = { m };
                ActorPack = EActorPack::Map;
                delete a;
                return;
            }

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                m->emplace(localActorId, actor);
                return;
            }
        }

        Y_ABORT();
    }

    void TMailbox::AttachAlias(ui64 localActorId, IActor* actor) noexcept {
        std::unique_lock g(*ActorsMutex);
        // Note: we assume the specified actor is registered and the alias is correct
        EnsureActorMap();
        actor->Aliases.insert(localActorId);
        ActorsInfo.Map.ActorsMap->Aliases.emplace(localActorId, actor);
    }

    IActor* TMailbox::DetachActor(ui64 localActorId) noexcept {
        std::unique_lock g(*ActorsMutex);
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("DetachActor(%" PRIu64 ") called for an empty mailbox", localActorId);

            case EActorPack::Simple: {
                if (ActorsInfo.Simple.ActorId == localActorId) {
                    IActor* actor = ActorsInfo.Simple.Actor;
                    Y_ABORT_UNLESS(actor->Aliases.empty(), "Unexpected actor aliases for EActorPack::Simple");
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    return actor;
                }
                break;
            }

            case EActorPack::Array: {
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    if (a->Actors[i].ActorId == localActorId) {
                        IActor* actor = a->Actors[i].Actor;
                        Y_ABORT_UNLESS(actor->Aliases.empty(), "Unexpected actor aliases for EActorPack::Array");
                        a->Actors[i] = a->Actors[ActorsInfo.Array.ActorsCount - 1];
                        if (0 == --ActorsInfo.Array.ActorsCount) {
                            ActorsInfo.Empty = {};
                            ActorPack = EActorPack::Empty;
                            delete a;
                        }
                        return actor;
                    }
                }
                break;
            }

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->find(localActorId);
                if (it != m->end()) {
                    IActor* actor = it->second;
                    if (!actor->Aliases.empty()) {
                        for (ui64 aliasId : actor->Aliases) {
                            bool removed = m->Aliases.erase(aliasId);
                            Y_ABORT_UNLESS(removed, "Unexpected failure to remove a register actor alias");
                        }
                        actor->Aliases.clear();
                    }
                    m->erase(it);
                    if (m->empty()) {
                        Y_ABORT_UNLESS(m->Aliases.empty(), "Unexpected actor aliases left in an empty EActorPack::Map");
                        ActorsInfo.Empty = {};
                        ActorPack = EActorPack::Empty;
                        delete m;
                    }
                    return actor;
                }
                break;
            }
        }

        Y_ABORT("DetachActor(%" PRIu64 ") called for an unknown actor", localActorId);
    }

    IActor* TMailbox::DetachAlias(ui64 localActorId) noexcept {
        std::unique_lock g(*ActorsMutex);
        //Cerr << (TStringBuilder() << __PRETTY_FUNCTION__  << " local_actor# " <<  localActorId
        //    << " mailbox " << (void*)this << Endl);
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("DetachAlias(%" PRIu64 ") called for an empty mailbox", localActorId);

            case EActorPack::Simple:
            case EActorPack::Array:
                break;

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->Aliases.find(localActorId);
                if (it != m->Aliases.end()) {
                    IActor* actor = it->second;
                    actor->Aliases.erase(localActorId);
                    m->Aliases.erase(it);
                    return actor;
                }
                break;
            }
        }

        Y_ABORT("DetachAlias(%" PRIu64 ") called for an unknown actor", localActorId);
    }

    void TMailbox::EnsureActorMap() {
        std::unique_lock g(*ActorsMutex);
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("Expected a non-empty mailbox");

            case EActorPack::Simple: {
                TActorMap* m = new TActorMap();
                m->emplace(ActorsInfo.Simple.ActorId, ActorsInfo.Simple.Actor);
                ActorsInfo.Map.ActorsMap = m;
                ActorPack = EActorPack::Map;
                return;
            }

            case EActorPack::Array: {
                TActorMap* m = new TActorMap();
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    m->emplace(a->Actors[i].ActorId, a->Actors[i].Actor);
                }
                ActorsInfo.Map.ActorsMap = m;
                ActorPack = EActorPack::Map;
                delete a;
                return;
            }

            case EActorPack::Map: {
                return;
            }
        }

        Y_ABORT();
    }

    void TMailbox::EnableStats() {
        EnsureActorMap();
    }

    void TMailbox::AddElapsedCycles(ui64 cycles) {
        std::unique_lock g(*ActorsMutex);
        if (ActorPack == EActorPack::Map) {
            ActorsInfo.Map.ActorsMap->Stats.ElapsedCycles += cycles;
        }
    }

    std::optional<ui64> TMailbox::GetElapsedCycles() {
        std::unique_lock g(*ActorsMutex);
        if (ActorPack == EActorPack::Map) {
            return ActorsInfo.Map.ActorsMap->Stats.ElapsedCycles;
        }
        return std::nullopt;
    }

    std::optional<double> TMailbox::GetElapsedSeconds() {
        if (auto x = GetElapsedCycles()) {
            return {NHPTimer::GetSeconds(*x)};
        }
        return std::nullopt;
    }

    bool TMailbox::CleanupActors() noexcept {
        bool done = true;

        std::unique_lock g(*ActorsMutex);
        // Note: actor destructor might register more actors (including the same mailbox)
        for (int round = 0; round < 10; ++round) {
            switch (ActorPack) {
                case EActorPack::Empty: {
                    return done;
                }

                case EActorPack::Simple: {
                    IActor* actor = ActorsInfo.Simple.Actor;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    delete actor;
                    done = false;
                    continue;
                }

                case EActorPack::Array: {
                    TActorArray* a = ActorsInfo.Array.ActorsArray;
                    size_t count = ActorsInfo.Array.ActorsCount;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    for (size_t i = 0; i < count; ++i) {
                        delete a->Actors[i].Actor;
                    }
                    delete a;
                    done = false;
                    continue;
                }

                case EActorPack::Map: {
                    TActorMap* m = ActorsInfo.Map.ActorsMap;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    for (auto& pr : *m) {
                        delete pr.second;
                    }
                    delete m;
                    done = false;
                    continue;
                }
            }

            Y_ABORT("CleanupActors called with an unexpected state");
        }

        Y_ABORT_UNLESS(ActorPack == EActorPack::Empty, "Actor destructors keep registering more actors");
        return done;
    }

    size_t TMailbox::CleanupEvents() noexcept {
        return SimpleQueue->Cleanup(false);
    }

    bool TMailbox::Cleanup() noexcept {
        MY_DEBUG(1, " mailbox# " << (void*)this << " current status# " << (int)Status.load());
        Status.store(MarkerFree);
        bool doneActors = CleanupActors();
        size_t eventsInQueue = CleanupEvents();
        if (!(doneActors && eventsInQueue == 0)) {
            MY_DEBUG(1, " mailbox# " << (void*)this << " doneActors# " << doneActors << " eventsInQueue# " << eventsInQueue);
        }
        if (Worker) {
            if (ActorSystemStarted) {
                *ActorSystemStarted = true;
                ActorSystemStarted->notify_all();
            }

            SimpleQueue->Cleanup(true);
            MY_DEBUG(1, " mailbox# " << (void*)this << " wait for worker to finish");
            Worker->join();
            Worker.reset();
        }
        return doneActors && eventsInQueue == 0;
    }

    TMailbox::TMailbox() {
    }

    TMailbox::~TMailbox() noexcept {
        Cleanup();
    }

    void TMailbox::OnPreProcessed(IEventHandle* , IEventHandle* ) noexcept {
    }

    void TMailbox::AppendPreProcessed(IEventHandle* , IEventHandle* ) noexcept {
    }

    void TMailbox::PrependPreProcessed(IEventHandle* , IEventHandle* ) noexcept {
    }

    EMailboxPush TMailbox::Push(TAutoPtr<IEventHandle>& evPtr) noexcept {
        uintptr_t current = Status.load(std::memory_order_relaxed);
        IEventHandle* ev = evPtr.Release();
        for (;;) {
            TStringStream s;
            if (ev) {
                s << " for actor# " << ev->GetRecipientRewrite() << " ev_type# " << ev->GetTypeName();
            }
            MY_DEBUG(2, "Status# " << Status.load() << " mailbox# " << (void*)this << " push event " << (void*)ev << s.Str());
            if (current == MarkerFree) {
                evPtr.Reset(ev);
                return EMailboxPush::Free;
            } else if (current == MarkerUnlocked) {
                if (Status.compare_exchange_weak(current, 0, std::memory_order_acquire)) {
                    SimpleQueue->Push(ev);
                    return EMailboxPush::Locked;
                }
            } else {
                SimpleQueue->Push(ev);
                return EMailboxPush::Pushed; // do not schedule mailbox activation
            }
        }
        return EMailboxPush::Free;
    }

    IEventHandle* TMailbox::PreProcessEvents() noexcept {
        return nullptr;
    }

    TAutoPtr<IEventHandle> TMailbox::Pop() noexcept {
        return SimpleQueue->Pop();
    }

    std::pair<ui32, ui32> TMailbox::CountMailboxEvents(ui64 , ui32 ) noexcept {
        return {0, 0};
    }

    bool TMailbox::TryLock() noexcept {
        uintptr_t expected = MarkerUnlocked;
        return Status.compare_exchange_strong(expected, 0, std::memory_order_acquire);
    }

    bool TMailbox::TryUnlock() noexcept {
        uintptr_t current = Status.load(std::memory_order_relaxed);
        if (current != 0) {
            return false;
        }

        return Status.compare_exchange_strong(current, MarkerUnlocked, std::memory_order_release);
    }

    void TMailbox::PushFront(TAutoPtr<IEventHandle>& ) noexcept {
        Y_ABORT();
    }

    bool TMailbox::IsFree() const noexcept {
        return Status.load(std::memory_order_relaxed) == MarkerFree;
    }

    void TMailbox::LockToFree() noexcept {
        uintptr_t current = Status.exchange(MarkerFree, std::memory_order_acquire);
        MY_DEBUG(2, " mailbox# " << (void*)this << " current status# " << (int)current);
        if (current) {
            Y_DEBUG_ABORT_UNLESS(current != MarkerUnlocked, "LockToFree called on an unlocked mailbox, status# %d, mailbox# %p", current, this);
            Y_DEBUG_ABORT_UNLESS(current != MarkerFree, "LockToFree called on a mailbox that is already free, status# %d, mailbox# %p", current, this);
        }
    }

    void TMailbox::LockFromFree() noexcept {
        uintptr_t current = MarkerFree;
        MY_DEBUG(2, " mailbox# " << (void*)this);
        if (!Status.compare_exchange_strong(current, 0, std::memory_order_relaxed)) {
            Y_ABORT("LockFromFree called on a mailbox that is not free, %p", this);
        }
    }

    void TMailbox::Unlock(IExecutorPool* , NHPTimer::STime , ui64& ) {
        if (!Worker) {
            MY_DEBUG(1, " mailbox# " << (void*)this << " new worker");
            Worker = std::thread(std::bind(&TMailbox::Work, this));
        } else {
            MY_DEBUG(1, " mailbox# " << (void*)this << " reuse existing worker");
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////
// TMailboxOld
////////////////////////////////////////////////////////////////////////////////////////////////////

    IActor* TMailboxOld::FindActor(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                return nullptr;

            case EActorPack::Simple:
                if (ActorsInfo.Simple.ActorId == localActorId) {
                    return ActorsInfo.Simple.Actor;
                }
                return nullptr;

            case EActorPack::Array:
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    auto& entry = ActorsInfo.Array.ActorsArray->Actors[i];
                    if (entry.ActorId == localActorId) {
                        return entry.Actor;
                    }
                }
                return nullptr;

            case EActorPack::Map: {
                auto it = ActorsInfo.Map.ActorsMap->find(localActorId);
                if (it != ActorsInfo.Map.ActorsMap->end()) {
                    return it->second;
                }
                return nullptr;
            }
        }

        Y_ABORT();
    }

    IActor* TMailboxOld::FindAlias(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
            case EActorPack::Simple:
            case EActorPack::Array:
                return nullptr;

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->Aliases.find(localActorId);
                if (it != m->Aliases.end()) {
                    return it->second;
                }
                return nullptr;
            }
        }

        Y_ABORT();
    }

    void TMailboxOld::AttachActor(ui64 localActorId, IActor* actor) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                ActorsInfo.Simple = { actor, localActorId };
                ActorPack = EActorPack::Simple;
                return;

            case EActorPack::Simple: {
                TActorArray* a = new TActorArray;
                a->Actors[0] = ActorsInfo.Simple;
                a->Actors[1] = TActorPair{ actor, localActorId };
                ActorsInfo.Array = { a, 2 };
                ActorPack = EActorPack::Array;
                return;
            }

            case EActorPack::Array: {
                if (ActorsInfo.Array.ActorsCount < ArrayCapacity) {
                    ActorsInfo.Array.ActorsArray->Actors[ActorsInfo.Array.ActorsCount++] = TActorPair{ actor, localActorId };
                    return;
                }

                TActorMap* m = new TActorMap();
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ArrayCapacity; ++i) {
                    m->emplace(a->Actors[i].ActorId, a->Actors[i].Actor);
                }
                m->emplace(localActorId, actor);

                ActorsInfo.Map = { m };
                ActorPack = EActorPack::Map;
                delete a;
                return;
            }

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                m->emplace(localActorId, actor);
                return;
            }
        }

        Y_ABORT();
    }

    void TMailboxOld::AttachAlias(ui64 localActorId, IActor* actor) noexcept {
        // Note: we assume the specified actor is registered and the alias is correct
        EnsureActorMap();
        actor->Aliases.insert(localActorId);
        ActorsInfo.Map.ActorsMap->Aliases.emplace(localActorId, actor);
    }

    IActor* TMailboxOld::DetachActor(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("DetachActor(%" PRIu64 ") called for an empty mailbox", localActorId);

            case EActorPack::Simple: {
                if (ActorsInfo.Simple.ActorId == localActorId) {
                    IActor* actor = ActorsInfo.Simple.Actor;
                    Y_ABORT_UNLESS(actor->Aliases.empty(), "Unexpected actor aliases for EActorPack::Simple");
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    return actor;
                }
                break;
            }

            case EActorPack::Array: {
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    if (a->Actors[i].ActorId == localActorId) {
                        IActor* actor = a->Actors[i].Actor;
                        Y_ABORT_UNLESS(actor->Aliases.empty(), "Unexpected actor aliases for EActorPack::Array");
                        a->Actors[i] = a->Actors[ActorsInfo.Array.ActorsCount - 1];
                        if (0 == --ActorsInfo.Array.ActorsCount) {
                            ActorsInfo.Empty = {};
                            ActorPack = EActorPack::Empty;
                            delete a;
                        }
                        return actor;
                    }
                }
                break;
            }

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->find(localActorId);
                if (it != m->end()) {
                    IActor* actor = it->second;
                    if (!actor->Aliases.empty()) {
                        for (ui64 aliasId : actor->Aliases) {
                            bool removed = m->Aliases.erase(aliasId);
                            Y_ABORT_UNLESS(removed, "Unexpected failure to remove a register actor alias");
                        }
                        actor->Aliases.clear();
                    }
                    m->erase(it);
                    if (m->empty()) {
                        Y_ABORT_UNLESS(m->Aliases.empty(), "Unexpected actor aliases left in an empty EActorPack::Map");
                        ActorsInfo.Empty = {};
                        ActorPack = EActorPack::Empty;
                        delete m;
                    }
                    return actor;
                }
                break;
            }
        }

        Y_ABORT("DetachActor(%" PRIu64 ") called for an unknown actor", localActorId);
    }

    IActor* TMailboxOld::DetachAlias(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("DetachAlias(%" PRIu64 ") called for an empty mailbox", localActorId);

            case EActorPack::Simple:
            case EActorPack::Array:
                break;

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->Aliases.find(localActorId);
                if (it != m->Aliases.end()) {
                    IActor* actor = it->second;
                    actor->Aliases.erase(localActorId);
                    m->Aliases.erase(it);
                    return actor;
                }
                break;
            }
        }

        Y_ABORT("DetachAlias(%" PRIu64 ") called for an unknown actor", localActorId);
    }

    void TMailboxOld::EnsureActorMap() {
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("Expected a non-empty mailbox");

            case EActorPack::Simple: {
                TActorMap* m = new TActorMap();
                m->emplace(ActorsInfo.Simple.ActorId, ActorsInfo.Simple.Actor);
                ActorsInfo.Map.ActorsMap = m;
                ActorPack = EActorPack::Map;
                return;
            }

            case EActorPack::Array: {
                TActorMap* m = new TActorMap();
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    m->emplace(a->Actors[i].ActorId, a->Actors[i].Actor);
                }
                ActorsInfo.Map.ActorsMap = m;
                ActorPack = EActorPack::Map;
                delete a;
                return;
            }

            case EActorPack::Map: {
                return;
            }
        }

        Y_ABORT();
    }

    void TMailboxOld::EnableStats() {
        EnsureActorMap();
    }

    void TMailboxOld::AddElapsedCycles(ui64 cycles) {
        if (ActorPack == EActorPack::Map) {
            ActorsInfo.Map.ActorsMap->Stats.ElapsedCycles += cycles;
        }
    }

    std::optional<ui64> TMailboxOld::GetElapsedCycles() {
        if (ActorPack == EActorPack::Map) {
            return ActorsInfo.Map.ActorsMap->Stats.ElapsedCycles;
        }
        return std::nullopt;
    }

    std::optional<double> TMailboxOld::GetElapsedSeconds() {
        if (auto x = GetElapsedCycles()) {
            return {NHPTimer::GetSeconds(*x)};
        }
        return std::nullopt;
    }

    bool TMailboxOld::CleanupActors() noexcept {
        bool done = true;

        // Note: actor destructor might register more actors (including the same mailbox)
        for (int round = 0; round < 10; ++round) {
            switch (ActorPack) {
                case EActorPack::Empty: {
                    return done;
                }

                case EActorPack::Simple: {
                    IActor* actor = ActorsInfo.Simple.Actor;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    delete actor;
                    done = false;
                    continue;
                }

                case EActorPack::Array: {
                    TActorArray* a = ActorsInfo.Array.ActorsArray;
                    size_t count = ActorsInfo.Array.ActorsCount;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    for (size_t i = 0; i < count; ++i) {
                        delete a->Actors[i].Actor;
                    }
                    delete a;
                    done = false;
                    continue;
                }

                case EActorPack::Map: {
                    TActorMap* m = ActorsInfo.Map.ActorsMap;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    for (auto& pr : *m) {
                        delete pr.second;
                    }
                    delete m;
                    done = false;
                    continue;
                }
            }

            Y_ABORT("CleanupActors called with an unexpected state");
        }

        Y_ABORT_UNLESS(ActorPack == EActorPack::Empty, "Actor destructors keep registering more actors");
        return done;
    }

    bool TMailboxOld::CleanupEvents() noexcept {
        bool hadEvents = false;

        // Note: new events cannot be added after this mailbox is marked free
        uintptr_t current = NextEventPtr.exchange(MarkerFree, std::memory_order_acquire);
        if (current && current != MarkerUnlocked && current != MarkerFree) {
            IEventHandle* top = reinterpret_cast<IEventHandle*>(current);
            do {
                IEventHandle* ev = top;
                top = GetNextPtr(ev);
                hadEvents = true;
                delete ev;
            } while (top);
        }

        if (EventHead) {
            do {
                IEventHandle* ev = EventHead;
                EventHead = GetNextPtr(ev);
                hadEvents = true;
                delete ev;
            } while (EventHead);
            EventTail = nullptr;
        }

        return !hadEvents;
    }

    bool TMailboxOld::Cleanup() noexcept {
        bool doneActors = CleanupActors();
        bool doneEvents = CleanupEvents();
        return doneActors && doneEvents;
    }

    TMailboxOld::~TMailboxOld() noexcept {
        Cleanup();
    }

    void TMailboxOld::OnPreProcessed(IEventHandle* head, IEventHandle* tail) noexcept {
        Y_DEBUG_ABORT_UNLESS(head && tail);
        Y_DEBUG_ABORT_UNLESS(GetNextPtr(tail) == nullptr);
    #ifdef ACTORSLIB_COLLECT_EXEC_STATS
        // Mark events as enqueued when usage stats are enabled
        if (ActorLibCollectUsageStats) {
            for (IEventHandle* ev = head; ev; ev = GetNextPtr(ev)) {
                if (IActor* actor = FindActor(ev->GetRecipientRewrite().LocalId())) {
                    actor->OnEnqueueEvent(ev->SendTime);
                } else if (IActor* alias = FindAlias(ev->GetRecipientRewrite().LocalId())) {
                    actor->OnEnqueueEvent(ev->SendTime);
                }
            }
        }
    #endif
    }

    void TMailboxOld::AppendPreProcessed(IEventHandle* head, IEventHandle* tail) noexcept {
        OnPreProcessed(head, tail);
        if (EventTail) {
            SetNextPtr(EventTail, head);
            EventTail = tail;
        } else {
            EventHead = head;
            EventTail = tail;
        }
    }

    void TMailboxOld::PrependPreProcessed(IEventHandle* head, IEventHandle* tail) noexcept {
        OnPreProcessed(head, tail);
        if (EventHead) {
            SetNextPtr(tail, EventHead);
            EventHead = head;
        } else {
            EventHead = head;
            EventTail = tail;
        }
    }

    EMailboxPush TMailboxOld::Push(TAutoPtr<IEventHandle>& evPtr) noexcept {
        IEventHandle* ev = evPtr.Release();
        uintptr_t current = NextEventPtr.load(std::memory_order_relaxed);
        for (;;) {
            if (current == MarkerFree) {
                evPtr.Reset(ev);
                return EMailboxPush::Free;
            }
            if (current == MarkerUnlocked) {
                // Note: try to lock an unlocked mailbox
                // The acquire memory order synchronizes with release in TryUnlock on success
                if (NextEventPtr.compare_exchange_weak(current, 0, std::memory_order_acquire)) {
                    // Success: add this event to the preprocessed events tail
                    SetNextPtr(ev, uintptr_t(0));
                    AppendPreProcessed(ev, ev);
                    return EMailboxPush::Locked;
                }
            } else {
                // Note: try to push event on top of the stack
                // The release memory order synchronizes with acquire in Pop on success
                SetNextPtr(ev, current);
                if (NextEventPtr.compare_exchange_weak(current, reinterpret_cast<uintptr_t>(ev), std::memory_order_release)) {
                    return EMailboxPush::Pushed;
                }
            }
        }
    }

    IEventHandle* TMailboxOld::PreProcessEvents() noexcept {
        uintptr_t current = NextEventPtr.load(std::memory_order_acquire);
        while (current && current != MarkerFree) {
            Y_DEBUG_ABORT_UNLESS(current != MarkerUnlocked);
            IEventHandle* last = reinterpret_cast<IEventHandle*>(current);

            // Eagerly move events to preprocessed on every iteration
            // We avoid unnecessary races with the pusher over the top of the stack
            if (IEventHandle* newTail = GetNextPtr(last)) {
                SetNextPtr(last, nullptr);

                // This inverts the list, forming the new [head, tail] list
                IEventHandle* newHead = newTail;
                IEventHandle* next = nullptr;
                while (IEventHandle* prev = GetNextPtr(newHead)) {
                    SetNextPtr(newHead, next);
                    next = newHead;
                    newHead = prev;
                }
                SetNextPtr(newHead, next);

                // Append the new partial list to preprocessed events
                AppendPreProcessed(newHead, newTail);

                // Now we have at least one preprocessed event
                return last;
            }

            if (EventHead) {
                // We already have some preprocessed events
                return last;
            }

            // We need to take a single item and replace it with nullptr
            if (NextEventPtr.compare_exchange_strong(current, 0, std::memory_order_acquire)) {
                AppendPreProcessed(last, last);
                return nullptr;
            }

            // We have failed, but the next iteration will have more than one item
        }

        return nullptr;
    }

    TAutoPtr<IEventHandle> TMailboxOld::Pop() noexcept {
        if (!EventHead || ActorLibCollectUsageStats) {
            PreProcessEvents();
        }

        IEventHandle* ev = EventHead;
        if (ev) {
            EventHead = GetNextPtr(ev);
            if (!EventHead) {
                EventTail = nullptr;
            }
            SetNextPtr(ev, nullptr);
        }
        return ev;
    }

    std::pair<ui32, ui32> TMailboxOld::CountMailboxEvents(ui64 localActorId, ui32 maxTraverse) noexcept {
        IEventHandle* last = PreProcessEvents();

        ui32 local = 0;
        ui32 total = 0;
        for (IEventHandle* ev = EventHead; ev; ev = GetNextPtr(ev)) {
            ++total;
            if (ev->GetRecipientRewrite().LocalId() == localActorId) {
                ++local;
            }
            if (total >= maxTraverse) {
                return { local, total };
            }
        }

        if (last) {
            ++total;
            if (last->GetRecipientRewrite().LocalId() == localActorId) {
                ++local;
            }
        }

        return { local, total };
    }

    bool TMailboxOld::TryLock() noexcept {
        uintptr_t expected = MarkerUnlocked;
        return NextEventPtr.compare_exchange_strong(expected, 0, std::memory_order_acquire);
    }

    bool TMailboxOld::TryUnlock() noexcept {
        if (EventHead) {
            return false;
        }

        uintptr_t current = NextEventPtr.load(std::memory_order_relaxed);
        if (current != 0) {
            return false;
        }

        return NextEventPtr.compare_exchange_strong(current, MarkerUnlocked, std::memory_order_release);
    }

    void TMailboxOld::PushFront(TAutoPtr<IEventHandle>& evPtr) noexcept {
        IEventHandle* ev = evPtr.Release();

    #ifdef ACTORSLIB_COLLECT_EXEC_STATS
        // This is similar to sending the event again
        ev->SendTime = (::NHPTimer::STime)GetCycleCountFast();
    #endif

        SetNextPtr(ev, nullptr);
        PrependPreProcessed(ev, ev);
    }

    bool TMailboxOld::IsFree() const noexcept {
        return NextEventPtr.load(std::memory_order_relaxed) == MarkerFree;
    }

    void TMailboxOld::LockToFree() noexcept {
        uintptr_t current = NextEventPtr.exchange(MarkerFree, std::memory_order_acquire);
        if (current) {
            Y_DEBUG_ABORT_UNLESS(current != MarkerUnlocked, "LockToFree called on an unlocked mailbox");
            Y_DEBUG_ABORT_UNLESS(current != MarkerFree, "LockToFree called on a mailbox that is already free");
            IEventHandle* newTail = reinterpret_cast<IEventHandle*>(current);
            IEventHandle* newHead = newTail;
            IEventHandle* next = nullptr;
            while (IEventHandle* prev = GetNextPtr(newHead)) {
                SetNextPtr(newHead, next);
                next = newHead;
                newHead = prev;
            }
            SetNextPtr(newHead, next);

            if (EventTail) {
                SetNextPtr(EventTail, newHead);
                EventTail = newTail;
            } else {
                EventHead = newHead;
                EventTail = newTail;
            }
        }
    }

    void TMailboxOld::LockFromFree() noexcept {
        uintptr_t current = MarkerFree;
        if (!NextEventPtr.compare_exchange_strong(current, 0, std::memory_order_relaxed)) {
            Y_ABORT("LockFromFree called on a mailbox that is not free");
        }
    }

    void TMailboxOld::Unlock(IExecutorPool* , NHPTimer::STime now, ui64& ) {
        if (!TryUnlock()) {
            ScheduleMoment = now;
            // pool->ScheduleActivationEx(this, ++revolvingCounter);
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////
// TMailboxCache
////////////////////////////////////////////////////////////////////////////////////////////////////


    TMailboxCache::TMailboxCache(TMailboxTable* table)
        : Table(table)
    {}

    TMailboxCache::~TMailboxCache() {
        std::lock_guard<std::mutex> g(Mutex);
        if (BackupBlock) {
            Table->FreeBlock(BackupBlock, BackupSize);
            BackupBlock = nullptr;
            BackupSize = 0;
        }

        if (CurrentBlock) {
            Table->FreeBlock(CurrentBlock, CurrentSize);
            CurrentBlock = nullptr;
            CurrentSize = 0;
        }
    }

    void TMailboxCache::Switch(TMailboxTable* table) {
        std::lock_guard<std::mutex> g(Mutex);
        if (Table != table) {
            if (BackupBlock) {
                Table->FreeBlock(BackupBlock, BackupSize);
                BackupBlock = nullptr;
                BackupSize = 0;
            }
            if (CurrentBlock) {
                Table->FreeBlock(CurrentBlock, CurrentSize);
                CurrentBlock = nullptr;
                CurrentSize = 0;
            }
            Table = table;
        }
    }

    TMailbox* TMailboxCache::Allocate() {
        std::unique_lock g(Mutex);
        Y_ABORT_UNLESS(Table);

        MY_DEBUG(1, "cache# " << (void*)this << " BackupBlock# " << (void*)BackupBlock << " block# " << (void*) CurrentBlock << " size# " << CurrentSize);
        if (!CurrentBlock) {
            if (BackupBlock) [[likely]] {
                CurrentBlock = BackupBlock;
                CurrentSize = BackupSize;
                BackupBlock = nullptr;
                BackupSize = 0;
            } else {
                auto block = Table->AllocateBlock();
                CurrentBlock = block.first;
                CurrentSize = block.second;
            }
        }

        Y_ABORT_UNLESS(CurrentBlock);
        Y_ABORT_UNLESS(CurrentSize > 0);

        TMailbox* mailbox = CurrentBlock;
        CurrentBlock = mailbox->ActorsInfo.Empty.NextFree;
        CurrentSize--;

        MY_DEBUG(1, "cache# " << (void*)this << " mailbox# " << (void*)mailbox << " block# " << (void*) CurrentBlock << " size# " << CurrentSize);
        if (!(CurrentBlock ? CurrentSize > 0 : CurrentSize == 0)) {
            Sleep(TDuration::Seconds(1));
        }
        Y_DEBUG_ABORT_UNLESS(CurrentBlock ? CurrentSize > 0 : CurrentSize == 0, "%p, %zu", CurrentBlock, CurrentSize);

        mailbox->ActorsInfo.Empty.NextFree = nullptr;
        return mailbox;
    }

    void TMailboxCache::Free(TMailbox* mailbox) {
        std::unique_lock g(Mutex);
        MY_DEBUG(1, "cache# " << (void*)this << " mailbox# " << (void*)mailbox << " block# " << (void*) CurrentBlock << " size# " << CurrentSize);
        Y_ABORT_UNLESS(Table);
        Y_ABORT_UNLESS(mailbox != CurrentBlock, "double free");

        if (CurrentSize >= TMailboxTable::BlockSize) {
            if (BackupBlock) {
                Table->FreeBlock(BackupBlock, BackupSize);
            }
            BackupBlock = CurrentBlock;
            BackupSize = CurrentSize;
            CurrentBlock = nullptr;
            CurrentSize = 0;
        }

        mailbox->ActorsInfo.Empty.NextFree = CurrentBlock;
        CurrentBlock = mailbox;
        CurrentSize++;
        MY_DEBUG(1, "cache# " << (void*)this << " mailbox# " << (void*)mailbox << " block# " << (void*) CurrentBlock << " size# " << CurrentSize);
    }

    TMailboxTable::TMailboxTable(std::atomic<bool>* actorSystemStarted)
        : ActorSystemStarted(actorSystemStarted)
    {}

    TMailboxTable::~TMailboxTable() {
        ui32 lineCount = GetAllocatedLinesCountSlow();
        for (size_t i = 0; i < lineCount; ++i) {
            if (auto* line = Lines[i].load(std::memory_order_acquire)) {
                delete line;
            }
        }
    }

    bool TMailboxTable::Cleanup() noexcept {
        bool done = true;
        ui32 lineCount = GetAllocatedLinesCountSlow();
        for (ui32 lineIndex = 0; lineIndex < lineCount; ++lineIndex) {
            auto* line = Lines[lineIndex].load(std::memory_order_acquire);
            if (line) [[likely]] {
                for (ui32 i = 0; i < MailboxesPerLine; ++i) {
                    done &= line->Mailboxes[i].Cleanup();
                }
            }
            if (lineCount == lineIndex + 1) {
                // In case cleanup allocated more mailboxes
                lineCount = GetAllocatedLinesCountSlow();
            }
        }
        return done;
    }

    size_t TMailboxTable::GetAllocatedLinesCountSlow() const {
        std::unique_lock g(Lock);
        return AllocatedLines.load(std::memory_order_relaxed);
    }

    TMailbox* TMailboxTable::Get(ui32 hint) const {
        ui32 lineIndex = (hint >> LineIndexShift) & LineIndexMask;
        if (lineIndex < LinesCount) [[likely]] {
            auto* line = Lines[lineIndex].load(std::memory_order_acquire);
            if (line) [[likely]] {
                return &line->Mailboxes[hint & MailboxIndexMask];
            }
        }
        return nullptr;
    }

    TMailbox* TMailboxTable::Allocate() {
        std::unique_lock g(Lock);

        if (!FreeMailboxes) [[unlikely]] {
            TMailbox* head = AllocateFullBlockLocked();
            if (!head) {
                throw std::bad_alloc();
            }
            FreeMailboxes = head;
            FreeMailboxesCount = BlockSize;
        }

        TMailbox* mailbox = FreeMailboxes;
        FreeMailboxes = mailbox->ActorsInfo.Empty.NextFree;
        FreeMailboxesCount--;

        Y_DEBUG_ABORT_UNLESS(FreeMailboxes ? FreeMailboxesCount > 0 : FreeMailboxesCount == 0);

        mailbox->ActorsInfo.Empty.NextFree = nullptr;
        return mailbox;
    }

    std::pair<TMailbox*, size_t> TMailboxTable::AllocateBlock() {
        std::unique_lock g(Lock);

        TMailbox* head = AllocateFullBlockLocked();
        MY_DEBUG(1, "cache# " << (void*)this << " head# " << (void*) head << " BlockSize# " << BlockSize);
        if (head) [[likely]] {
            return { head, BlockSize };
        }

        if (!FreeMailboxes) [[unlikely]] {
            throw std::bad_alloc();
        }

        // Take a single free mailbox and return it as a 1-item block
        TMailbox* mailbox = FreeMailboxes;
        FreeMailboxes = mailbox->ActorsInfo.Empty.NextFree;
        FreeMailboxesCount--;

        Y_DEBUG_ABORT_UNLESS(FreeMailboxes ? FreeMailboxesCount > 0 : FreeMailboxesCount == 0);

        mailbox->ActorsInfo.Empty.NextFree = nullptr;
        return { mailbox, 1u };
    }

    void TMailboxTable::Free(TMailbox* mailbox) {
        std::unique_lock g(Lock);

        Y_DEBUG_ABORT_UNLESS(FreeMailboxesCount < BlockSize);

        mailbox->ActorsInfo.Empty.NextFree = FreeMailboxes;
        FreeMailboxes = mailbox;
        FreeMailboxesCount++;

        if (FreeMailboxesCount == BlockSize) {
            FreeFullBlock(FreeMailboxes);
            FreeMailboxes = nullptr;
            FreeMailboxesCount = 0;
        }
    }

    void TMailboxTable::FreeBlock(TMailbox* head, size_t count) {
        if (count == BlockSize) [[likely]] {
            FreeFullBlock(head);
            return;
        }

        std::unique_lock g(Lock);

        Y_DEBUG_ABORT_UNLESS(count < BlockSize);
        Y_DEBUG_ABORT_UNLESS(FreeMailboxesCount < BlockSize);

        while (head) {
            Y_DEBUG_ABORT_UNLESS(count > 0, "count# %zu", count);
            TMailbox* mailbox = head;
            head = head->ActorsInfo.Empty.NextFree;
            count--;

            mailbox->ActorsInfo.Empty.NextFree = FreeMailboxes;
            FreeMailboxes = mailbox;
            FreeMailboxesCount++;
            if (FreeMailboxesCount == BlockSize) {
                FreeFullBlock(FreeMailboxes);
                FreeMailboxes = nullptr;
                FreeMailboxesCount = 0;
            }
        }

        Y_DEBUG_ABORT_UNLESS(count == 0, "count# %zu", count);
    }

    void TMailboxTable::FreeFullBlock(TMailbox* head) noexcept {
        TMailbox* current = FreeBlocks.load(std::memory_order_relaxed);
        do {
            head->ActorsInfo.Empty.NextFreeBlock = current;
        } while (!FreeBlocks.compare_exchange_weak(current, head, std::memory_order_release));
    }

    TMailbox* TMailboxTable::AllocateFullBlockLocked() {
        TMailbox* current = FreeBlocks.load(std::memory_order_acquire);
        while (current) {
            // We are removing blocks under a mutex, so accessing NextFreeBlock
            // is safe. However other threads may free more blocks concurrently.
            TMailbox* head = current;
            TMailbox* next = current->ActorsInfo.Empty.NextFreeBlock;
            if (FreeBlocks.compare_exchange_weak(current, next, std::memory_order_acquire)) {
                head->ActorsInfo.Empty.NextFreeBlock = nullptr;
                return head;
            }
        }

        // We need to allocate a new line
        size_t lineIndex = AllocatedLines.load(std::memory_order_relaxed);
        if (lineIndex < LinesCount) [[likely]] {
            static_assert((MailboxesPerLine & (BlockSize - 1)) == 0,
                "Per line mailboxes are not divisible into blocks");

            // Note: this line may throw bad_alloc
            TMailboxLine* line = new TMailboxLine;

            TMailbox* head = &line->Mailboxes[0];
            TMailbox* tail = head;
            ui32 base = lineIndex << LineIndexShift;
            for (ui32 i = 0; i < MailboxesPerLine; ++i) {
                TMailbox* mailbox = &line->Mailboxes[i];
                mailbox->Hint = base + i;
                mailbox->ActorSystemStarted = ActorSystemStarted;
                if (i > 0) {
                    if ((i & (BlockSize - 1)) == 0) {
                        // This is the first mailbox is the next block
                        tail->ActorsInfo.Empty.NextFreeBlock = mailbox;
                        tail = mailbox;
                    } else {
                        // This is the next free mailbox is the current block
                        line->Mailboxes[i - 1].ActorsInfo.Empty.NextFree = mailbox;
                    }
                }
            }

            // Publish the new line (mailboxes become available via Get using their hint)
            Lines[lineIndex].store(line, std::memory_order_release);
            AllocatedLines.store(lineIndex + 1, std::memory_order_relaxed);

            // Take the first new block as the result
            TMailbox* result = head;
            if (result->Hint == 0) [[unlikely]] {
                // Skip the very first block because it has a hint==0 mailbox
                result = std::exchange(result->ActorsInfo.Empty.NextFreeBlock, nullptr);
            }
            head = std::exchange(result->ActorsInfo.Empty.NextFreeBlock, nullptr);

            // Other blocks are atomically prepended to the list of free blocks
            if (head) [[likely]] {
                current = FreeBlocks.load(std::memory_order_relaxed);
                do {
                    tail->ActorsInfo.Empty.NextFreeBlock = current;
                } while (!FreeBlocks.compare_exchange_weak(current, head, std::memory_order_release));
            }

            return result;
        }

        // We don't have any more lines available (more than 536M actors)
        return nullptr;
    }

} // namespace NActors
