#include "mailbox.h"
#include "actorsystem.h"
#include "actor.h"

#include <ydb/library/actors/util/datetime.h>

#include <util/system/sanitizers.h>

namespace NActors {
    TMailboxTable::TMailboxTable()
        : LastAllocatedLine(0)
        , AllocatedMailboxCount(0)
        , CachedSimpleMailboxes(0)
        , CachedRevolvingMailboxes(0)
        , CachedHTSwapMailboxes(0)
        , CachedReadAsFilledMailboxes(0)
        , CachedTinyReadAsFilledMailboxes(0)
    {
        memset((void*)Lines, 0, sizeof(Lines));
    }

    bool IsGoodForCleanup(const TMailboxHeader* header) {
        switch (AtomicLoad(&header->ExecutionState)) {
            case TMailboxHeader::TExecutionState::Inactive:
            case TMailboxHeader::TExecutionState::Scheduled:
                return true;
            case TMailboxHeader::TExecutionState::Leaving:
            case TMailboxHeader::TExecutionState::Executing:
            case TMailboxHeader::TExecutionState::LeavingMarked:
                return false;
            case TMailboxHeader::TExecutionState::Free:
            case TMailboxHeader::TExecutionState::FreeScheduled:
                return true;
            case TMailboxHeader::TExecutionState::FreeLeaving:
            case TMailboxHeader::TExecutionState::FreeExecuting:
            case TMailboxHeader::TExecutionState::FreeLeavingMarked:
                return false;
            default:
                Y_ABORT();
        }
    }

    template <typename TMailbox>
    void DestructMailboxLine(ui8* begin, ui8* end) {
        const ui32 sx = TMailbox::AlignedSize();
        for (ui8* x = begin; x + sx <= end; x += sx) {
            TMailbox* mailbox = reinterpret_cast<TMailbox*>(x);
            Y_ABORT_UNLESS(IsGoodForCleanup(mailbox));
            mailbox->ExecutionState = Max<ui32>();
            mailbox->~TMailbox();
        }
    }

    template <typename TMailbox>
    bool CleanupMailboxLine(ui8* begin, ui8* end) {
        const ui32 sx = TMailbox::AlignedSize();
        bool done = true;
        for (ui8* x = begin; x + sx <= end; x += sx) {
            TMailbox* mailbox = reinterpret_cast<TMailbox*>(x);
            Y_ABORT_UNLESS(IsGoodForCleanup(mailbox));
            done &= mailbox->CleanupActors() && mailbox->CleanupEvents();
        }
        return done;
    }

    TMailboxTable::~TMailboxTable() {
        // on cleanup we must traverse everything and free stuff
        for (ui32 i = 0; i < LastAllocatedLine; ++i) {
            if (TMailboxLineHeader* lineHeader = Lines[i]) {
                switch (lineHeader->MailboxType) {
                    case TMailboxType::Simple:
                        DestructMailboxLine<TSimpleMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::Revolving:
                        DestructMailboxLine<TRevolvingMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::HTSwap:
                        DestructMailboxLine<THTSwapMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::ReadAsFilled:
                        DestructMailboxLine<TReadAsFilledMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::TinyReadAsFilled:
                        DestructMailboxLine<TTinyReadAsFilledMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    default:
                        Y_ABORT();
                }

                lineHeader->~TMailboxLineHeader();
                free(lineHeader);
                Lines[i] = nullptr;
            }
        }

        while (MailboxCacheSimple.Pop(0))
            ;
        while (MailboxCacheRevolving.Pop(0))
            ;
        while (MailboxCacheHTSwap.Pop(0))
            ;
        while (MailboxCacheReadAsFilled.Pop(0))
            ;
        while (MailboxCacheTinyReadAsFilled.Pop(0))
            ;
    }

    bool TMailboxTable::Cleanup() {
        bool done = true;
        for (ui32 i = 0; i < LastAllocatedLine; ++i) {
            if (TMailboxLineHeader* lineHeader = Lines[i]) {
                switch (lineHeader->MailboxType) {
                    case TMailboxType::Simple:
                        done &= CleanupMailboxLine<TSimpleMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::Revolving:
                        done &= CleanupMailboxLine<TRevolvingMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::HTSwap:
                        done &= CleanupMailboxLine<THTSwapMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::ReadAsFilled:
                        done &= CleanupMailboxLine<TReadAsFilledMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    case TMailboxType::TinyReadAsFilled:
                        done &= CleanupMailboxLine<TTinyReadAsFilledMailbox>((ui8*)lineHeader + 64, (ui8*)lineHeader + LineSize);
                        break;
                    default:
                        Y_ABORT();
                }
            }
        }
        return done;
    }

    TMailboxHeader* TMailboxTable::Get(ui32 hint) {
        // get line
        const ui32 lineIndex = (hint & LineIndexMask) >> LineIndexShift;
        const ui32 lineHint = hint & LineHintMask;

        Y_ABORT_UNLESS((lineIndex < MaxLines) && (lineHint < LineSize / 64));
        if (lineHint == 0)
            return nullptr;

        if (TMailboxLineHeader* const x = AtomicLoad(Lines + lineIndex)) {
            switch (x->MailboxType) {
                case TMailboxType::Simple:
                    return TSimpleMailbox::Get(lineHint, x);
                case TMailboxType::Revolving:
                    return TRevolvingMailbox::Get(lineHint, x);
                case TMailboxType::HTSwap:
                    return THTSwapMailbox::Get(lineHint, x);
                case TMailboxType::ReadAsFilled:
                    return TReadAsFilledMailbox::Get(lineHint, x);
                case TMailboxType::TinyReadAsFilled:
                    return TTinyReadAsFilledMailbox::Get(lineHint, x);
                default:
                    Y_DEBUG_ABORT_UNLESS(false);
                    break;
            }
        }

        return nullptr;
    }

    template <TMailboxTable::TEPScheduleActivationFunction EPSpecificScheduleActivation>
    bool TMailboxTable::GenericSendTo(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool) {
        const TActorId& recipient = ev->GetRecipientRewrite();
        const ui32 hint = recipient.Hint();

        // copy-paste from Get to avoid duplicated type-switches
        const ui32 lineIndex = (hint & LineIndexMask) >> LineIndexShift;
        const ui32 lineHint = hint & LineHintMask;

        Y_ABORT_UNLESS((lineIndex < MaxLines) && (lineHint < LineSize / 64));
        if (lineHint == 0)
            return false;

        if (TMailboxLineHeader* const x = AtomicLoad(Lines + lineIndex)) {
            switch (x->MailboxType) {
                case TMailboxType::Simple: {
                    TSimpleMailbox* const mailbox = TSimpleMailbox::Get(lineHint, x);
                    mailbox->Push(recipient.LocalId());
#if (!defined(_tsan_enabled_))
                    Y_DEBUG_ABORT_UNLESS(mailbox->Type == (ui32)x->MailboxType);
#endif
                    mailbox->Queue.Push(ev.Release());
                    if (mailbox->MarkForSchedule()) {
                        RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                        (executorPool->*EPSpecificScheduleActivation)(hint);
                    }
                }
                    return true;
                case TMailboxType::Revolving: {
                    // The actorid could be stale and coming from a different machine. If local process has restarted than
                    // the stale actorid coming from a remote machine might be referencing an actor with simple mailbox
                    // which is smaller than revolving mailbox. In this cases 'lineHint' index might be greater than actual
                    // array size. Normally its ok to store stale event to other actor's valid mailbox beacuse Receive will
                    // compare receiver actor id and discard stale event. But in this case we should discard the event right away
                    // instead of trying to enque it to a mailbox at invalid address.
                    // NOTE: lineHint is 1-based
                    static_assert(TSimpleMailbox::AlignedSize() <= TRevolvingMailbox::AlignedSize(),
                                  "We expect that one line can store more simple mailboxes than revolving mailboxes");
                    if (lineHint > TRevolvingMailbox::MaxMailboxesInLine())
                        return false;

                    TRevolvingMailbox* const mailbox = TRevolvingMailbox::Get(lineHint, x);
                    mailbox->Push(recipient.LocalId());
#if (!defined(_tsan_enabled_))
                    Y_DEBUG_ABORT_UNLESS(mailbox->Type == (ui32)x->MailboxType);
#endif
                    mailbox->QueueWriter.Push(ev.Release());
                    if (mailbox->MarkForSchedule()) {
                        RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                        (executorPool->*EPSpecificScheduleActivation)(hint);
                    }
                }
                    return true;
                case TMailboxType::HTSwap: {
                    THTSwapMailbox* const mailbox = THTSwapMailbox::Get(lineHint, x);
                    mailbox->Push(recipient.LocalId());
#if (!defined(_tsan_enabled_))
                    Y_DEBUG_ABORT_UNLESS(mailbox->Type == (ui32)x->MailboxType);
#endif
                    mailbox->Queue.Push(ev.Release());
                    if (mailbox->MarkForSchedule()) {
                        RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                        (executorPool->*EPSpecificScheduleActivation)(hint);
                    }
                }
                    return true;
                case TMailboxType::ReadAsFilled: {
                    if (lineHint > TReadAsFilledMailbox::MaxMailboxesInLine())
                        return false;

                    TReadAsFilledMailbox* const mailbox = TReadAsFilledMailbox::Get(lineHint, x);
                    mailbox->Push(recipient.LocalId());
#if (!defined(_tsan_enabled_))
                    Y_DEBUG_ABORT_UNLESS(mailbox->Type == (ui32)x->MailboxType);
#endif
                    mailbox->Queue.Push(ev.Release());
                    if (mailbox->MarkForSchedule()) {
                        RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                        (executorPool->*EPSpecificScheduleActivation)(hint);
                    }
                }
                    return true;
                case TMailboxType::TinyReadAsFilled: {
                    if (lineHint > TTinyReadAsFilledMailbox::MaxMailboxesInLine())
                        return false;

                    TTinyReadAsFilledMailbox* const mailbox = TTinyReadAsFilledMailbox::Get(lineHint, x);
                    mailbox->Push(recipient.LocalId());
#if (!defined(_tsan_enabled_))
                    Y_DEBUG_ABORT_UNLESS(mailbox->Type == (ui32)x->MailboxType);
#endif
                    mailbox->Queue.Push(ev.Release());
                    if (mailbox->MarkForSchedule()) {
                        RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                        (executorPool->*EPSpecificScheduleActivation)(hint);
                    }
                }
                    return true;
                default:
                    Y_ABORT("unknown mailbox type");
            }
        }

        return false;
    }

    ui32 TMailboxTable::AllocateMailbox(TMailboxType::EType type, ui64 revolvingCounter) {
        ui32 x = TryAllocateMailbox(type, revolvingCounter);
        if (x == 0)
            x = AllocateNewLine(type);
        return x;
    }

    ui32 TMailboxTable::TryAllocateMailbox(TMailboxType::EType type, ui64 revolvingCounter) {
        switch (type) {
            case TMailboxType::Simple:
                do {
                    if (ui32 ret = MailboxCacheSimple.Pop(revolvingCounter)) {
                        AtomicDecrement(CachedSimpleMailboxes);
                        return ret;
                    }
                } while (AtomicGet(CachedSimpleMailboxes) > (MailboxCacheSimple.Concurrency * 512));
                return 0;
            case TMailboxType::Revolving:
                do {
                    if (ui32 ret = MailboxCacheRevolving.Pop(revolvingCounter)) {
                        AtomicDecrement(CachedRevolvingMailboxes);
                        return ret;
                    }
                } while (AtomicGet(CachedRevolvingMailboxes) > (MailboxCacheRevolving.Concurrency * 512));
                return 0;
            case TMailboxType::HTSwap:
                do {
                    if (ui32 ret = MailboxCacheHTSwap.Pop(revolvingCounter)) {
                        AtomicDecrement(CachedHTSwapMailboxes);
                        return ret;
                    }
                } while (AtomicGet(CachedHTSwapMailboxes) > (MailboxCacheHTSwap.Concurrency * 512));
                return 0;
            case TMailboxType::ReadAsFilled:
                do {
                    if (ui32 ret = MailboxCacheReadAsFilled.Pop(revolvingCounter)) {
                        AtomicDecrement(CachedReadAsFilledMailboxes);
                        return ret;
                    }
                } while (AtomicGet(CachedReadAsFilledMailboxes) > (MailboxCacheReadAsFilled.Concurrency * 512));
                return 0;
            case TMailboxType::TinyReadAsFilled:
                do {
                    if (ui32 ret = MailboxCacheTinyReadAsFilled.Pop(revolvingCounter)) {
                        AtomicDecrement(CachedTinyReadAsFilledMailboxes);
                        return ret;
                    }
                } while (AtomicGet(CachedTinyReadAsFilledMailboxes) > (MailboxCacheTinyReadAsFilled.Concurrency * 512));
                return 0;
            default:
                Y_ABORT("Unknown mailbox type");
        }
    }


    template
    bool TMailboxTable::GenericSendTo<&IExecutorPool::ScheduleActivation>(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool);
    template
    bool TMailboxTable::GenericSendTo<&IExecutorPool::SpecificScheduleActivation>(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool);

    void TMailboxTable::ReclaimMailbox(TMailboxType::EType type, ui32 hint, ui64 revolvingCounter) {
        if (hint != 0) {
            switch (type) {
                case TMailboxType::Simple:
                    MailboxCacheSimple.Push(hint, revolvingCounter);
                    AtomicIncrement(CachedSimpleMailboxes);
                    break;
                case TMailboxType::Revolving:
                    MailboxCacheRevolving.Push(hint, revolvingCounter);
                    AtomicIncrement(CachedRevolvingMailboxes);
                    break;
                case TMailboxType::HTSwap:
                    MailboxCacheHTSwap.Push(hint, revolvingCounter);
                    AtomicIncrement(CachedHTSwapMailboxes);
                    break;
                case TMailboxType::ReadAsFilled:
                    MailboxCacheReadAsFilled.Push(hint, revolvingCounter);
                    AtomicIncrement(CachedReadAsFilledMailboxes);
                    break;
                case TMailboxType::TinyReadAsFilled:
                    MailboxCacheTinyReadAsFilled.Push(hint, revolvingCounter);
                    AtomicIncrement(CachedTinyReadAsFilledMailboxes);
                    break;
                default:
                    Y_ABORT();
            }
        }
    }

    TMailboxHeader::TMailboxHeader(TMailboxType::EType type)
        : ExecutionState(TExecutionState::Free)
        , Reserved(0)
        , Type(type)
        , ActorPack(TMailboxActorPack::Simple)
        , Knobs(0)
    {
        ActorsInfo.Simple.ActorId = 0;
        ActorsInfo.Simple.Actor = nullptr;
    }

    TMailboxHeader::~TMailboxHeader() {
        CleanupActors();
    }

    bool TMailboxHeader::CleanupActors(TMailboxActorPack::EType &actorPack, TActorsInfo &ActorsInfo) {
        bool done = true;
        switch (actorPack) {
            case TMailboxActorPack::Simple: {
                if (ActorsInfo.Simple.ActorId != 0) {
                    delete ActorsInfo.Simple.Actor;
                    done = false;
                }
                break;
            }
            case TMailboxActorPack::Map: {
                for (auto& [actorId, actor] : *ActorsInfo.Map.ActorsMap) {
                    delete actor;
                }
                delete ActorsInfo.Map.ActorsMap;
                done = false;
                break;
            }
            case TMailboxActorPack::Array: {
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    delete ActorsInfo.Array.ActorsArray->Actors[i].Actor;
                }
                delete ActorsInfo.Array.ActorsArray;
                done = false;
                break;
            }
            case TMailboxActorPack::Complex:
                Y_ABORT("Unexpected ActorPack type");
        }
        actorPack = TMailboxActorPack::Simple;
        ActorsInfo.Simple.ActorId = 0;
        ActorsInfo.Simple.Actor = nullptr;
        return done;
    }

    bool TMailboxHeader::CleanupActors() {
        if (ActorPack != TMailboxActorPack::Complex) {
            TMailboxActorPack::EType pack = ActorPack;
            bool done = CleanupActors(pack, ActorsInfo);
            ActorPack = pack;
            return done;
        } else {
            bool done = CleanupActors(ActorsInfo.Complex->ActorPack, ActorsInfo.Complex->ActorsInfo);
            delete ActorsInfo.Complex;
            ActorPack = TMailboxActorPack::Simple;
            ActorsInfo.Simple.ActorId = 0;
            ActorsInfo.Simple.Actor = nullptr;
            return done;
        }
    }

    std::pair<ui32, ui32> TMailboxHeader::CountMailboxEvents(ui64 localActorId, ui32 maxTraverse) {
        switch (Type) {
            case TMailboxType::Simple:
                return static_cast<TMailboxTable::TSimpleMailbox*>(this)->CountSimpleMailboxEvents(localActorId, maxTraverse);
            case TMailboxType::Revolving:
                return static_cast<TMailboxTable::TRevolvingMailbox*>(this)->CountRevolvingMailboxEvents(localActorId, maxTraverse);
            default:
                return {0, 0};
        }
    }

    TMailboxUsageImpl<true>::~TMailboxUsageImpl() {
        while (auto *e = PendingEventQueue.Pop()) {
            delete e;
        }
    }

    void TMailboxUsageImpl<true>::Push(ui64 localId) {
        PendingEventQueue.Push(new TPendingEvent{localId, GetCycleCountFast()});
    }

    void TMailboxUsageImpl<true>::ProcessEvents(TMailboxHeader *mailbox) {
        while (std::unique_ptr<TPendingEvent> e{PendingEventQueue.Pop()}) {
            if (IActor *actor = mailbox->FindActor(e->LocalId)) {
                actor->OnEnqueueEvent(e->Timestamp);
            }
        }
    }

    TMailboxTable::TSimpleMailbox::TSimpleMailbox()
        : TMailboxHeader(TMailboxType::Simple)
        , ScheduleMoment(0)
    {
    }

    TMailboxTable::TSimpleMailbox::~TSimpleMailbox() {
        CleanupEvents();
    }

    bool TMailboxTable::TSimpleMailbox::CleanupEvents() {
        const bool done = (Queue.Head() == nullptr);
        while (IEventHandle* ev = Queue.Pop())
            delete ev;
        return done;
    }

    std::pair<ui32, ui32> TMailboxTable::TSimpleMailbox::CountSimpleMailboxEvents(ui64 localActorId, ui32 maxTraverse) {
        ui32 local = 0;
        ui32 total = 0;

        auto it = Queue.ReadIterator();
        while (IEventHandle* x = it.Next()) {
            ++total;
            if (x->GetRecipientRewrite().LocalId() == localActorId)
                ++local;
            if (total >= maxTraverse)
                break;
        }

        return std::make_pair(local, total);
    }

    TMailboxTable::TRevolvingMailbox::TRevolvingMailbox()
        : TMailboxHeader(TMailboxType::Revolving)
        , QueueWriter(QueueReader)
        , Reserved1(0)
        , Reserved2(0)
        , ScheduleMoment(0)
    {
    }

    TMailboxTable::TRevolvingMailbox::~TRevolvingMailbox() {
        CleanupEvents();
    }

    bool TMailboxTable::TRevolvingMailbox::CleanupEvents() {
        const bool done = (QueueReader.Head() == nullptr);
        while (IEventHandle* ev = QueueReader.Pop())
            delete ev;
        return done;
    }

    std::pair<ui32, ui32> TMailboxTable::TRevolvingMailbox::CountRevolvingMailboxEvents(ui64 localActorId, ui32 maxTraverse) {
        ui32 local = 0;
        ui32 total = 0;

        auto it = QueueReader.Iterator();

        while (IEventHandle* x = it.Next()) {
            ++total;
            if (x->GetRecipientRewrite().LocalId() == localActorId)
                ++local;
            if (total >= maxTraverse)
                break;
        }

        return std::make_pair(local, total);
    }

    template <typename T>
    static ui32 InitNewLine(ui8* x, ui8* end) {
        const ui32 sx = T::AlignedSize();

        for (ui32 index = 1; x + sx <= end; x += sx, ++index)
            ::new (x) T();

        return sx;
    }

    ui32 TMailboxTable::AllocateNewLine(TMailboxType::EType type) {
        ui8* ptr = (ui8*)malloc(LineSize);
        ui8* end = ptr + LineSize;

        const ui32 lineIndex = (ui32)AtomicIncrement(LastAllocatedLine) - 1;
        const ui32 lineIndexMask = (lineIndex << LineIndexShift) & LineIndexMask;

        // first 64 bytes is TMailboxLineHeader
        TMailboxLineHeader* header = ::new (ptr) TMailboxLineHeader(type, lineIndex);

        ui8* x = ptr + 64;
        ui32 sx = 0;
        TMailboxCache* cache = nullptr;
        TAtomic* counter = nullptr;

        switch (type) {
            case TMailboxType::Simple:
                sx = InitNewLine<TSimpleMailbox>(x, end);
                cache = &MailboxCacheSimple;
                counter = &CachedSimpleMailboxes;
                break;
            case TMailboxType::Revolving:
                sx = InitNewLine<TRevolvingMailbox>(x, end);
                cache = &MailboxCacheRevolving;
                counter = &CachedRevolvingMailboxes;
                break;
            case TMailboxType::HTSwap:
                sx = InitNewLine<THTSwapMailbox>(x, end);
                cache = &MailboxCacheHTSwap;
                counter = &CachedHTSwapMailboxes;
                break;
            case TMailboxType::ReadAsFilled:
                sx = InitNewLine<TReadAsFilledMailbox>(x, end);
                cache = &MailboxCacheReadAsFilled;
                counter = &CachedReadAsFilledMailboxes;
                break;
            case TMailboxType::TinyReadAsFilled:
                sx = InitNewLine<TTinyReadAsFilledMailbox>(x, end);
                cache = &MailboxCacheTinyReadAsFilled;
                counter = &CachedTinyReadAsFilledMailboxes;
                break;
            default:
                Y_ABORT();
        }

        AtomicStore(Lines + lineIndex, header);

        ui32 ret = lineIndexMask | 1;

        ui32 index = 2;
        for (ui32 endIndex = LineSize / sx; index != endIndex;) {
            const ui32 bufSize = 8;
            ui32 buf[bufSize];
            ui32 bufIndex;
            for (bufIndex = 0; index != endIndex && bufIndex != bufSize; ++bufIndex, ++index)
                buf[bufIndex] = lineIndexMask | index;
            cache->PushBulk(buf, bufIndex, index);
            AtomicAdd(*counter, bufIndex);
        }

        AtomicAdd(AllocatedMailboxCount, index - 1);

        return ret;
    }

    bool TMailboxTable::SendTo(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool) {
        return GenericSendTo<&IExecutorPool::ScheduleActivation>(ev, executorPool);
    }

    bool TMailboxTable::SpecificSendTo(TAutoPtr<IEventHandle>& ev, IExecutorPool* executorPool) {
        return GenericSendTo<&IExecutorPool::SpecificScheduleActivation>(ev, executorPool);
    }
}
