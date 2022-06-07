#pragma once

/*
  Completely wait-free queue, multiple producers - one consumer. Strict order.
  The queue algorithm is using concept of virtual infinite array.

  A producer takes a number from a counter and atomically increments the counter.
  The number taken is a number of a slot for the producer to put a new message
  into infinite array.

  Then producer constructs a virtual infinite array by bidirectional linked list
  of blocks. Each block contains several slots.

  There is a hint pointer which optimistically points to the last block
  of the list and never goes backward.

  Consumer exploits the property of the hint pointer always going forward
  to free old blocks eventually. Consumer periodically read the hint pointer
  and the counter and thus deduce producers which potentially holds the pointer
  to a block. Consumer can free the block if all that producers filled their
  slots and left the queue.

  No producer can stop the progress for other producers.

  Consumer can't stop the progress for producers.
  Consumer can skip not-yet-filled slots and read them later.
  Thus no producer can stop the progress for consumer.
  The algorithm is virtually strictly ordered because it skips slots only
  if it is really does not matter in which order the slots were produced and
  consumed.

  WARNING: there is no wait&notify mechanic for consumer,
  consumer receives nullptr if queue was empty.

  WARNING: though the algorithm itself is completely wait-free
  but producers and consumer could be blocked by memory allocator

  WARNING: copy constructors of the queue are not thread-safe
 */

#include <util/generic/deque.h>
#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spinlock.h>

#include "tune.h"

namespace NThreading {
    namespace NReadAsFilledPrivate {
        typedef void* TMsgLink;

        static constexpr ui32 DEFAULT_BUNCH_SIZE = 251;

        struct TEmpty {
        };

        struct TEmptyAux {
            TEmptyAux Retrieve() const {
                return TEmptyAux();
            }

            void Store(TEmptyAux&) {
            }

            static constexpr TEmptyAux Zero() {
                return TEmptyAux();
            }
        };

        template <typename TAux>
        struct TSlot {
            TMsgLink volatile Msg;
            TAux AuxiliaryData;

            inline void Store(TAux& aux) {
                AuxiliaryData.Store(aux);
            }

            inline TAux Retrieve() const {
                return AuxiliaryData.Retrieve();
            }

            static TSlot<TAux> NullElem() {
                return {nullptr, TAux::Zero()};
            }

            static TSlot<TAux> Pair(TMsgLink msg, TAux aux) {
                return {msg, std::move(aux)};
            }
        };

        template <>
        struct TSlot<TEmptyAux> {
            TMsgLink volatile Msg;

            inline void Store(TEmptyAux&) {
            }

            inline TEmptyAux Retrieve() const {
                return TEmptyAux();
            }

            static TSlot<TEmptyAux> NullElem() {
                return {nullptr};
            }

            static TSlot<TEmptyAux> Pair(TMsgLink msg, TEmptyAux) {
                return {msg};
            }
        };

        enum TPushResult {
            PUSH_RESULT_OK,
            PUSH_RESULT_BACKWARD,
            PUSH_RESULT_FORWARD,
        };

        template <ui32 BUNCH_SIZE = DEFAULT_BUNCH_SIZE,
                  typename TBase = TEmpty,
                  typename TAux = TEmptyAux>
        struct TMsgBunch: public TBase {
            static constexpr size_t RELEASE_SIZE = BUNCH_SIZE * 2;

            ui64 FirstSlot;

            TSlot<TAux> LinkArray[BUNCH_SIZE];

            TMsgBunch* volatile NextBunch;
            TMsgBunch* volatile BackLink;

            ui64 volatile Token;
            TMsgBunch* volatile NextToken;

            /* this push can return PUSH_RESULT_BLOCKED */
            inline TPushResult Push(TMsgLink msg, ui64 slot, TAux auxiliary) {
                if (Y_UNLIKELY(slot < FirstSlot)) {
                    return PUSH_RESULT_BACKWARD;
                }

                if (Y_UNLIKELY(slot >= FirstSlot + BUNCH_SIZE)) {
                    return PUSH_RESULT_FORWARD;
                }

                LinkArray[slot - FirstSlot].Store(auxiliary);

                AtomicSet(LinkArray[slot - FirstSlot].Msg, msg);
                return PUSH_RESULT_OK;
            }

            inline bool IsSlotHere(ui64 slot) {
                return slot < FirstSlot + BUNCH_SIZE;
            }

            inline TMsgLink GetSlot(ui64 slot) const {
                return AtomicGet(LinkArray[slot - FirstSlot].Msg);
            }

            inline TSlot<TAux> GetSlotAux(ui64 slot) const {
                auto msg = GetSlot(slot);
                auto aux = LinkArray[slot - FirstSlot].Retrieve();
                return TSlot<TAux>::Pair(msg, aux);
            }

            inline TMsgBunch* GetNextBunch() const {
                return AtomicGet(NextBunch);
            }

            inline bool SetNextBunch(TMsgBunch* ptr) {
                return AtomicCas(&NextBunch, ptr, nullptr);
            }

            inline TMsgBunch* GetBackLink() const {
                return AtomicGet(BackLink);
            }

            inline TMsgBunch* GetToken(ui64 slot) {
                return reinterpret_cast<TMsgBunch*>(
                    LinkArray[slot - FirstSlot].Msg);
            }

            inline void IncrementToken() {
                AtomicIncrement(Token);
            }

            // the object could be destroyed after this method
            inline void DecrementToken() {
                if (Y_UNLIKELY(AtomicDecrement(Token) == RELEASE_SIZE)) {
                    Release(this);
                    AtomicGet(NextToken)->DecrementToken();
                    // this could be invalid here
                }
            }

            // the object could be destroyed after this method
            inline void SetNextToken(TMsgBunch* next) {
                AtomicSet(NextToken, next);
                if (Y_UNLIKELY(AtomicAdd(Token, RELEASE_SIZE) == RELEASE_SIZE)) {
                    Release(this);
                    next->DecrementToken();
                }
                // this could be invalid here
            }

            TMsgBunch(ui64 start, TMsgBunch* backLink) {
                AtomicSet(FirstSlot, start);
                memset(&LinkArray, 0, sizeof(LinkArray));
                AtomicSet(NextBunch, nullptr);
                AtomicSet(BackLink, backLink);

                AtomicSet(Token, 1);
                AtomicSet(NextToken, nullptr);
            }

            static void Release(TMsgBunch* block) {
                auto backLink = AtomicGet(block->BackLink);
                if (backLink == nullptr) {
                    return;
                }
                AtomicSet(block->BackLink, nullptr);

                do {
                    auto bbackLink = backLink->BackLink;
                    delete backLink;
                    backLink = bbackLink;
                } while (backLink != nullptr);
            }

            void Destroy() {
                for (auto tail = BackLink; tail != nullptr;) {
                    auto next = tail->BackLink;
                    delete tail;
                    tail = next;
                }

                for (auto next = this; next != nullptr;) {
                    auto nnext = next->NextBunch;
                    delete next;
                    next = nnext;
                }
            }
        };

        template <ui32 BUNCH_SIZE = DEFAULT_BUNCH_SIZE,
                  typename TBunchBase = NReadAsFilledPrivate::TEmpty,
                  typename TAux = TEmptyAux>
        class TWriteBucket {
        public:
            using TUsingAux = TAux; // for TReadBucket binding
            using TBunch = TMsgBunch<BUNCH_SIZE, TBunchBase, TAux>;

            TWriteBucket(TBunch* bunch = new TBunch(0, nullptr)) {
                AtomicSet(LastBunch, bunch);
                AtomicSet(SlotCounter, 0);
            }

            TWriteBucket(TWriteBucket&& move)
                : LastBunch(move.LastBunch)
                , SlotCounter(move.SlotCounter)
            {
                move.LastBunch = nullptr;
            }

            ~TWriteBucket() {
                if (LastBunch != nullptr) {
                    LastBunch->Destroy();
                }
            }

            inline void Push(TMsgLink msg, TAux aux) {
                ui64 pushSlot = AtomicGetAndIncrement(SlotCounter);
                TBunch* hintBunch = GetLastBunch();

                for (;;) {
                    auto hint = hintBunch->Push(msg, pushSlot, aux);
                    if (Y_LIKELY(hint == PUSH_RESULT_OK)) {
                        return;
                    }
                    HandleHint(hintBunch, hint);
                }
            }

        protected:
            template <typename, template <typename, typename...> class>
            friend class TReadBucket;

            TBunch* volatile LastBunch; // Hint
            volatile ui64 SlotCounter;

            inline TBunch* GetLastBunch() const {
                return AtomicGet(LastBunch);
            }

            void HandleHint(TBunch*& hintBunch, TPushResult hint) {
                if (Y_UNLIKELY(hint == PUSH_RESULT_BACKWARD)) {
                    hintBunch = hintBunch->GetBackLink();
                    return;
                }

                // PUSH_RESULT_FORWARD
                auto nextBunch = hintBunch->GetNextBunch();

                if (nextBunch == nullptr) {
                    auto first = hintBunch->FirstSlot + BUNCH_SIZE;
                    nextBunch = new TBunch(first, hintBunch);
                    if (Y_UNLIKELY(!hintBunch->SetNextBunch(nextBunch))) {
                        delete nextBunch;
                        nextBunch = hintBunch->GetNextBunch();
                    }
                }

                // hintBunch could not be freed here so it cannot be reused
                // it's alright if this CAS was not succeeded,
                // it means that other thread did that recently
                AtomicCas(&LastBunch, nextBunch, hintBunch);

                hintBunch = nextBunch;
            }
        };

        template <typename TWBucket = TWriteBucket<>,
                  template <typename, typename...> class TContainer = TDeque>
        class TReadBucket {
        public:
            using TAux = typename TWBucket::TUsingAux;
            using TBunch = typename TWBucket::TBunch;

            static constexpr int MAX_NUMBER_OF_TRIES_TO_READ = 5;

            TReadBucket(TWBucket* writer)
                : Writer(writer)
                , ReadBunch(writer->GetLastBunch())
                , LastKnownPushBunch(writer->GetLastBunch())
            {
                ReadBunch->DecrementToken(); // no previous token
            }

            TReadBucket(TReadBucket toCopy, TWBucket* writer)
                : TReadBucket(std::move(toCopy))
            {
                Writer = writer;
            }

            ui64 ReadyCount() const {
                return AtomicGet(Writer->SlotCounter) - ReadSlot;
            }

            TMsgLink Pop() {
                return PopAux().Msg;
            }

            TMsgLink Peek() {
                return PeekAux().Msg;
            }

            TSlot<TAux> PopAux() {
                for (;;) {
                    if (Y_UNLIKELY(ReadNow.size() != 0)) {
                        auto result = PopSkipped();
                        if (Y_LIKELY(result.Msg != nullptr)) {
                            return result;
                        }
                    }

                    if (Y_UNLIKELY(ReadSlot == LastKnownPushSlot)) {
                        if (Y_LIKELY(!RereadPushSlot())) {
                            return TSlot<TAux>::NullElem();
                        }
                        continue;
                    }

                    if (Y_UNLIKELY(!ReadBunch->IsSlotHere(ReadSlot))) {
                        if (Y_UNLIKELY(!SwitchToNextBunch())) {
                            return TSlot<TAux>::NullElem();
                        }
                    }

                    auto result = ReadBunch->GetSlotAux(ReadSlot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        ++ReadSlot;
                        return result;
                    }

                    result = StubbornPop();
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        return result;
                    }
                }
            }

            TSlot<TAux> PeekAux() {
                for (;;) {
                    if (Y_UNLIKELY(ReadNow.size() != 0)) {
                        auto result = PeekSkipped();
                        if (Y_LIKELY(result.Msg != nullptr)) {
                            return result;
                        }
                    }

                    if (Y_UNLIKELY(ReadSlot == LastKnownPushSlot)) {
                        if (Y_LIKELY(!RereadPushSlot())) {
                            return TSlot<TAux>::NullElem();
                        }
                        continue;
                    }

                    if (Y_UNLIKELY(!ReadBunch->IsSlotHere(ReadSlot))) {
                        if (Y_UNLIKELY(!SwitchToNextBunch())) {
                            return TSlot<TAux>::NullElem();
                        }
                    }

                    auto result = ReadBunch->GetSlotAux(ReadSlot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        return result;
                    }

                    result = StubbornPeek();
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        return result;
                    }
                }
            }

        private:
            TWBucket* Writer;
            TBunch* ReadBunch;
            ui64 ReadSlot = 0;
            TBunch* LastKnownPushBunch;
            ui64 LastKnownPushSlot = 0;

            struct TSkipItem {
                TBunch* Bunch;
                ui64 Slot;
                TBunch* Token;
            };

            TContainer<TSkipItem> ReadNow;
            TContainer<TSkipItem> ReadLater;

            void AddToReadLater() {
                ReadLater.push_back({ReadBunch, ReadSlot, LastKnownPushBunch});
                LastKnownPushBunch->IncrementToken();
                ++ReadSlot;
            }

            // MUST BE: ReadSlot == LastKnownPushSlot
            bool RereadPushSlot() {
                ReadNow = std::move(ReadLater);
                ReadLater.clear();

                auto oldSlot = LastKnownPushSlot;

                auto currentPushBunch = Writer->GetLastBunch();
                auto currentPushSlot = AtomicGet(Writer->SlotCounter);

                if (currentPushBunch != LastKnownPushBunch) {
                    // LastKnownPushBunch could be invalid after this line
                    LastKnownPushBunch->SetNextToken(currentPushBunch);
                }

                LastKnownPushBunch = currentPushBunch;
                LastKnownPushSlot = currentPushSlot;

                return oldSlot != LastKnownPushSlot;
            }

            bool SwitchToNextBunch() {
                for (int q = 0; q < MAX_NUMBER_OF_TRIES_TO_READ; ++q) {
                    auto next = ReadBunch->GetNextBunch();
                    if (next != nullptr) {
                        ReadBunch = next;
                        return true;
                    }
                    SpinLockPause();
                }
                return false;
            }

            TSlot<TAux> StubbornPop() {
                for (int q = 0; q < MAX_NUMBER_OF_TRIES_TO_READ; ++q) {
                    auto result = ReadBunch->GetSlotAux(ReadSlot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        ++ReadSlot;
                        return result;
                    }
                    SpinLockPause();
                }

                AddToReadLater();
                return TSlot<TAux>::NullElem();
            }

            TSlot<TAux> StubbornPeek() {
                for (int q = 0; q < MAX_NUMBER_OF_TRIES_TO_READ; ++q) {
                    auto result = ReadBunch->GetSlotAux(ReadSlot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        return result;
                    }
                    SpinLockPause();
                }

                AddToReadLater();
                return TSlot<TAux>::NullElem();
            }

            TSlot<TAux> PopSkipped() {
                do {
                    auto elem = ReadNow.front();
                    ReadNow.pop_front();

                    auto result = elem.Bunch->GetSlotAux(elem.Slot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        elem.Token->DecrementToken();
                        return result;
                    }

                    ReadLater.emplace_back(elem);

                } while (ReadNow.size() > 0);

                return TSlot<TAux>::NullElem();
            }

            TSlot<TAux> PeekSkipped() {
                do {
                    auto elem = ReadNow.front();

                    auto result = elem.Bunch->GetSlotAux(elem.Slot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        return result;
                    }

                    ReadNow.pop_front();
                    ReadLater.emplace_back(elem);

                } while (ReadNow.size() > 0);

                return TSlot<TAux>::NullElem();
            }
        };

        struct TDefaultParams {
            static constexpr ui32 BUNCH_SIZE = DEFAULT_BUNCH_SIZE;
            using TBunchBase = TEmpty;

            template <typename TElem, typename... TRest>
            using TContainer = TDeque<TElem, TRest...>;

            static constexpr bool DeleteItems = true;
        };

    } //namespace NReadAsFilledPrivate

    DeclareTuneValueParam(TRaFQueueBunchSize, ui32, BUNCH_SIZE);
    DeclareTuneTypeParam(TRaFQueueBunchBase, TBunchBase);
    DeclareTuneContainer(TRaFQueueSkipContainer, TContainer);
    DeclareTuneValueParam(TRaFQueueDeleteItems, bool, DeleteItems);

    template <typename TItem = void, typename... TParams>
    class TReadAsFilledQueue {
    private:
        using TTuned = TTune<NReadAsFilledPrivate::TDefaultParams, TParams...>;

        static constexpr ui32 BUNCH_SIZE = TTuned::BUNCH_SIZE;

        using TBunchBase = typename TTuned::TBunchBase;

        template <typename TElem, typename... TRest>
        using TContainer =
            typename TTuned::template TContainer<TElem, TRest...>;

        using TWriteBucket =
            NReadAsFilledPrivate::TWriteBucket<BUNCH_SIZE, TBunchBase>;
        using TReadBucket =
            NReadAsFilledPrivate::TReadBucket<TWriteBucket, TContainer>;

    public:
        TReadAsFilledQueue()
            : RBucket(&WBucket)
        {
        }

        ~TReadAsFilledQueue() {
            if (TTuned::DeleteItems) {
                for (;;) {
                    auto msg = Pop();
                    if (msg == nullptr) {
                        break;
                    }
                    TDelete::Destroy(msg);
                }
            }
        }

        void Push(TItem* msg) {
            WBucket.Push((void*)msg, NReadAsFilledPrivate::TEmptyAux());
        }

        TItem* Pop() {
            return (TItem*)RBucket.Pop();
        }

        TItem* Peek() {
            return (TItem*)RBucket.Peek();
        }

    protected:
        TWriteBucket WBucket;
        TReadBucket RBucket;
    };
}
