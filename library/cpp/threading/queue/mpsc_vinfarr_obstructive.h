#pragma once

/*
  Semi-wait-free queue, multiple producers - one consumer. Strict order.
  The queue algorithm is using concept of virtual infinite array.

  A producer takes a number from a counter and atomicaly increments the counter.
  The number taken is a number of a slot for the producer to put a new message
  into infinite array.

  Then producer constructs a virtual infinite array by bidirectional linked list
  of blocks. Each block contains several slots.

  There is a hint pointer which optimisticly points to the last block
  of the list and never goes backward.

  Consumer exploits the property of the hint pointer always going forward
  to free old blocks eventually. Consumer periodically read the hint pointer
  and the counter and thus deduce producers which potentially holds the pointer
  to a block. Consumer can free the block if all that producers filled their
  slots and left the queue.

  No producer can stop the progress for other producers.

  Consumer can obstruct a slot of a delayed producer by putting special mark.
  Thus no producer can stop the progress for consumer.
  But a slow producer may be forced to retry unlimited number of times.
  Though it's very unlikely for a non-preempted producer to be obstructed.
  That's why the algorithm is semi-wait-free.

  WARNING: there is no wait&notify mechanic for consumer,
  consumer receives nullptr if queue was empty.

  WARNING: though the algorithm itself is lock-free
  but producers and consumer could be blocked by memory allocator

  WARNING: copy constructers of the queue are not thread-safe
 */

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spinlock.h>

#include "tune.h"

namespace NThreading {
    namespace NObstructiveQueuePrivate {
        typedef void* TMsgLink;

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
            PUSH_RESULT_BLOCKED,
        };

        template <typename TAux, ui32 BUNCH_SIZE, typename TBase = TEmpty>
        struct TMsgBunch: public TBase {
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

                auto oldValue = AtomicSwap(&LinkArray[slot - FirstSlot].Msg, msg);

                if (Y_LIKELY(oldValue == nullptr)) {
                    return PUSH_RESULT_OK;
                } else {
                    LeaveBlocked(oldValue);
                    return PUSH_RESULT_BLOCKED;
                }
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

            void LeaveBlocked(ui64 slot) {
                auto token = GetToken(slot);
                token->DecrementToken();
            }

            void LeaveBlocked(TMsgLink msg) {
                auto token = reinterpret_cast<TMsgBunch*>(msg);
                token->DecrementToken();
            }

            TSlot<TAux> BlockSlotAux(ui64 slot, TMsgBunch* token) {
                auto old =
                    AtomicSwap(&LinkArray[slot - FirstSlot].Msg, (TMsgLink)token);
                if (old == nullptr) {
                    // It's valid to increment after AtomicCas
                    // because token will release data only after SetNextToken
                    token->IncrementToken();
                    return TSlot<TAux>::NullElem();
                }
                return TSlot<TAux>::Pair(old, LinkArray[slot - FirstSlot].Retrieve());
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
                return reinterpret_cast<TMsgBunch*>(LinkArray[slot - FirstSlot].Msg);
            }

            inline void IncrementToken() {
                AtomicIncrement(Token);
            }

            // the object could be destroyed after this method
            inline void DecrementToken() {
                if (Y_UNLIKELY(AtomicDecrement(Token) == BUNCH_SIZE)) {
                    Release(this);
                    AtomicGet(NextToken)->DecrementToken();
                    // this could be invalid here
                }
            }

            // the object could be destroyed after this method
            inline void SetNextToken(TMsgBunch* next) {
                AtomicSet(NextToken, next);
                if (Y_UNLIKELY(AtomicAdd(Token, BUNCH_SIZE) == BUNCH_SIZE)) {
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

            static void Release(TMsgBunch* bunch) {
                auto backLink = AtomicGet(bunch->BackLink);
                if (backLink == nullptr) {
                    return;
                }
                AtomicSet(bunch->BackLink, nullptr);

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

        template <typename TAux, ui32 BUNCH_SIZE, typename TBunchBase = TEmpty>
        class TWriteBucket {
        public:
            static const ui64 GROSS_SIZE;

            using TBunch = TMsgBunch<TAux, BUNCH_SIZE, TBunchBase>;

            TWriteBucket(TBunch* bunch = new TBunch(0, nullptr))
                : LastBunch(bunch)
                , SlotCounter(0)
            {
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

            inline bool Push(TMsgLink msg, TAux aux) {
                ui64 pushSlot = AtomicGetAndIncrement(SlotCounter);
                TBunch* hintBunch = GetLastBunch();

                for (;;) {
                    auto hint = hintBunch->Push(msg, pushSlot, aux);
                    if (Y_LIKELY(hint == PUSH_RESULT_OK)) {
                        return true;
                    }
                    bool hhResult = HandleHint(hintBunch, hint);
                    if (Y_UNLIKELY(!hhResult)) {
                        return false;
                    }
                }
            }

        protected:
            template <typename, ui32, typename>
            friend class TReadBucket;

            TBunch* volatile LastBunch; // Hint
            volatile ui64 SlotCounter;

            inline TBunch* GetLastBunch() const {
                return AtomicGet(LastBunch);
            }

            bool HandleHint(TBunch*& hintBunch, TPushResult hint) {
                if (Y_UNLIKELY(hint == PUSH_RESULT_BLOCKED)) {
                    return false;
                }

                if (Y_UNLIKELY(hint == PUSH_RESULT_BACKWARD)) {
                    hintBunch = hintBunch->GetBackLink();
                    return true;
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
                return true;
            }
        };

        template <typename TAux, ui32 BUNCH_SIZE, typename TBunchBase>
        class TReadBucket {
        public:
            static constexpr int MAX_NUMBER_OF_TRIES_TO_READ = 20;

            using TWBucket = TWriteBucket<TAux, BUNCH_SIZE, TBunchBase>;
            using TBunch = TMsgBunch<TAux, BUNCH_SIZE, TBunchBase>;

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

            inline TMsgLink Pop() {
                return PopAux().Msg;
            }

            inline TSlot<TAux> PopAux() {
                for (;;) {
                    if (Y_UNLIKELY(ReadSlot == LastKnownPushSlot)) {
                        if (Y_LIKELY(!RereadPushSlot())) {
                            return TSlot<TAux>::NullElem();
                        }
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

                    if (ReadSlot + 1 == AtomicGet(Writer->SlotCounter)) {
                        return TSlot<TAux>::NullElem();
                    }

                    result = StubbornPopAux();

                    if (result.Msg != nullptr) {
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

            // MUST BE: ReadSlot == LastKnownPushSlot
            bool RereadPushSlot() {
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

            TSlot<TAux> StubbornPopAux() {
                for (int q = 0; q < MAX_NUMBER_OF_TRIES_TO_READ; ++q) {
                    auto result = ReadBunch->GetSlotAux(ReadSlot);
                    if (Y_LIKELY(result.Msg != nullptr)) {
                        ++ReadSlot;
                        return result;
                    }
                    SpinLockPause();
                }

                return ReadBunch->BlockSlotAux(ReadSlot++, LastKnownPushBunch);
            }
        };

        struct TDefaultParams {
            static constexpr bool DeleteItems = true;
            using TAux = NObstructiveQueuePrivate::TEmptyAux;
            using TBunchBase = NObstructiveQueuePrivate::TEmpty;
            static constexpr ui32 BUNCH_SIZE = 251;
        };

    } //namespace NObstructiveQueuePrivate

    DeclareTuneValueParam(TObstructiveQueueBunchSize, ui32, BUNCH_SIZE);
    DeclareTuneValueParam(TObstructiveQueueDeleteItems, bool, DeleteItems);
    DeclareTuneTypeParam(TObstructiveQueueBunchBase, TBunchBase);
    DeclareTuneTypeParam(TObstructiveQueueAux, TAux);

    template <typename TItem = void, typename... TParams>
    class TObstructiveConsumerAuxQueue {
    private:
        using TTuned =
            TTune<NObstructiveQueuePrivate::TDefaultParams, TParams...>;

        using TAux = typename TTuned::TAux;
        using TSlot = NObstructiveQueuePrivate::TSlot<TAux>;
        using TMsgLink = NObstructiveQueuePrivate::TMsgLink;
        using TBunchBase = typename TTuned::TBunchBase;
        static constexpr bool DeleteItems = TTuned::DeleteItems;
        static constexpr ui32 BUNCH_SIZE = TTuned::BUNCH_SIZE;

    public:
        TObstructiveConsumerAuxQueue()
            : RBuckets(&WBucket)
        {
        }

        ~TObstructiveConsumerAuxQueue() {
            if (DeleteItems) {
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
            while (!WBucket.Push(reinterpret_cast<TMsgLink>(msg), TAux())) {
            }
        }

        TItem* Pop() {
            return reinterpret_cast<TItem*>(RBuckets.Pop());
        }

        TSlot PopAux() {
            return RBuckets.PopAux();
        }

    private:
        NObstructiveQueuePrivate::TWriteBucket<TAux, BUNCH_SIZE, TBunchBase>
            WBucket;
        NObstructiveQueuePrivate::TReadBucket<TAux, BUNCH_SIZE, TBunchBase>
            RBuckets;
    };

    template <typename TItem = void, bool DeleteItems = true>
    class TObstructiveConsumerQueue
       : public TObstructiveConsumerAuxQueue<TItem,
                                              TObstructiveQueueDeleteItems<DeleteItems>> {
    };
}
