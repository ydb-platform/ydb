#pragma once

#include "defs.h"
#include "queue_chunk.h"

template <typename T, ui32 TSize, typename D = TNoAction, typename TChunk = TQueueChunk<T, TSize>>
class TOneOneQueueInplace : TNonCopyable {
    static_assert(std::is_integral<T>::value || std::is_pointer<T>::value, "expect std::is_integral<T>::value || std::is_pointer<T>::value");

    TChunk* ReadFrom;
    ui32 ReadPosition;
    ui32 WritePosition;
    TChunk* WriteTo;

public:
    class TReadIterator {
        TChunk* ReadFrom;
        ui32 ReadPosition;

    public:
        TReadIterator(TChunk* readFrom, ui32 readPosition)
            : ReadFrom(readFrom)
            , ReadPosition(readPosition)
        {
        }

        inline T Next() {
            TChunk* head = ReadFrom;
            if (ReadPosition == TChunk::EntriesCount) [[unlikely]] {
                head = AtomicLoad(&head->Next);
                if (!head) {
                    return T{};
                }
                ReadFrom = head;
                ReadPosition = 0;
            }
            return AtomicLoad(&head->Entries[ReadPosition++]);
        }
    };

    TOneOneQueueInplace()
        : ReadFrom(new TChunk())
        , ReadPosition(0)
        , WritePosition(0)
        , WriteTo(ReadFrom)
    {
    }

    ~TOneOneQueueInplace() {
        if constexpr (!std::is_same_v<D, TNoAction>) {
            while (T x = Pop()) {
                D::Destroy(x);
            }
            delete ReadFrom;
        } else {
            TChunk* next = ReadFrom;
            do {
                TChunk* head = next;
                next = AtomicLoad(&head->Next);
                delete head;
            } while (next);
        }
    }

    struct TPtrCleanDestructor {
        static inline void Destroy(TOneOneQueueInplace* x) noexcept {
            while (T head = x->Pop()) {
                ::CheckedDelete(head);
            }
            delete x;
        }
    };

    struct TCleanDestructor {
        static inline void Destroy(TOneOneQueueInplace* x) noexcept {
            delete x;
        }
    };

    void Push(T x) {
        if (WritePosition == TChunk::EntriesCount) [[unlikely]] {
            TChunk* next = new TChunk();
            AtomicStore(&next->Entries[0], x);
            AtomicStore(&WriteTo->Next, next);
            WriteTo = next;
            WritePosition = 1;
        } else {
            AtomicStore(&WriteTo->Entries[WritePosition++], x);
        }
    }

    T Head() {
        TChunk* head = ReadFrom;
        if (ReadPosition == TChunk::EntriesCount) [[unlikely]] {
            TChunk* next = AtomicLoad(&head->Next);
            if (!next) {
                return T{};
            }
            delete head;
            head = next;
            ReadFrom = next;
            ReadPosition = 0;
        }
        return AtomicLoad(&head->Entries[ReadPosition]);
    }

    T Pop() {
        T ret = Head();
        if (ret) {
            ++ReadPosition;
        }
        return ret;
    }

    TReadIterator Iterator() {
        return TReadIterator(ReadFrom, ReadPosition);
    }
};
