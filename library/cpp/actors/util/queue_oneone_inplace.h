#pragma once

#include "defs.h"
#include "queue_chunk.h"

template <typename T, ui32 TSize, typename TChunk = TQueueChunk<T, TSize>>
class TOneOneQueueInplace : TNonCopyable {
    static_assert(std::is_integral<T>::value || std::is_pointer<T>::value, "expect std::is_integral<T>::value || std::is_pointer<T>::valuer");

    TChunk* ReadFrom;
    ui32 ReadPosition;
    ui32 WritePosition;
    TChunk* WriteTo;

    friend class TReadIterator;

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
            if (ReadPosition != TChunk::EntriesCount) {
                return AtomicLoad(&head->Entries[ReadPosition++]);
            } else if (TChunk* next = AtomicLoad(&head->Next)) {
                ReadFrom = next;
                ReadPosition = 0;
                return Next();
            }
            return T{};
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
        Y_DEBUG_ABORT_UNLESS(Head() == 0);
        delete ReadFrom;
    }

    struct TPtrCleanDestructor {
        static inline void Destroy(TOneOneQueueInplace<T, TSize>* x) noexcept {
            while (T head = x->Pop())
                delete head;
            delete x;
        }
    };

    struct TCleanDestructor {
        static inline void Destroy(TOneOneQueueInplace<T, TSize>* x) noexcept {
            while (x->Pop() != nullptr)
                continue;
            delete x;
        }
    };

    struct TPtrCleanInplaceMallocDestructor {
        template <typename TPtrVal>
        static inline void Destroy(TOneOneQueueInplace<TPtrVal*, TSize>* x) noexcept {
            while (TPtrVal* head = x->Pop()) {
                head->~TPtrVal();
                free(head);
            }
            delete x;
        }
    };

    void Push(T x) noexcept {
        if (WritePosition != TChunk::EntriesCount) {
            AtomicStore(&WriteTo->Entries[WritePosition], x);
            ++WritePosition;
        } else {
            TChunk* next = new TChunk();
            next->Entries[0] = x;
            AtomicStore(&WriteTo->Next, next);
            WriteTo = next;
            WritePosition = 1;
        }
    }

    T Head() {
        TChunk* head = ReadFrom;
        if (ReadPosition != TChunk::EntriesCount) {
            return AtomicLoad(&head->Entries[ReadPosition]);
        } else if (TChunk* next = AtomicLoad(&head->Next)) {
            ReadFrom = next;
            delete head;
            ReadPosition = 0;
            return Head();
        }
        return T{};
    }

    T Pop() {
        T ret = Head();
        if (ret)
            ++ReadPosition;
        return ret;
    }

    TReadIterator Iterator() {
        return TReadIterator(ReadFrom, ReadPosition);
    }
};
