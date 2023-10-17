#pragma once

#include "defs.h"

template<typename T, ui32 TSize>
struct TSimpleQueueChunk {
    static const ui32 EntriesCount = (TSize - sizeof(TSimpleQueueChunk*)) / sizeof(T);
    static_assert(EntriesCount > 0, "expect EntriesCount > 0");

    T Entries[EntriesCount];
    TSimpleQueueChunk * volatile Next;

    TSimpleQueueChunk() {
    }
};


template<typename T, ui32 TSize, typename TChunk = TSimpleQueueChunk<T, TSize>>
class TQueueInplace : TNonCopyable {
    TChunk * ReadFrom;
    ui32 ReadPosition;
    ui32 WritePosition;
    TChunk * WriteTo;
    size_t Size;

public:
    TQueueInplace()
        : ReadFrom(new TChunk())
        , ReadPosition(0)
        , WritePosition(0)
        , WriteTo(ReadFrom)
        , Size(0)
    {}

    ~TQueueInplace() {
        Y_DEBUG_ABORT_UNLESS(Head() == nullptr && Size == 0);
        delete ReadFrom;
    }

    struct TPtrCleanDestructor {
        static inline void Destroy(TQueueInplace<T, TSize> *x) noexcept {
            while (const T *head = x->Head()) {
                delete *head;
                x->Pop();
            }
            delete x;
        }
    };

    struct TCleanDestructor {
        static inline void Destroy(TQueueInplace<T, TSize> *x) noexcept {
            while (x->Head()) {
                x->Pop();
            }
            delete x;
        }

        void operator ()(TQueueInplace<T, TSize> *x) const noexcept {
            Destroy(x);
        }
    };

    void Push(const T &x) noexcept {
        ++Size;
        if (WritePosition != TChunk::EntriesCount) {
            WriteTo->Entries[WritePosition] = x;
            ++WritePosition;
        } else {
            TChunk *next = new TChunk();
            next->Entries[0] = x;
            WriteTo->Next = next;
            WriteTo = next;
            WritePosition = 1;
        }
    }

    void Push(T &&x) noexcept {
        ++Size;
        if (WritePosition != TChunk::EntriesCount) {
            WriteTo->Entries[WritePosition] = std::move(x);
            ++WritePosition;
        } else {
            TChunk *next = new TChunk();
            next->Entries[0] = std::move(x);
            WriteTo->Next = next;
            WriteTo = next;
            WritePosition = 1;
        }
    }

    T *Head() {
        TChunk *head = ReadFrom;
        if (ReadFrom == WriteTo && ReadPosition == WritePosition) {
            return nullptr;
        } else if (ReadPosition != TChunk::EntriesCount) {
            return &(head->Entries[ReadPosition]);
        } else if (TChunk *next = head->Next) {
            ReadFrom = next;
            delete head;
            ReadPosition = 0;
            return Head();
        }
        return nullptr;
    }

    void Pop() {
        const T *ret = Head();
        if (ret) {
            ++ReadPosition;
            --Size;
        }
    }

    size_t GetSize() const {
        return Size;
    }
};
