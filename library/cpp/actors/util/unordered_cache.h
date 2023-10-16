#pragma once

#include "defs.h"
#include "queue_chunk.h"

template <typename T, ui32 Size = 512, ui32 ConcurrencyFactor = 1, typename TChunk = TQueueChunk<T, Size>>
class TUnorderedCache : TNonCopyable {
    static_assert(std::is_integral<T>::value || std::is_pointer<T>::value, "expect std::is_integral<T>::value || std::is_pointer<T>::value");

public:
    static constexpr ui32 Concurrency = ConcurrencyFactor * 4;

private:
    struct TReadSlot {
        TChunk* volatile ReadFrom;
        volatile ui32 ReadPosition;
        char Padding[64 - sizeof(TChunk*) - sizeof(ui32)]; // 1 slot per cache line
    };

    struct TWriteSlot {
        TChunk* volatile WriteTo;
        volatile ui32 WritePosition;
        char Padding[64 - sizeof(TChunk*) - sizeof(ui32)]; // 1 slot per cache line
    };

    static_assert(sizeof(TReadSlot) == 64, "expect sizeof(TReadSlot) == 64");
    static_assert(sizeof(TWriteSlot) == 64, "expect sizeof(TWriteSlot) == 64");

private:
    TReadSlot ReadSlots[Concurrency];
    TWriteSlot WriteSlots[Concurrency];

    static_assert(sizeof(TChunk*) == sizeof(TAtomic), "expect sizeof(TChunk*) == sizeof(TAtomic)");

private:
    struct TLockedWriter {
        TWriteSlot* Slot;
        TChunk* WriteTo;

        TLockedWriter()
            : Slot(nullptr)
            , WriteTo(nullptr)
        { }

        TLockedWriter(TWriteSlot* slot, TChunk* writeTo)
            : Slot(slot)
            , WriteTo(writeTo)
        { }

        ~TLockedWriter() noexcept {
            Drop();
        }

        void Drop() {
            if (Slot) {
                AtomicStore(&Slot->WriteTo, WriteTo);
                Slot = nullptr;
            }
        }

        TLockedWriter(const TLockedWriter&) = delete;
        TLockedWriter& operator=(const TLockedWriter&) = delete;

        TLockedWriter(TLockedWriter&& rhs)
            : Slot(rhs.Slot)
            , WriteTo(rhs.WriteTo)
        {
            rhs.Slot = nullptr;
        }

        TLockedWriter& operator=(TLockedWriter&& rhs) {
            if (Y_LIKELY(this != &rhs)) {
                Drop();
                Slot = rhs.Slot;
                WriteTo = rhs.WriteTo;
                rhs.Slot = nullptr;
            }
            return *this;
        }
    };

private:
    TLockedWriter LockWriter(ui64 writerRotation) {
        ui32 cycle = 0;
        for (;;) {
            TWriteSlot* slot = &WriteSlots[writerRotation % Concurrency];
            if (AtomicLoad(&slot->WriteTo) != nullptr) {
                if (TChunk* writeTo = AtomicSwap(&slot->WriteTo, nullptr)) {
                    return TLockedWriter(slot, writeTo);
                }
            }
            ++writerRotation;

            // Do a spinlock pause after a full cycle
            if (++cycle == Concurrency) {
                SpinLockPause();
                cycle = 0;
            }
        }
    }

    void WriteOne(TLockedWriter& lock, T x) {
        Y_DEBUG_ABORT_UNLESS(x != 0);

        const ui32 pos = AtomicLoad(&lock.Slot->WritePosition);
        if (pos != TChunk::EntriesCount) {
            AtomicStore(&lock.Slot->WritePosition, pos + 1);
            AtomicStore(&lock.WriteTo->Entries[pos], x);
        } else {
            TChunk* next = new TChunk();
            AtomicStore(&next->Entries[0], x);
            AtomicStore(&lock.Slot->WritePosition, 1u);
            AtomicStore(&lock.WriteTo->Next, next);
            lock.WriteTo = next;
        }
    }

public:
    TUnorderedCache() {
        for (ui32 i = 0; i < Concurrency; ++i) {
            ReadSlots[i].ReadFrom = new TChunk();
            ReadSlots[i].ReadPosition = 0;

            WriteSlots[i].WriteTo = ReadSlots[i].ReadFrom;
            WriteSlots[i].WritePosition = 0;
        }
    }

    ~TUnorderedCache() {
        Y_ABORT_UNLESS(!Pop(0));

        for (ui64 i = 0; i < Concurrency; ++i) {
            if (ReadSlots[i].ReadFrom) {
                delete ReadSlots[i].ReadFrom;
                ReadSlots[i].ReadFrom = nullptr;
            }
            WriteSlots[i].WriteTo = nullptr;
        }
    }

    T Pop(ui64 readerRotation) noexcept {
        ui64 readerIndex = readerRotation;
        const ui64 endIndex = readerIndex + Concurrency;
        for (; readerIndex != endIndex; ++readerIndex) {
            TReadSlot* slot = &ReadSlots[readerIndex % Concurrency];
            if (AtomicLoad(&slot->ReadFrom) != nullptr) {
                if (TChunk* readFrom = AtomicSwap(&slot->ReadFrom, nullptr)) {
                    const ui32 pos = AtomicLoad(&slot->ReadPosition);
                    if (pos != TChunk::EntriesCount) {
                        if (T ret = AtomicLoad(&readFrom->Entries[pos])) {
                            AtomicStore(&slot->ReadPosition, pos + 1);
                            AtomicStore(&slot->ReadFrom, readFrom); // release lock with same chunk
                            return ret;                             // found, return
                        } else {
                            AtomicStore(&slot->ReadFrom, readFrom); // release lock with same chunk
                        }
                    } else if (TChunk* next = AtomicLoad(&readFrom->Next)) {
                        if (T ret = AtomicLoad(&next->Entries[0])) {
                            AtomicStore(&slot->ReadPosition, 1u);
                            AtomicStore(&slot->ReadFrom, next); // release lock with next chunk
                            delete readFrom;
                            return ret;
                        } else {
                            AtomicStore(&slot->ReadPosition, 0u);
                            AtomicStore(&slot->ReadFrom, next); // release lock with new chunk
                            delete readFrom;
                        }
                    } else {
                        // nothing in old chunk and no next chunk, just release lock with old chunk
                        AtomicStore(&slot->ReadFrom, readFrom);
                    }
                }
            }
        }

        return 0; // got nothing after full cycle, return
    }

    void Push(T x, ui64 writerRotation) {
        TLockedWriter lock = LockWriter(writerRotation);
        WriteOne(lock, x);
    }

    void PushBulk(T* x, ui32 xcount, ui64 writerRotation) {
        for (;;) {
            // Fill no more then one queue chunk per round
            const ui32 xround = Min(xcount, (ui32)TChunk::EntriesCount);

            {
                TLockedWriter lock = LockWriter(writerRotation++);
                for (T* end = x + xround; x != end; ++x)
                    WriteOne(lock, *x);
            }

            if (xcount <= TChunk::EntriesCount)
                break;

            xcount -= TChunk::EntriesCount;
        }
    }
};
