#pragma once

#include "defs.h"
#include <ydb/library/actors/util/queue_chunk.h>

namespace NActors {
    // add some concurrency to basic queue to avoid hangs under contention (we pay with memory, so use only when really expect contention)
    // ordering: every completed push guarantied to seen before any not-yet-initiated push. parallel pushes could reorder (and that is natural for concurrent queues).
    // try to place reader/writer on different cache-lines to avoid congestion b/w reader and writers.
    // if strict ordering does not matter - look at TManyOneQueue.

    template <typename T, ui32 TWriteConcurrency = 3, ui32 TSize = 128>
    class TRevolvingMailboxQueue {
        static_assert(std::is_integral<T>::value || std::is_pointer<T>::value, "expect std::is_integral<T>::value || std::is_pointer<T>::value");

        struct TValTagPair {
            volatile T Value;
            volatile ui64 Tag;
        };

        typedef TQueueChunk<TValTagPair, TSize> TChunk;

        static_assert(sizeof(TAtomic) == sizeof(TChunk*), "expect sizeof(TAtomic) == sizeof(TChunk*)");
        static_assert(sizeof(TAtomic) == sizeof(ui64), "expect sizeof(TAtomic) == sizeof(ui64)");

    public:
        class TWriter;

        class TReader {
            TChunk* ReadFrom[TWriteConcurrency];
            ui32 ReadPosition[TWriteConcurrency];

            friend class TRevolvingMailboxQueue<T, TWriteConcurrency, TSize>::TWriter; // for access to ReadFrom in constructor

            bool ChunkHead(ui32 idx, ui64* tag, T* value) {
                TChunk* head = ReadFrom[idx];
                const ui32 pos = ReadPosition[idx];
                if (pos != TChunk::EntriesCount) {
                    if (const T xval = AtomicLoad(&head->Entries[pos].Value)) {
                        const ui64 xtag = head->Entries[pos].Tag;
                        if (xtag < *tag) {
                            *value = xval;
                            *tag = xtag;
                            return true;
                        }
                    }
                } else if (TChunk* next = AtomicLoad(&head->Next)) {
                    ReadFrom[idx] = next;
                    delete head;
                    ReadPosition[idx] = 0;
                    return ChunkHead(idx, tag, value);
                }

                return false;
            }

            T Head(bool pop) {
                ui64 tag = Max<ui64>();
                T ret = T{};
                ui32 idx = 0;

                for (ui32 i = 0; i < TWriteConcurrency; ++i)
                    if (ChunkHead(i, &tag, &ret))
                        idx = i;

                // w/o second pass we could reorder updates with 'already scanned' range
                if (ret) {
                    for (ui32 i = 0; i < TWriteConcurrency; ++i)
                        if (ChunkHead(i, &tag, &ret))
                            idx = i;
                }

                if (pop && ret)
                    ++ReadPosition[idx];

                return ret;
            }

        public:
            TReader() {
                for (ui32 i = 0; i != TWriteConcurrency; ++i) {
                    ReadFrom[i] = new TChunk();
                    ReadPosition[i] = 0;
                }
            }

            ~TReader() {
                Y_DEBUG_ABORT_UNLESS(Head() == 0);
                for (ui32 i = 0; i < TWriteConcurrency; ++i)
                    delete ReadFrom[i];
            }

            T Pop() {
                return Head(true);
            }

            T Head() {
                return Head(false);
            }

            class TReadIterator {
                TChunk* ReadFrom[TWriteConcurrency];
                ui32 ReadPosition[TWriteConcurrency];

                bool ChunkHead(ui32 idx, ui64* tag, T* value) {
                    TChunk* head = ReadFrom[idx];
                    const ui32 pos = ReadPosition[idx];
                    if (pos != TChunk::EntriesCount) {
                        if (const T xval = AtomicLoad(&head->Entries[pos].Value)) {
                            const ui64 xtag = head->Entries[pos].Tag;
                            if (xtag < *tag) {
                                *value = xval;
                                *tag = xtag;
                                return true;
                            }
                        }
                    } else if (TChunk* next = AtomicLoad(&head->Next)) {
                        ReadFrom[idx] = next;
                        ReadPosition[idx] = 0;
                        return ChunkHead(idx, tag, value);
                    }

                    return false;
                }

            public:
                TReadIterator(TChunk* const* readFrom, const ui32* readPosition) {
                    memcpy(ReadFrom, readFrom, TWriteConcurrency * sizeof(TChunk*));
                    memcpy(ReadPosition, readPosition, TWriteConcurrency * sizeof(ui32));
                }

                T Next() {
                    ui64 tag = Max<ui64>();
                    T ret = T{};
                    ui32 idx = 0;

                    for (ui32 i = 0; i < TWriteConcurrency; ++i)
                        if (ChunkHead(i, &tag, &ret))
                            idx = i;

                    // w/o second pass we could reorder updates with 'already scanned' range
                    if (ret) {
                        for (ui32 i = 0; i < TWriteConcurrency; ++i)
                            if (ChunkHead(i, &tag, &ret))
                                idx = i;
                    }

                    if (ret)
                        ++ReadPosition[idx];

                    return ret;
                }
            };

            TReadIterator Iterator() const {
                return TReadIterator(ReadFrom, ReadPosition);
            }
        };

        class TWriter {
            TChunk* volatile WriteTo[TWriteConcurrency];
            volatile ui64 Tag;
            ui32 WritePosition[TWriteConcurrency];

        public:
            TWriter(const TReader& reader)
                : Tag(0)
            {
                for (ui32 i = 0; i != TWriteConcurrency; ++i) {
                    WriteTo[i] = reader.ReadFrom[i];
                    WritePosition[i] = 0;
                }
            }

            bool TryPush(T x) {
                Y_ABORT_UNLESS(x != 0);

                for (ui32 i = 0; i != TWriteConcurrency; ++i) {
                    if (RelaxedLoad(&WriteTo[i]) != nullptr) {
                        if (TChunk* writeTo = AtomicSwap(&WriteTo[i], nullptr)) {
                            const ui64 nextTag = AtomicIncrement(Tag);
                            Y_DEBUG_ABORT_UNLESS(nextTag < Max<ui64>());
                            const ui32 writePosition = WritePosition[i];
                            if (writePosition != TChunk::EntriesCount) {
                                writeTo->Entries[writePosition].Tag = nextTag;
                                AtomicStore(&writeTo->Entries[writePosition].Value, x);
                                ++WritePosition[i];
                            } else {
                                TChunk* next = new TChunk();
                                next->Entries[0].Tag = nextTag;
                                next->Entries[0].Value = x;
                                AtomicStore(&writeTo->Next, next);
                                writeTo = next;
                                WritePosition[i] = 1;
                            }
                            AtomicStore(WriteTo + i, writeTo);
                            return true;
                        }
                    }
                }
                return false;
            }

            ui32 Push(T x) {
                ui32 spins = 0;
                while (!TryPush(x)) {
                    ++spins;
                    SpinLockPause();
                }
                return spins;
            }
        };
    };
}
