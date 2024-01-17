#pragma once

#include "scheduler_cookie.h"

#include <ydb/library/actors/util/queue_chunk.h>
#include <ydb/library/actors/core/event.h>

namespace NActors {
    class IEventHandle;
    class ISchedulerCookie;

    namespace NSchedulerQueue {
        struct TEntry {
            ui64 InstantMicroseconds;
            IEventHandle* Ev;
            ISchedulerCookie* Cookie;
        };

        struct TChunk : TQueueChunkDerived<TEntry, 512, TChunk> {};

        class TReader;
        class TWriter;
        class TWriterWithPadding;

        class TReader : ::TNonCopyable {
            TChunk* ReadFrom;
            ui32 ReadPosition;

            friend class TWriter;

        public:
            TReader()
                : ReadFrom(new TChunk())
                , ReadPosition(0)
            {
            }

            ~TReader() {
                while (TEntry* x = Pop()) {
                    if (x->Cookie)
                        x->Cookie->Detach();
                    delete x->Ev;
                }
                delete ReadFrom;
            }

            TEntry* Pop() {
                TChunk* head = ReadFrom;
                if (ReadPosition != TChunk::EntriesCount) {
                    if (AtomicLoad(&head->Entries[ReadPosition].InstantMicroseconds) != 0)
                        return const_cast<TEntry*>(&head->Entries[ReadPosition++]);
                    else
                        return nullptr;
                } else if (TChunk* next = AtomicLoad(&head->Next)) {
                    ReadFrom = next;
                    delete head;
                    ReadPosition = 0;
                    return Pop();
                }

                return nullptr;
            }
        };

        class TWriter : ::TNonCopyable {
            TChunk* WriteTo;
            ui32 WritePosition;

        public:
            TWriter()
                : WriteTo(nullptr)
                , WritePosition(0)
            {
            }

            void Init(const TReader& reader) {
                WriteTo = reader.ReadFrom;
                WritePosition = 0;
            }

            void Push(ui64 instantMicrosends, IEventHandle* ev, ISchedulerCookie* cookie) {
                if (Y_UNLIKELY(instantMicrosends == 0)) {
                    // Protect against Pop() getting stuck forever
                    instantMicrosends = 1;
                }
                if (WritePosition != TChunk::EntriesCount) {
                    volatile TEntry& entry = WriteTo->Entries[WritePosition];
                    entry.Cookie = cookie;
                    entry.Ev = ev;
                    AtomicStore(&entry.InstantMicroseconds, instantMicrosends);
                    ++WritePosition;
                } else {
                    TChunk* next = new TChunk();
                    volatile TEntry& entry = next->Entries[0];
                    entry.Cookie = cookie;
                    entry.Ev = ev;
                    entry.InstantMicroseconds = instantMicrosends;
                    AtomicStore(&WriteTo->Next, next);
                    WriteTo = next;
                    WritePosition = 1;
                }
            }
        };

        class TWriterWithPadding: public TWriter {
        private:
            ui8 CacheLinePadding[64 - sizeof(TWriter)];

            void UnusedCacheLinePadding() {
                Y_UNUSED(CacheLinePadding);
            }
        };

        struct TQueueType {
            TReader Reader;
            TWriter Writer;

            TQueueType() {
                Writer.Init(Reader);
            }
        };
    }
}
