#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"

namespace NKikimr {
    namespace NIncrHuge {

        class TAllocator
            : public TKeeperComponentBase
        {
            // is there a chunk reservation query in flight?
            bool ChunkReserveInFlight = false;

            // number of reserved, but uncommitted chunks
            ui32 NumReservedUncommittedChunks = 0;

        public:
            TAllocator(TKeeper& keeper);
            ~TAllocator();

            // this function is called whenever intent queue item is used; it checks if actor should allocate more chunks
            // for operation
            void CheckForAllocationNeed(const TActorContext& ctx);

            // callback function that is invoked on chunk reservation success/failure; this function is called by keeper
            // on TEvChunkWriteResult event reception
            void ApplyAllocate(NKikimrProto::EReplyStatus status, TVector<TChunkIdx>&& chunks, const TActorContext& ctx);
        };

    } // NIncrHuge
} // NKikimr
