#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"
#include "incrhuge.h"

#include <util/generic/bitmap.h>

namespace NKikimr {
    namespace NIncrHuge {

        class TDeleter
            : public TKeeperComponentBase
        {
            // vector of sequence numbers of deletion for each owner
            std::array<ui64, 256> OwnerToSeqNo;

            struct TDeleteQueueItem;
            using TDeleteQueue = TList<TDeleteQueueItem>;
            TDeleteQueue DeleteQueue;
            THashMap<TIncrHugeBlobId, TDeleteQueue::iterator> WriteInProgress;

        public:
            TDeleter(TKeeper& keeper);
            ~TDeleter();

            // delete request handler
            void HandleDelete(TEvIncrHugeDelete::TPtr& ev, const TActorContext& ctx);

            // delete some locators generated while defragmenting
            void DeleteDefrag(TVector<TBlobDeleteLocator>&& deleteLocators, const TActorContext& ctx);

            // issue chunk deletion request after it is completely freed
            void IssueLogChunkDelete(TChunkIdx chunkIdx, const TActorContext& ctx);

            // setup owner's position when doing recovery
            void InsertOwnerOnRecovery(ui8 owner, ui64 seqNo);

            void OnItemDefragWritten(TIncrHugeBlobId id, const TActorContext& ctx);

        private:
            void ProcessDeleteItem(TDeleteQueue::iterator it, const TActorContext& ctx);

            // post-log part of delete process (when log finished successfully)
            void ProcessDeletedLocators(const TVector<TBlobDeleteLocator>& deleteLocators, bool deleteFromLookup,
                    const TActorContext& ctx);
        };

    } // NIncrHuge
} // NKikimr
