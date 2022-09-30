#pragma once

#include "defs.h"
#include "blobstorage_synclogdata.h"
#include "blobstorage_synclogkeeper_committer.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogRepaired
        // This is output of local recovery process concerning SyncLog, we pass
        // it to new SyncLog actor and then to SyncLogKeeper respectively
        ////////////////////////////////////////////////////////////////////////////
        struct TSyncLogRepaired {
            TSyncLogPtr SyncLogPtr;
            TVector<ui32> ChunksToDelete;
            TCommitHistory CommitHistory;
            bool NeedsInitialCommit;

            ~TSyncLogRepaired() = default;

            // return nullptr on error
            static std::unique_ptr<TSyncLogRepaired> Construct(
                TSyncLogParams &&params,
                const TString &data,
                ui64 entryPointLsn,
                TString &explanation);

            static std::unique_ptr<TSyncLogRepaired> Construct(
                TSyncLogParams &&params,
                const char *data,
                size_t size,
                ui64 entryPointLsn,
                TString &explanation);

        private:
            TSyncLogRepaired(
                TSyncLogPtr &&syncLog,
                TVector<ui32> &&chunksToDelete,
                TCommitHistory &&commitHistory,
                bool needsInitialCommit);
        };


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogRecovery
        // The class manages the process of local recovery for SyncLog
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogRecovery : public TThrRefBase {
        public:
            using TLevelSegment = NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;

        public:
            TSyncLogRecovery(std::unique_ptr<TSyncLogRepaired> &&repaired);
            void PutLogoBlob(
                const TBlobStorageGroupType &gtype,
                ui64 lsn,
                const TLogoBlobID &id,
                const TIngress &ingress);
            void PutBlock(ui64 lsn, ui64 tabletId, ui32 gen);
            void PutGC(
                const TBlobStorageGroupType &gtype,
                ui64 lsn,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                const TBarrierIngress &ingress);
            void PutBarrier(
                ui64 lsn,
                ui64 tabletId,
                ui32 channel,
                ui32 gen,
                ui32 genCounter,
                ui32 collectGen,
                ui32 collectStep,
                bool hard,
                const TBarrierIngress &ingress);
            std::unique_ptr<TSyncLogRepaired> ReleaseRepaired();
            ui64 GetLastLsnOfIndexRecord() const;
            ui64 GetLastLsn() const;
            const TSyncLogHeader &GetSyncLogHeader() const;
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;

        private:
            TString ToString() const;

            std::unique_ptr<TSyncLogRepaired> Repaired;
            ui64 AddSegs = 0;
            ui64 LogoBlobs = 0;
            ui64 Blocks = 0;
            ui64 Gcs = 0;
            ui64 Barriers = 0;
        };

    } // NSyncLog
} // NKikimr
