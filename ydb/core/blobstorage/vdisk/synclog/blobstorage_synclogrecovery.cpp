#include "blobstorage_synclogrecovery.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst_it.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogRepaired
        ////////////////////////////////////////////////////////////////////////////
        std::unique_ptr<TSyncLogRepaired> TSyncLogRepaired::Construct(
                TSyncLogParams &&params,
                const TString &data,
                ui64 entryPointLsn,
                TString &explanation)
        {
            return Construct(std::move(params), data.data(), data.size(), entryPointLsn, explanation);
        }

        std::unique_ptr<TSyncLogRepaired> TSyncLogRepaired::Construct(
                TSyncLogParams &&params,
                const char* data,
                size_t size,
                ui64 entryPointLsn,
                TString &explanation)
        {
            TEntryPointParser parser(std::move(params));
            bool needsInitialCommit = false;
            bool success = parser.ParseArray(data, size, needsInitialCommit, explanation);
            if (!success) {
                return nullptr;
            }
            // build commit history
            const ui64 recoveryLogConfirmedLsn = parser.GetRecoveryLogConfirmedLsn();
            TCommitHistory commitHistory(TAppData::TimeProvider->Now(), entryPointLsn, recoveryLogConfirmedLsn);
            // build TSyncLogRepaired
            return std::unique_ptr<TSyncLogRepaired>(new TSyncLogRepaired(parser.GetSyncLogPtr(),
                parser.GetChunksToDelete(),
                std::move(commitHistory),
                needsInitialCommit));
        }

        TSyncLogRepaired::TSyncLogRepaired(
                TSyncLogPtr &&syncLog,
                TVector<ui32> &&chunksToDelete,
                TCommitHistory &&commitHistory,
                bool needsInitialCommit)
            : SyncLogPtr(std::move(syncLog))
            , ChunksToDelete(std::move(chunksToDelete))
            , CommitHistory(std::move(commitHistory))
            , NeedsInitialCommit(needsInitialCommit)
        {}


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogRecovery
        ////////////////////////////////////////////////////////////////////////////
        TSyncLogRecovery::TSyncLogRecovery(std::unique_ptr<TSyncLogRepaired> &&repaired) {
            Repaired = std::move(repaired);
        }

        void TSyncLogRecovery::PutLogoBlob(
                const TBlobStorageGroupType &gtype,
                ui64 lsn,
                const TLogoBlobID &id,
                const TIngress &ingress)
        {
            Y_ABORT_UNLESS(GetLastLsnOfIndexRecord() < lsn,
                     "State# %s lsn# %" PRIu64, ToString().data(), lsn);
            ++LogoBlobs;

            char buf[NSyncLog::MaxRecFullSize];
            ui32 size = NSyncLog::TSerializeRoutines::SetLogoBlob(gtype, buf, lsn, id, ingress);
            Repaired->SyncLogPtr->PutOne((const NSyncLog::TRecordHdr *)buf, size);
        }

        void TSyncLogRecovery::PutBlock(ui64 lsn, ui64 tabletId, ui32 gen) {
            Y_ABORT_UNLESS(GetLastLsnOfIndexRecord() < lsn,
                     "State# %s lsn# %" PRIu64, ToString().data(), lsn);
            ++Blocks;

            char buf[NSyncLog::MaxRecFullSize];
            ui32 size = NSyncLog::TSerializeRoutines::SetBlock(buf, lsn, tabletId, gen, 0);
            Repaired->SyncLogPtr->PutOne((const NSyncLog::TRecordHdr *)buf, size);
        }

        void TSyncLogRecovery::PutGC(
                const TBlobStorageGroupType &gtype,
                ui64 lsn,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                const TBarrierIngress &ingress)
        {
            Y_ABORT_UNLESS(GetLastLsnOfIndexRecord() < lsn,
                     "State# %s lsn# %" PRIu64, ToString().data(), lsn);
            ++Gcs;

            const bool collect = record.HasCollectGeneration();
            ui32 vecSize = (!!collect +
                            record.KeepSize() +
                            record.DoNotKeepSize()) * NSyncLog::MaxRecFullSize;

            TVector<char> buf;
            buf.resize(vecSize);
            ui32 size = NSyncLog::TSerializeRoutines::SetGC(gtype,
                                                            &(buf[0]),
                                                            lsn,
                                                            GetLastLsnOfIndexRecord(),
                                                            record,
                                                            ingress);
            Repaired->SyncLogPtr->PutMany(&(buf[0]), size);
        }

        void TSyncLogRecovery::PutBarrier(
                ui64 lsn,
                ui64 tabletId,
                ui32 channel,
                ui32 gen,
                ui32 genCounter,
                ui32 collectGen,
                ui32 collectStep,
                bool hard,
                const TBarrierIngress &ingress)
        {
            Y_ABORT_UNLESS(GetLastLsnOfIndexRecord() < lsn,
                     "State# %s lsn# %" PRIu64, ToString().data(), lsn);
            ++Barriers;

            char buf[NSyncLog::MaxRecFullSize];
            ui32 size = NSyncLog::TSerializeRoutines::SetBarrier(buf, lsn, tabletId,
                                                                 channel, gen, genCounter,
                                                                 collectGen, collectStep,
                                                                 hard, ingress);
            Repaired->SyncLogPtr->PutOne((const NSyncLog::TRecordHdr *)buf, size);
        }

        std::unique_ptr<TSyncLogRepaired> TSyncLogRecovery::ReleaseRepaired() {
            // after finishing recovery check that Dsk and Mem do not intersect
            Y_ABORT_UNLESS(Repaired->SyncLogPtr->CheckMemAndDiskRecLogsDoNotIntersect(),
                     "%s", Repaired->SyncLogPtr->BoundariesToString().data());
            return std::exchange(Repaired, nullptr);
        }

        ui64 TSyncLogRecovery::GetLastLsnOfIndexRecord() const {
            return Repaired->SyncLogPtr->LastLsnOfIndexRecord;
        }

        ui64 TSyncLogRecovery::GetLastLsn() const {
            return Repaired->SyncLogPtr->GetLastLsn();
        }

        const TSyncLogHeader &TSyncLogRecovery::GetSyncLogHeader() const {
            return Repaired->SyncLogPtr->Header;
        }

        void TSyncLogRecovery::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            for (TChunkIdx chunkIdx : Repaired->ChunksToDelete) {
                const bool inserted = chunks.insert(chunkIdx).second;
                Y_ABORT_UNLESS(inserted);
            }
            Repaired->SyncLogPtr->GetOwnedChunks(chunks);
        }

        TString TSyncLogRecovery::ToString() const {
            TStringStream s;
            s << "{GetLastLsnOfIndexRecord# " << GetLastLsnOfIndexRecord()
              << " AddSegs# " << AddSegs << " LogoBlobs# " << LogoBlobs
              << " Blocks# " << Blocks << " Gcs# " << Gcs
              << " Barriers# " << Barriers << "}";
            return s.Str();
        }

    } // NSyncLog
} // NKikimr
