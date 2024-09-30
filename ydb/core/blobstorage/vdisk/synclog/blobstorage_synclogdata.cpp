#include "blobstorage_synclogdata.h"
#include "blobstorage_synclogrecovery.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

using namespace NKikimrServices;

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TEntryPointDbgInfo
        ////////////////////////////////////////////////////////////////////////////
        void TEntryPointDbgInfo::Output(IOutputStream &s) const {

            s << "{ByteSize# " << ByteSize << " ChunkToDeleteNum# "
              << ChunkToDeleteNum << " IndexRecsNum# " << IndexRecsNum << "}";
        }

        TString TEntryPointDbgInfo::ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogHeader
        ////////////////////////////////////////////////////////////////////////////
        bool TSyncLogHeader::CheckEntryPoint(const char *pos, const char *end) {
            if (size_t(end - pos) != HdrSize)
                return false;

            // check signature
            if (*(const ui32*)pos != SyncLogOldSignature)
                return false;
            pos += sizeof(ui32);

            // reserved bytes
            if (*(const ui32*)pos != 0)
                return false;
            pos += sizeof(ui32);

            // PDiskGuid
            pos += sizeof(ui64);

            // VDiskIncarnationGuid
            pos += sizeof(TVDiskIncarnationGuid);
            return true;
        }

        TSyncLogHeader TSyncLogHeader::Constructor(const char *pos, const char *end) {
            Y_UNUSED(end);
            pos += sizeof(ui32) + sizeof(ui32);    // Signature and Reserved
            ui64 pdiskGuid = *(const ui64 *)pos;
            pos += sizeof(ui64);
            TVDiskIncarnationGuid vdiskIncarnationGuid = *(const TVDiskIncarnationGuid *)pos;
            pos += sizeof(TVDiskIncarnationGuid);

            return TSyncLogHeader(pdiskGuid, vdiskIncarnationGuid);
        }

        ////////////////////////////////////////////////////////////////////////////
        // TLogEssence
        ////////////////////////////////////////////////////////////////////////////
        TLogEssence::TLogEssence()
            : LogStartLsn(0)
            , MemLogEmpty(true)
            , DiskLogEmpty(true)
            , FirstMemLsn(0)
            , LastMemLsn(0)
            , FirstDiskLsn(0)
            , LastDiskLsn(0)
        {}

        TLogEssence::TLogEssence(
                ui64 logStartLsn,
                bool memLogEmpty,
                bool diskLogEmpty,
                ui64 firstMemLsn,
                ui64 lastMemLsn,
                ui64 firstDiskLsn,
                ui64 lastDiskLsn)
            : LogStartLsn(logStartLsn)
            , MemLogEmpty(memLogEmpty)
            , DiskLogEmpty(diskLogEmpty)
            , FirstMemLsn(firstMemLsn)
            , LastMemLsn(lastMemLsn)
            , FirstDiskLsn(firstDiskLsn)
            , LastDiskLsn(lastDiskLsn)
        {}

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        void TSyncLogSnapshot::FillInLogEssence(TLogEssence *e) const {
            e->LogStartLsn = LogStartLsn;
            e->MemLogEmpty = MemSnapPtr->Empty();
            e->DiskLogEmpty = DiskSnapPtr->Empty();
            e->FirstMemLsn = (e->MemLogEmpty ? 0 : MemSnapPtr->GetFirstLsn());
            e->LastMemLsn = (e->MemLogEmpty ? 0 : MemSnapPtr->GetLastLsn());
            e->FirstDiskLsn = (e->DiskLogEmpty ? 0 : DiskSnapPtr->GetFirstLsn());
            e->LastDiskLsn = (e->DiskLogEmpty ? 0 : DiskSnapPtr->GetLastLsn());
        }

        TString TSyncLogSnapshot::BoundariesToString() const {
            return Sprintf("{LogStartLsn: %" PRIu64 " %s %s}", LogStartLsn,
                           MemSnapPtr->BoundariesToString().data(), DiskSnapPtr->BoundariesToString().data());
        }

        TSyncLogSnapshot::TSyncLogSnapshot(TDiskRecLogSnapshotPtr diskSnapPtr,
                                           TMemRecLogSnapshotPtr memSnapPtr,
                                           ui64 logStartLsn,
                                           ui32 appendBlockSize,
                                           const TEntryPointDbgInfo &lastEntryPointDbgInfo,
                                           const TSyncLogHeader &header)
            : DiskSnapPtr(diskSnapPtr)
            , MemSnapPtr(memSnapPtr)
            , LogStartLsn(logStartLsn)
            , AppendBlockSize(appendBlockSize)
            , LastEntryPointDbgInfo(lastEntryPointDbgInfo)
            , Header(header)
        {
            CheckSnapshotConsistency(); // For debug
        }

        void TSyncLogSnapshot::CheckSnapshotConsistency() const {
            if (!MemSnapPtr->Empty()) {
                ui64 memFirstLsn = MemSnapPtr->GetFirstLsn();
                ui64 memLastLsn = MemSnapPtr->GetLastLsn();
                Y_ABORT_UNLESS(memFirstLsn <= memLastLsn, "%s", BoundariesToString().data());
                // For paranoid mode we can check memory snapshot
                // consistency by calling "MemSnapPtr->CheckSnapshotConsistency()",
                // but it's heavy, turned off by default
            }

            if (!DiskSnapPtr->Empty()) {
                ui64 diskFirstLsn = DiskSnapPtr->GetFirstLsn();
                ui64 diskLastLsn = DiskSnapPtr->GetLastLsn();
                Y_ABORT_UNLESS(diskFirstLsn <= diskLastLsn, "%s", BoundariesToString().data());
            }
        }

        ui32 TSyncLogSnapshot::SerializeToProto(
                NKikimrVDiskData::TSyncLogEntryPoint &pb,
                const TDeltaToDiskRecLog &delta)
        {
            pb.SetPDiskGuid(Header.PDiskGuid);
            pb.SetVDiskIncarnationGuid(Header.VDiskIncarnationGuid);
            pb.SetLogStartLsn(LogStartLsn);
            // DiskRecLog
            TStringStream s;
            ui32 indexRecsNum = DiskSnapPtr->Serialize(s, delta);
            pb.SetDiskRecLogSerialized(s.Str());
            return indexRecsNum;
        }

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogParams
        ////////////////////////////////////////////////////////////////////////////
        TSyncLogParams::TSyncLogParams(ui64 pDiskGuid,
                                       ui64 chunkSize,
                                       ui32 appendBlockSize,
                                       ui32 syncLogAdvisedIndexedBlockSize,
                                       const TMemoryConsumer &memSizeBytes)
            : PDiskGuid(pDiskGuid)
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , IndexBulk(CalculateIndexBulk(syncLogAdvisedIndexedBlockSize, appendBlockSize, chunkSize))
            , MemSizeBytes(memSizeBytes)
        {}

        ui32 TSyncLogParams::CalculateIndexBulk(
                ui32 syncLogAdvisedIndexedBlockSize,
                ui32 appendBlockSize,
                ui32 chunkSize)
        {
            ui32 bytes = syncLogAdvisedIndexedBlockSize;
            ui32 indexBulk = 1;
            if (appendBlockSize) {
                indexBulk = bytes / appendBlockSize;
                if (indexBulk == 0)
                    indexBulk = 1;
                else if (indexBulk > (chunkSize / appendBlockSize))
                    indexBulk = chunkSize / appendBlockSize;
            }
            return indexBulk;
        }

        ////////////////////////////////////////////////////////////////////////
        // TSyncLog: Getters/Checks
        ////////////////////////////////////////////////////////////////////////
        ui64 TSyncLog::GetLastLsn() const {
            TMaybe<ui64> e; // empty
            TMaybe<ui64> memLsn = MemRecLog.Empty() ? e : TMaybe<ui64>(MemRecLog.GetLastLsn());
            TMaybe<ui64> diskLsn = DiskRecLog.Empty() ? e : TMaybe<ui64>(DiskRecLog.GetLastLsn());
            TMaybe<ui64> startLsn = LogStartLsn > 0 ? LogStartLsn - 1 : 0;

            auto maxMaybe = [] (TMaybe<ui64> v1, TMaybe<ui64> v2) {
                // rules:
                // 1. none-null value greater than null value
                // 2. none-null values are compared via library Max
                if (!v1) {
                    return v2;
                } else if (!v2) {
                    return v1;
                } else {
                    return TMaybe<ui64>(Max(*v1, *v2));
                }
            };

            return *maxMaybe(startLsn, maxMaybe(memLsn, diskLsn));
        }

        ui64 TSyncLog::FirstLsnToKeep() const {
            ui64 unwrittenLsn = 0;
            if (MemRecLog.Empty()) {
                unwrittenLsn = ui64(-1);
            } else {
                if (DiskRecLog.Empty()) {
                    unwrittenLsn = MemRecLog.GetFirstLsn();
                } else {
                    // both MemRec and DiskRec are not empty
                    const ui64 lastMemLsn = MemRecLog.GetLastLsn();
                    const ui64 lastDiskLsn = DiskRecLog.GetLastLsn();
                    if (lastMemLsn <= lastDiskLsn) {
                        // all mem is written to disk
                        unwrittenLsn = ui64(-1);
                    } else {
                        const ui64 firstMemLsn = MemRecLog.GetFirstLsn();
                        if (firstMemLsn > lastDiskLsn) {
                            unwrittenLsn = firstMemLsn;
                        } else {
                            unwrittenLsn = DiskRecLog.GetLastLsn() + 1;
                        }
                    }
                }
            }

            // we don't need records below LogStartLsn
            return Max(unwrittenLsn, LogStartLsn);
        }

        TString TSyncLog::FirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{LogStartLsn# " << LogStartLsn
                << " MemRecLog.Empty# " << MemRecLog.Empty()
                << " DiskRecLog.Empty# " << DiskRecLog.Empty();
            if (!MemRecLog.Empty()) {
                str << " MemRecLog.GetFirstLsn# " << MemRecLog.GetFirstLsn()
                    << " MemRecLog.GetLastLsn# " << MemRecLog.GetLastLsn();
            }
            if (!DiskRecLog.Empty()) {
                str << " DiskRecLog.GetFirstLsn# " << DiskRecLog.GetFirstLsn()
                    << " DiskRecLog.GetLastLsn# " << DiskRecLog.GetLastLsn();
            }
            str << "}";
            return str.Str();
        }

        const TEntryPointDbgInfo &TSyncLog::GetLastEntryPointDbgInfo() const {
            return LastEntryPointDbgInfo;
        }

        TString TSyncLog::BoundariesToString() const {
            return Sprintf("{LogStartLsn: %" PRIu64 " LastLsnOfIndexRecord: %" PRIu64
                           " %s %s}", LogStartLsn, LastLsnOfIndexRecord,
                           MemRecLog.BoundariesToString().data(), DiskRecLog.BoundariesToString().data());
        }

        TSyncLogSnapshotPtr TSyncLog::GetSnapshot() const {
            return TSyncLogSnapshotPtr(new TSyncLogSnapshot(DiskRecLog.GetSnapshot(),
                                                            MemRecLog.GetSnapshot(),
                                                            LogStartLsn,
                                                            DiskRecLog.AppendBlockSize,
                                                            LastEntryPointDbgInfo,
                                                            Header));
        }

        bool TSyncLog::CheckMemAndDiskRecLogsDoNotIntersect() const {
            if (!MemRecLog.Empty() && !DiskRecLog.Empty()) {
                const ui64 firstMem = MemRecLog.GetFirstLsn();
                const ui64 lastMem = MemRecLog.GetLastLsn();
                const ui64 firstDisk = DiskRecLog.GetFirstLsn();
                const ui64 lastDisk = DiskRecLog.GetLastLsn();
                return (lastDisk < firstMem) || (lastMem < firstDisk);
            } else {
                return true;
            }
        }

        void TSyncLog::FillInLogEssence(TLogEssence *e) const {
            e->LogStartLsn = LogStartLsn;
            e->MemLogEmpty = MemRecLog.Empty();
            e->DiskLogEmpty = DiskRecLog.Empty();
            e->FirstMemLsn = (e->MemLogEmpty ? 0 : MemRecLog.GetFirstLsn());
            e->LastMemLsn = (e->MemLogEmpty ? 0 : MemRecLog.GetLastLsn());
            e->FirstDiskLsn = (e->DiskLogEmpty ? 0 : DiskRecLog.GetFirstLsn());
            e->LastDiskLsn = (e->DiskLogEmpty ? 0 : DiskRecLog.GetLastLsn());
        }

        ////////////////////////////////////////////////////////////////////////
        // TSyncLog: Puts/Trims
        ////////////////////////////////////////////////////////////////////////
        void TSyncLog::PutOne(const TRecordHdr *rec, ui32 size) {
            MemRecLog.PutOne(rec, size);
        }

        void TSyncLog::PutMany(const void *buf, ui32 size) {
            MemRecLog.PutMany(buf, size);
        }

        TVector<ui32> TSyncLog::TrimLogByConfirmedLsn(
            ui64 confirmedCutLsn,
            std::shared_ptr<IActorNotify> notifier,
            std::function<void(const TString&)> logger)
        {
            if (LogStartLsn > confirmedCutLsn + 1) {
                // if confirmedCutLsn is outdates, ignore it, write to the log
                TStringStream s;
                s << "cut log command is outdated: confirmedCutLsn# " << confirmedCutLsn
                  << " SyncLog# " << BoundariesToString();
                logger(s.Str());
                return {};
            } else {
                TVector<ui32> chunks;
                DiskRecLog.TrimLog(confirmedCutLsn, std::move(notifier), chunks);
                MemRecLog.TrimLog(confirmedCutLsn);
                LogStartLsn = confirmedCutLsn + 1;
                return chunks;
            }
        }

        TVector<ui32> TSyncLog::TrimLogByRemovingChunks(
            ui32 numChunksToDel,
            std::shared_ptr<IActorNotify> notifier)
        {
            TVector<ui32> chunks;
            ui64 sLsn = DiskRecLog.DeleteChunks(numChunksToDel, std::move(notifier), chunks);
            Y_ABORT_UNLESS(LogStartLsn <= sLsn, "sLsn# %" PRIu64 " %s", sLsn, BoundariesToString().data());
            LogStartLsn = sLsn;
            return chunks;
        }

        ////////////////////////////////////////////////////////////////////////
        // Funcs about MemRecLog
        ////////////////////////////////////////////////////////////////////////
        ui32 TSyncLog::GetNumberOfPagesInMemory() const {
            return MemRecLog.GetNumberOfPages();
        }

        ui32 TSyncLog::GetAppendBlockSize() const {
            return MemRecLog.AppendBlockSize;
        }

        ui32 TSyncLog::RemoveCachedPages(ui32 pagesMax, ui64 diskLastLsn) {
            return MemRecLog.RemoveCachedPages(pagesMax, diskLastLsn);
        }

        TMemRecLogSnapshotPtr TSyncLog::BuildMemSwapSnapshot(ui64 diskLastLsn,
                                                             ui64 freeUpToLsn, // excluding
                                                             ui32 freeNPages) {
            return MemRecLog.BuildSwapSnapshot(diskLastLsn, freeUpToLsn, freeNPages);
        }


        ////////////////////////////////////////////////////////////////////////
        // TSyncLog: Funcs about DiskRecLog
        ////////////////////////////////////////////////////////////////////////
        ui32 TSyncLog::HowManyChunksAdds(const TMemRecLogSnapshotPtr &swapSnap) const {
            return DiskRecLog.HowManyChunksAdds(swapSnap);
        }

        ui32 TSyncLog::GetSizeInChunks() const {
            return DiskRecLog.GetSizeInChunks();
        }

        // returns lsn of last record stored in DiskRecLog or 0 if it's empty
        ui64 TSyncLog::GetDiskLastLsn() const {
            return DiskRecLog.Empty() ? 0 : DiskRecLog.GetLastLsn();
        }

        // update index of DiskRecLog
        void TSyncLog::UpdateDiskIndex(const TDeltaToDiskRecLog &delta,
                                       const TEntryPointDbgInfo &dbg) {
            DiskRecLog.UpdateIndex(delta);
            LastEntryPointDbgInfo = dbg;
        }

        ui32 TSyncLog::GetChunkSize() const {
            return DiskRecLog.ChunkSize;
        }

        void TSyncLog::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            DiskRecLog.GetOwnedChunks(chunks);
        }

        ////////////////////////////////////////////////////////////////////////
        // TSyncLog: PRIVATE
        ////////////////////////////////////////////////////////////////////////
        TSyncLog::TSyncLog(const TSyncLogHeader &header,
                           TDiskRecLog &&diskRecLog,
                           ui64 logStartLsn,
                           const TMemoryConsumer &memBytes)
            : DiskRecLog(std::move(diskRecLog))
            , MemRecLog(DiskRecLog.AppendBlockSize, memBytes)
            , LogStartLsn(logStartLsn)
            , Header(header)
            , LastLsnOfIndexRecord(CalculateLastLsnOfIndexRecord())
        {}

        ui64 TSyncLog::CalculateLastLsnOfIndexRecord() const {
            // we call this function after reading entry point, so MemRecLog MUST be empty
            Y_ABORT_UNLESS(MemRecLog.Empty());

            if (DiskRecLog.Empty()) {
                return LogStartLsn > 0 ? LogStartLsn - 1 : 0;
            } else {
                return DiskRecLog.GetLastLsn();
            }
        }


        ////////////////////////////////////////////////////////////////////////////
        // TEntryPointSerializer - a class for serializing SyncLog entry point
        ////////////////////////////////////////////////////////////////////////////
        TEntryPointSerializer::TEntryPointSerializer(
                TSyncLogSnapshotPtr syncLogSnap,
                TVector<ui32> &&chunksToDeleteDelayed,
                ui64 recoveryLogConfirmedLsn)
            : RecoveryLogConfirmedLsn(recoveryLogConfirmedLsn)
            , SyncLogSnap(std::move(syncLogSnap))
            , ChunksToDeleteDelayed(std::move(chunksToDeleteDelayed))
        {}

        void TEntryPointSerializer::Serialize(const TDeltaToDiskRecLog &delta) {
            // fill in the protobuf
            NKikimrVDiskData::TSyncLogEntryPoint pb;
            pb.SetRecoveryLogConfirmedLsn(RecoveryLogConfirmedLsn);
            pb.MutableChunksToDeleteDelayed()->Reserve(ChunksToDeleteDelayed.size());
            for (const auto &x : ChunksToDeleteDelayed) {
                pb.AddChunksToDeleteDelayed(x);
            }
            const ui32 indexRecsNum = SyncLogSnap->SerializeToProto(pb, delta);
            // produce serialized data for the entry point
            SerializedData = Serialize(pb);

            // fill in EntryPointDbgInfo
            EntryPointDbgInfo = TEntryPointDbgInfo(SerializedData.size(),
                    ChunksToDeleteDelayed.size(),
                    indexRecsNum);
        }

        TString TEntryPointSerializer::Serialize(const NKikimrVDiskData::TSyncLogEntryPoint &pb) {
            // signature
            TStringStream s;
            s.Write(&TSyncLogHeader::SyncLogPbSignature, sizeof(ui32));
            // pb payload
            bool success = pb.SerializeToArcadiaStream(&s);
            Y_ABORT_UNLESS(success);
            return s.Str();
        }

        ////////////////////////////////////////////////////////////////////////////
        // TEntryPointParser - a class for parsing SyncLog entry point
        ////////////////////////////////////////////////////////////////////////////
        bool TEntryPointParser::Parse(const TString &serializedData, bool &needsInitialCommit, TString &explanation) {
            return ParseArray(serializedData.data(), serializedData.size(), needsInitialCommit, explanation);
        }

        bool TEntryPointParser::ParseArray(const char* serializedData, size_t size, bool &needsInitialCommit, TString &explanation) {
            NKikimrVDiskData::TSyncLogEntryPoint pb;
            bool success = ParseArrayToProto(pb, serializedData, size, needsInitialCommit, explanation);
            if (!success) {
                return false;
            }

            // disk rec log serialized data
            const char *diskRecLogStart = nullptr;
            const char *diskRecLogEnd = nullptr;
            if (!pb.GetDiskRecLogSerialized().empty()) {
                diskRecLogStart = pb.GetDiskRecLogSerialized().data();
                diskRecLogEnd = pb.GetDiskRecLogSerialized().data() + pb.GetDiskRecLogSerialized().size();
            }

            // create DiskRecLog
            TDiskRecLog diskRecLog(Params.ChunkSize,
                    Params.AppendBlockSize,
                    Params.IndexBulk,
                    diskRecLogStart,
                    diskRecLogEnd);

            TSyncLogHeader header(pb.GetPDiskGuid(), pb.GetVDiskIncarnationGuid());
            SyncLogPtr = new TSyncLog(header,
                    std::move(diskRecLog),
                    pb.GetLogStartLsn(),
                    Params.MemSizeBytes);

            RecoveryLogConfirmedLsn = pb.GetRecoveryLogConfirmedLsn();
            ui64 chunksToDeleteDelayedSize = pb.ChunksToDeleteDelayedSize();
            ChunksToDelete.reserve(chunksToDeleteDelayedSize);
            for (ui64 i = 0; i != chunksToDeleteDelayedSize; ++i) {
                ChunksToDelete.push_back(pb.GetChunksToDeleteDelayed(i));
            }

            return true;
        }

        bool TEntryPointParser::ParseToProto(
                NKikimrVDiskData::TSyncLogEntryPoint &pb,
                const TString &serializedData,
                bool &needsInitialCommit,
                TString &explanation)
        {
            return ParseArrayToProto(pb, serializedData.data(), serializedData.size(), needsInitialCommit, explanation);
        }

        bool TEntryPointParser::ParseArrayToProto(
                NKikimrVDiskData::TSyncLogEntryPoint &pb,
                const char* serializedData,
                size_t size,
                bool &needsInitialCommit,
                TString &explanation)
        {
            TStringStream err;
            if (size == 0) {
                // empty entry point
                needsInitialCommit = true;
                FillInEmptyEntryPoint(pb);
                return true;
            }

            if (size < sizeof(ui32)) {
                err << "Can't check signature because serialized data size is less than sizeof(ui32)";
                explanation = err.Str();
                return false;
            }

            // check signature
            const ui32 signature = *(const ui32*)serializedData;
            if (signature == TSyncLogHeader::SyncLogOldSignature) {
                // old format, convert to proto
                return ConvertOldFormatArrayToProto(pb, serializedData, size, explanation);
            } else if (signature == TSyncLogHeader::SyncLogPbSignature) {
                // new format -- protobuf
                bool success = pb.ParseFromArray(serializedData + sizeof(ui32),
                    size - sizeof(ui32));
                if (!success) {
                    err << "Failed to parse protobuf";
                    explanation = err.Str();
                }
                return success;
            } else {
                err << "Unknown signature: " << signature;
                explanation = err.Str();
                return false;
            }
        }

        bool TEntryPointParser::ConvertOldFormatToProto(
                NKikimrVDiskData::TSyncLogEntryPoint &pb,
                const TString &serializedData,
                TString &explanation)
        {
            return ConvertOldFormatArrayToProto(pb, serializedData.data(), serializedData.size(), explanation);
        }

        bool TEntryPointParser::ConvertOldFormatArrayToProto(
                NKikimrVDiskData::TSyncLogEntryPoint &pb,
                const char* serializedData,
                size_t size,
                TString &explanation)
        {
            Y_ABORT_UNLESS(size != 0);
            // Data Format
            // data ::= [TSyncLogHeader] [LogStartLsn=8b] ChunksToDelete DiskRecLogData
            // ChunksToDelete ::= [size=4b] [chunkIdx=4b]*
            // DiskRecLogData format is defined in TDiskRecLog

            const char *pos = serializedData;
            const char *end = serializedData + size;
            // create header
            if (size_t(end - pos) < TSyncLogHeader::HdrSize ||
                !TSyncLogHeader::CheckEntryPoint(pos, pos + TSyncLogHeader::HdrSize)) {
                explanation = "incorrect header";
                return false;
            }
            const TSyncLogHeader header = TSyncLogHeader::Constructor(pos, pos + TSyncLogHeader::HdrSize);
            pos += TSyncLogHeader::HdrSize;
            pb.SetPDiskGuid(header.PDiskGuid);
            pb.SetVDiskIncarnationGuid(header.VDiskIncarnationGuid.Guid);

            // parse LogStartLsn
            if (size_t(end - pos) < sizeof(ui64) + sizeof(ui32)) {
                explanation = "payload is too small";
                return false;
            }
            const ui64 logStartLsn = *(const ui64 *)pos;
            pos += sizeof(ui64);
            pb.SetLogStartLsn(logStartLsn);
            pb.SetRecoveryLogConfirmedLsn(0);

            // parse del chunks
            const ui32 delChunksSize = *(const ui32 *)pos;
            pos += sizeof(ui32);
            if (size_t(end - pos) < sizeof(ui32) * delChunksSize) {
                explanation = "deleted chunks size and actual records mismatch";
                return false;
            }
            TVector<ui32> chunksToDelete;
            chunksToDelete.reserve(delChunksSize);
            for (ui32 i = 0; i < delChunksSize; i++) {
                ui32 chunkIdx = *(const ui32 *)pos;
                pos += sizeof(ui32);
                chunksToDelete.push_back(chunkIdx);
            }
            pb.MutableChunksToDeleteDelayed()->Reserve(chunksToDelete.size());
            for (const auto &x : chunksToDelete) {
                pb.AddChunksToDeleteDelayed(x);
            }
            if (!NSyncLog::TDiskRecLog::CheckEntryPoint(pos, end)) {
                explanation = "error in TDiskRecLog serialized data";
                return false;
            }
            pb.SetDiskRecLogSerialized(TString(pos, end));
            return true;
        }

        void TEntryPointParser::FillInEmptyEntryPoint(NKikimrVDiskData::TSyncLogEntryPoint &pb) {
            pb.SetPDiskGuid(Params.PDiskGuid);
            TVDiskIncarnationGuid vdiskIncarnationGuid = TVDiskIncarnationGuid::Generate();
            pb.SetVDiskIncarnationGuid(vdiskIncarnationGuid.Guid);
            pb.SetLogStartLsn(0);
            pb.SetRecoveryLogConfirmedLsn(0);
        }

    } // NSyncLog
} // NKikimr
