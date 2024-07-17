#pragma once

#include "defs.h"
#include "blobstorage_synclogmem.h"
#include "blobstorage_synclogdsk.h"
#include "blobstorage_synclogneighbors.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>
#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/deque.h>
#include <util/generic/queue.h>
#include <util/generic/algorithm.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TEntryPointDbgInfo
        // Information about what entry point is made from (for info/debug purposes
        // mainly)
        ////////////////////////////////////////////////////////////////////////////
        struct TEntryPointDbgInfo {
            ui32 ByteSize = 0;
            ui32 ChunkToDeleteNum = 0;
            ui32 IndexRecsNum = 0;

            TEntryPointDbgInfo() = default;
            TEntryPointDbgInfo(ui32 byteSize, ui32 chunksToDel, ui32 idxRecsNum)
                : ByteSize(byteSize)
                , ChunkToDeleteNum(chunksToDel)
                , IndexRecsNum(idxRecsNum)
            {}
            void Output(IOutputStream &s) const;
            TString ToString() const;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogHeader
        ////////////////////////////////////////////////////////////////////////////
        struct TSyncLogHeader {
            // Data Format
            // data ::= [signature=4b] [reserved=4b]
            //          [PDiskGuid=8b] [VDiskIncarnationGuid=8b]

            const ui64 PDiskGuid;
            const TVDiskIncarnationGuid VDiskIncarnationGuid;

            static constexpr ui32 SyncLogOldSignature = 0x6a8cf35c;
            static constexpr ui32 SyncLogPbSignature = 0xf5247a90;
            static constexpr size_t HdrSize = sizeof(ui32) + sizeof(ui32) +
            sizeof(ui64) + sizeof(TVDiskIncarnationGuid);

            TSyncLogHeader(ui64 pdiskGuid, TVDiskIncarnationGuid vdiskGuid)
                : PDiskGuid(pdiskGuid)
                , VDiskIncarnationGuid(vdiskGuid)
            {}

            static bool CheckEntryPoint(const char *pos, const char *end);
            static TSyncLogHeader Constructor(const char *pos, const char *end);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TLogEssence
        ////////////////////////////////////////////////////////////////////////////
        struct TLogEssence {
            ui64 LogStartLsn = 0;
            bool MemLogEmpty = true;
            bool DiskLogEmpty = true;
            ui64 FirstMemLsn = 0;
            ui64 LastMemLsn = 0;
            ui64 FirstDiskLsn = 0;
            ui64 LastDiskLsn = 0;

            TLogEssence();
            TLogEssence(ui64 logStartLsn,
                    bool memLogEmpty,
                    bool diskLogEmpty,
                    ui64 firstMemLsn,
                    ui64 lastMemLsn,
                    ui64 firstDiskLsn,
                    ui64 lastDiskLsn);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogSnapshot
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogSnapshot : public TThrRefBase {
        public:
            void FillInLogEssence(TLogEssence *e) const;
            TString BoundariesToString() const;
            void CheckSnapshotConsistency() const;
            // returns number of index records
            ui32 SerializeToProto(NKikimrVDiskData::TSyncLogEntryPoint &pb, const TDeltaToDiskRecLog &delta);

        public:
            TDiskRecLogSnapshotConstPtr DiskSnapPtr;
            TMemRecLogSnapshotConstPtr MemSnapPtr;
            const ui64 LogStartLsn;
            const ui32 AppendBlockSize;
            const TEntryPointDbgInfo LastEntryPointDbgInfo;
            const TSyncLogHeader Header;

        private:
            TSyncLogSnapshot(TDiskRecLogSnapshotPtr diskSnapPtr,
                             TMemRecLogSnapshotPtr memSnapPtr,
                             ui64 logStartLsn,
                             ui32 appendBlockSize,
                             const TEntryPointDbgInfo &lastEntryPointDbgInfo,
                             const TSyncLogHeader &header);

            friend class TSyncLog;
        };

        using TSyncLogSnapshotPtr = TIntrusivePtr<TSyncLogSnapshot>;


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogParams
        // We need them to initialize SyncLog
        ////////////////////////////////////////////////////////////////////////////
        struct TSyncLogParams {
            // pdisk guid provided
            ui64 PDiskGuid;
            // size of the disk chunk
            ui64 ChunkSize;
            // size of the memory page
            ui32 AppendBlockSize;
            // we have one index record for IndexBulk number of pages written to disk
            ui32 IndexBulk;
            // memory bytes we use to store SyncLog data
            TMemoryConsumer MemSizeBytes;

            TSyncLogParams(ui64 pDiskGuid,
                           ui64 chunkSize,
                           ui32 appendBlockSize,
                           ui32 syncLogAdvisedIndexedBlockSize,
                           const TMemoryConsumer &memSizeBytes);
            static ui32 CalculateIndexBulk(ui32 syncLogAdvisedIndexedBlockSize, ui32 appendBlockSize, ui32 chunkSize);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLog
        ////////////////////////////////////////////////////////////////////////////
        struct TSyncLogRepaired;
        struct TDeltaToDiskRecLog;
        class TSyncLog;
        using TSyncLogPtr = TIntrusivePtr<TSyncLog>;

        class TSyncLog : public TThrRefBase {
        public:
            ////////////////////////////////////////////////////////////////////////
            // Serialize/Parse
            ////////////////////////////////////////////////////////////////////////
            friend class TEntryPointParser;


            ////////////////////////////////////////////////////////////////////////
            // Getters/Checks
            ////////////////////////////////////////////////////////////////////////
            // Just lsn of the last record in index or (LogStartLsn - 1)
            ui64 GetLastLsn() const;
            // First lsn to keep in Recovery Log (it doesn't care about entry point lsn,
            // because SyncLog knows nothing about its entry point; ask SyncLogKeeper
            // about it)
            ui64 FirstLsnToKeep() const;
            // Verbose FirstLsnToKeep
            TString FirstLsnToKeepDecomposed() const;
            // just a reference to last entry point dbg info
            const TEntryPointDbgInfo &GetLastEntryPointDbgInfo() const;
            TString BoundariesToString() const;
            // create snaphsot of SyncLog (can be worked with asynchronously)
            TSyncLogSnapshotPtr GetSnapshot() const;
            // check that MemRecLog and DiskRecLog don't intersect by lsns,
            // it must be true after local recovery
            bool CheckMemAndDiskRecLogsDoNotIntersect() const;
            // fill data about synclog
            void FillInLogEssence(TLogEssence *e) const;


            ////////////////////////////////////////////////////////////////////////
            // Puts/Trims
            ////////////////////////////////////////////////////////////////////////
            // puts into SyncLog (actually they to MemRecLog)
            void PutOne(const TRecordHdr *rec, ui32 size);
            void PutMany(const void *buf, ui32 size);
            // trim log, i.e. remove pages/chunks we don't need because
            // peer vdisks synced to confirmedCutLsn; the function returns
            // chunks to delete (i.e. free)
            TVector<ui32> TrimLogByConfirmedLsn(
                ui64 confirmedCutLsn,
                std::shared_ptr<IActorNotify> notifier,
                std::function<void(const TString&)> logger);
            // trim log by removing chunks over some quota, i.e. we got
            // too many chunks allocated for SyncLog and want to remove numChunksToDel;
            // the function returns chunks to delete (i.e. free)
            TVector<ui32> TrimLogByRemovingChunks(ui32 numChunksToDel, std::shared_ptr<IActorNotify> notifier);


            ////////////////////////////////////////////////////////////////////////
            // Funcs about MemRecLog
            ////////////////////////////////////////////////////////////////////////
            // number of pages in Memory (i.e. MemRecLog)
            ui32 GetNumberOfPagesInMemory() const;
            // returns page size
            ui32 GetAppendBlockSize() const;
            // remove pages from mem which are already dumped to disk
            // to free up some memory
            ui32 RemoveCachedPages(ui32 pagesMax, ui64 diskLastLsn);
            // build snapshot of memory pages to write to disk to free up
            // some memory
            TMemRecLogSnapshotPtr BuildMemSwapSnapshot(ui64 diskLastLsn,
                                                       ui64 freeUpToLsn, // excluding
                                                       ui32 freeNPages);


            ////////////////////////////////////////////////////////////////////////
            // Funcs about DiskRecLog
            ////////////////////////////////////////////////////////////////////////
            // calculate how many chunks adds this memory snapshot to current state
            // if we write it to disk
            ui32 HowManyChunksAdds(const TMemRecLogSnapshotPtr &swapSnap) const;
            // returns how many chunks we use
            ui32 GetSizeInChunks() const;
            // returns lsn of last record stored in DiskRecLog or 0 if it's empty
            ui64 GetDiskLastLsn() const;
            // update index of DiskRecLog
            void UpdateDiskIndex(const TDeltaToDiskRecLog &delta,
                                 const TEntryPointDbgInfo &dbg);
            // size of the chunk
            ui32 GetChunkSize() const;
            // returns chunks owned by DiskRecLog
            void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;

        private:
            // part of the log on disk
            TDiskRecLog DiskRecLog;
            // part of the log on disk
            TMemRecLog MemRecLog;
            // the log has information started from this Lsn, all previous records
            // were removed
            ui64 LogStartLsn;
            // info about last serialized data written to the log as an entry point
            TEntryPointDbgInfo LastEntryPointDbgInfo;

            TSyncLog(const TSyncLogHeader &header,
                     TDiskRecLog &&diskRecLog,
                     ui64 logStartLsn,
                     const TMemoryConsumer &memBytes);
            ui64 CalculateLastLsnOfIndexRecord() const;

        public:
            // constant header of SyncLog
            const TSyncLogHeader Header;
            // after reading entry point of SyncLog we calculate lsn of the last
            // record stored in SyncLog data structures (i.e. DiskRecLog),
            // i.e. not in recovery log.
            // NOTES (WHATS FOR): after recovery we need to guarantee that new
            // SyncLog pages that we have in memory do not overlap with SyncLog
            // pages stored on disk. This is requirement of the algorithm that swaps
            // memory SyncLog pages to disk. The field below is used to guarantee this,
            // we never put records from recovery log to memory if we have already
            // swaped these records to disk
            const ui64 LastLsnOfIndexRecord;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TEntryPointSerializer - a class for serializing SyncLog entry point
        ////////////////////////////////////////////////////////////////////////////
        class TEntryPointSerializer {
        public:
            TEntryPointSerializer(TSyncLogSnapshotPtr syncLogSnap,
                TVector<ui32> &&chunksToDeleteDelayed,
                ui64 recoveryLogConfirmedLsn);

            void Serialize(const TDeltaToDiskRecLog &delta);
            TString GetSerializedData() const { return SerializedData; }
            TEntryPointDbgInfo GetEntryPointDbgInfo() const { return EntryPointDbgInfo; }

        public:
            const ui64 RecoveryLogConfirmedLsn;
        private:
            TSyncLogSnapshotPtr SyncLogSnap;
            TVector<ui32> ChunksToDeleteDelayed;
            TString SerializedData;
            TEntryPointDbgInfo EntryPointDbgInfo;

            static TString Serialize(const NKikimrVDiskData::TSyncLogEntryPoint &pb);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TEntryPointParser - a class for parsing SyncLog entry point
        ////////////////////////////////////////////////////////////////////////////
        class TEntryPointParser {
        public:
            TEntryPointParser(TSyncLogParams &&params)
                : Params(std::move(params))
            {}

            bool Parse(const TString &serializedData, bool &needsInitialCommit, TString &explanation);
            bool ParseArray(const char* serializedData, size_t size, bool &needsInitialCommit, TString &explanation);
            TSyncLogPtr GetSyncLogPtr() const { return SyncLogPtr; }
            TVector<ui32> GetChunksToDelete() const { return ChunksToDelete; }
            ui64 GetRecoveryLogConfirmedLsn() const { return RecoveryLogConfirmedLsn; }

        private:
            const TSyncLogParams Params;
            TSyncLogPtr SyncLogPtr;
            TVector<ui32> ChunksToDelete;
            ui64 RecoveryLogConfirmedLsn = 0;

            bool ParseToProto(
                    NKikimrVDiskData::TSyncLogEntryPoint &pb,
                    const TString &serializedData,
                    bool &needsInitialCommit,
                    TString &explanation);

            bool ParseArrayToProto(
                    NKikimrVDiskData::TSyncLogEntryPoint &pb,
                    const char* serializedData,
                    size_t size,
                    bool &needsInitialCommit,
                    TString &explanation);

            bool ConvertOldFormatToProto(
                    NKikimrVDiskData::TSyncLogEntryPoint &pb,
                    const TString &serializedData,
                    TString &explanation);

            bool ConvertOldFormatArrayToProto(
                    NKikimrVDiskData::TSyncLogEntryPoint &pb,
                    const char* serializedData,
                    size_t size,
                    TString &explanation);

            void FillInEmptyEntryPoint(NKikimrVDiskData::TSyncLogEntryPoint &pb);
        };

    } // NSyncLog
} // NKikimr
