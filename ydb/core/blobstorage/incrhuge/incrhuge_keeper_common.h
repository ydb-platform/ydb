#pragma once

#include "defs.h"
#include "incrhuge_id_dict.h"
#include "incrhuge_data.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

#include <util/generic/queue.h>
#include <util/generic/hash_set.h>

#define IHLOG_DEBUG(CTX, ...) LOG_DEBUG_S((CTX), NKikimrServices::BS_INCRHUGE, LogPrefix << Sprintf(__VA_ARGS__))
#define IHLOG_INFO(CTX, ...) LOG_INFO_S((CTX), NKikimrServices::BS_INCRHUGE, LogPrefix << Sprintf(__VA_ARGS__))

namespace NKikimr {
    namespace NIncrHuge {

        class TKeeper;
        struct TBlobIndexHeader;
        struct TBlobIndexRecord;

        // initial incremental huge keeper settings
        struct TKeeperSettings {
            ui32                PDiskId;            // PDisk id
            TActorId            PDiskActorId;       // PDisk actor id
            ui64                PDiskGuid;          // PDisk GUID
            ui32                MinHugeBlobInBytes; // minimal blob size we will receive
            ui32                MinCleanChunks;     // minimal number of clean chunks we will hold
            ui32                MinAllocationBatch; // minimal number of chunks we will request for
            ui32                UnalignedBlockSize; // preferred block size; will rounded up to AppendBlockSize to get BlockSize
            ui32                MaxInFlightWrites;  // maximal number of in flight blob writes
            NPDisk::TOwnerRound InitOwnerRound;     // initial owner round
        };

        enum class EDiskState {
            SpaceRed,    // we can't do anything
            SpaceOrange, // we don't accept new writes
            SpaceYellow, // we enforce defragmentation
            SpaceGreen,  // we work as usual
        };

        enum class EChunkState {
            Complete,    // chunk with data and index written
            Finalizing,  // index write in progress, data written
            Current,     // writing data
            WriteIntent, // going to write data in not so distant future
            Deleting,    // chunk is going to be deleted
            Unknown,     // going to determine real state
        };

        inline const char *ChunkStateToString(EChunkState state) {
            switch (state) {
                case EChunkState::Complete:    return "Complete";
                case EChunkState::Finalizing:  return "Finalizing";
                case EChunkState::Current:     return "Current";
                case EChunkState::WriteIntent: return "WriteIntent";
                case EChunkState::Deleting:    return "Deleting";
                case EChunkState::Unknown:     return "Unknown";
            }
            Y_ABORT("unexpected case");
        }

        // per chunk information structure
        struct TChunkInfo {
            EChunkState  State;         // chunk state
            TChunkSerNum ChunkSerNum;   // unique serial number of this chunk
            ui32         NumUsedBlocks; // number of used blocks
            ui32         NumItems;      // index size of this chunk
            TDynBitMap   DeletedItems;  // bitmap of deleted items; size = MaxBlobsPerChunk
            ui32         InFlightReq;   // number of in-flight requests to this chunk
        };

        // blob locator structure
        struct TBlobLocator {
            TChunkIdx ChunkIdx;         // chunk index
            ui32 OffsetInBlocks;        // offset inside chunk in blocks
            ui32 PayloadSize;           // size of data payload in bytes
            ui32 IndexInsideChunk : 23; // ordinal number of blob inside containing chunk
            ui32 Owner : 8;             // blob owner
            ui32 DeleteInProgress : 1;  // this blob is being deleted

            std::tuple<TChunkIdx, ui32, ui32, ui32, ui8> AsTuple() const {
                return std::make_tuple(ChunkIdx, OffsetInBlocks, PayloadSize, IndexInsideChunk, Owner);
            }

            friend bool operator ==(const TBlobLocator& left, const TBlobLocator& right) {
                return left.AsTuple() == right.AsTuple();
            }

            friend bool operator <(const TBlobLocator& left, const TBlobLocator& right) {
                return left.AsTuple() < right.AsTuple();
            }

            TString ToString() const {
                return Sprintf("{ChunkIdx# %" PRIu32 " OffsetInBlocks# %" PRIu32 " PayloadSize# %" PRIu32
                        " IndexInsideChunk# %" PRIu32 " Owner# %" PRIu32 " DeleteInProgress# %" PRIu32 "}", ChunkIdx,
                        OffsetInBlocks, PayloadSize, IndexInsideChunk, Owner, DeleteInProgress);
            }
        };

        // blob delete locator structure
        struct TBlobDeleteLocator {
            TChunkIdx ChunkIdx;
            TChunkSerNum ChunkSerNum;
            TIncrHugeBlobId Id;
            ui32 IndexInsideChunk;
            ui32 SizeInBlocks;

            std::tuple<TChunkIdx, TChunkSerNum, TIncrHugeBlobId, ui32, ui32> AsTuple() const {
                return std::make_tuple(ChunkIdx, ChunkSerNum, Id, IndexInsideChunk, SizeInBlocks);
            }

            friend bool operator ==(const TBlobDeleteLocator& left, const TBlobDeleteLocator& right) {
                return left.AsTuple() == right.AsTuple();
            }

            friend bool operator <(const TBlobDeleteLocator& left, const TBlobDeleteLocator& right) {
                return left.AsTuple() < right.AsTuple();
            }

            TString ToString() const {
                return Sprintf("{ChunkIdx# %" PRIu32 " ChunkSerNum# %s Id# %016" PRIx64 " IndexInsideChunk# %"
                    PRIu32 " SizeInBlocks# %" PRIu32 "}", ChunkIdx, ChunkSerNum.ToString().data(), Id, IndexInsideChunk,
                    SizeInBlocks);
            }
        };

        struct TKeeperCommonState : public TThrRefBase {
            // initial keeper settings
            const TKeeperSettings Settings;

            // pdisk parameters acquired after yard init
            TIntrusivePtr<TPDiskParams> PDiskParams;

            ////////////////////////////
            // Operational parameters //
            ////////////////////////////

            ui32 BlockSize = 0;            // block size in bytes
            ui32 BlocksInChunk = 0;        // chunk size in blocks
            ui32 MaxBlobsPerChunk = 0;     // maximal number of blobs in one chunk
            ui32 BlocksInDataSection = 0;  // data section size in blocks
            ui32 BlocksInIndexSection = 0; // index section size in blocks
            ui32 BlocksInMinBlob = 0;      // number of blocks in minimal blob

            // current id of this state; recovered from the last chunk log records and incremented for each allocated
            // chunk
            TChunkSerNum CurrentSerNum{1000};

            // disk space state
            EDiskState DiskState = EDiskState::SpaceGreen;

            ///////////////////////
            // Blob location map //
            ///////////////////////

            TIdLookupTable<TBlobLocator, ui16, 4096> BlobLookup;

            ///////////////
            // Chunk map //
            ///////////////

            // list of all chunks (including current one)
            THashMap<TChunkIdx, TChunkInfo> Chunks;

            /////////////////////////
            // Writer shared state //
            /////////////////////////

            // the chunk index writer is currently writing to; if zero then there is no active chunk
            TChunkIdx CurrentChunk = 0;

            // write intent queue; contains a list of chunks we are going to fill in the near future; these all chunks
            // are confirmed, that is they have correspoding TCommitRecord successfully logged; each entry has
            // corresponding item in Chunks
            TQueue<TChunkIdx> WriteIntentQueue;

            // is blob keeper ready?
            bool Ready = false;

            // number of in-flight writes
            ui32 InFlightWrites = 0;

            // set of defragmenter items being written right now
            THashMap<TIncrHugeBlobId, bool> DefragWriteInProgress;

            // set of spawned children actors
            THashSet<TActorId> ChildActors;

            TKeeperCommonState(const TKeeperSettings& settings)
                : Settings(settings)
            {}

            // calculate number of blocks occupied by a single blob
            ui32 GetBlobSizeInBlocks(ui32 payloadSize) const {
                return (sizeof(TBlobHeader) + payloadSize + BlockSize - 1) / BlockSize;
            }
        };

        // event callback base; it is a class that holds some data and has an apply function which is called when
        // log entry is written or write is failed or for other event; uses NALF incremental allocator, because it
        // is short-term object
        class IEventCallback {
        public:
            virtual ~IEventCallback() = default;
            virtual void Apply(NKikimrProto::EReplyStatus status, IEventBase *result, const TActorContext& ctx) = 0;
        };

        // lambda callback wrapper, for convenience
        template<typename TFunctor>
        class TLambdaEventCallback : public IEventCallback {
            TFunctor Functor;

        public:
            TLambdaEventCallback(TFunctor&& functor)
                : Functor(std::move(functor))
            {}

            void Apply(NKikimrProto::EReplyStatus status, IEventBase *result, const TActorContext& ctx) override {
                Functor(status, result, ctx);
            }
        };

        // simple lambda callback wrapper -- for messages not using result
        template<typename TFunctor>
        class TSimpleLambdaEventCallback : public IEventCallback {
            TFunctor Functor;

        public:
            TSimpleLambdaEventCallback(TFunctor&& functor)
                : Functor(std::move(functor))
            {}

            void Apply(NKikimrProto::EReplyStatus status, IEventBase* /*result*/, const TActorContext& ctx) override {
                Functor(status, ctx);
            }
        };

        template<typename TFunctor>
        inline std::unique_ptr<IEventCallback> MakeCallback(TFunctor&& functor) {
            return std::make_unique<TLambdaEventCallback<TFunctor>>(std::move(functor));
        }

        template<typename TFunctor>
        inline std::unique_ptr<IEventCallback> MakeSimpleCallback(TFunctor&& functor) {
            return std::make_unique<TSimpleLambdaEventCallback<TFunctor>>(std::move(functor));
        }

        // callback demultiplexer
        class TMuxCallback : public IEventCallback {
            TVector<std::unique_ptr<IEventCallback>> Callbacks;

        public:
            TMuxCallback(TVector<std::unique_ptr<IEventCallback>>&& callbacks)
                : Callbacks(std::move(callbacks))
            {}

            void Apply(NKikimrProto::EReplyStatus status, IEventBase *result, const TActorContext& ctx) override {
                for (auto& callback : Callbacks) {
                    callback->Apply(status, result, ctx);
                }
            }
        };

        class TKeeperComponentBase {
        protected:
            // keeper itself
            TKeeper& Keeper;

            // log prefix
            TString LogPrefix;

        public:
            TKeeperComponentBase(TKeeper& keeper, const char *name);
        };

        // callback event; handled by keeper by invoking this callback :-)
        struct TEvIncrHugeCallback : public TEventLocal<TEvIncrHugeCallback, TEvBlobStorage::EvIncrHugeCallback> {
            std::unique_ptr<IEventCallback> Callback;

            TEvIncrHugeCallback(std::unique_ptr<IEventCallback>&& callback)
                : Callback(std::move(callback))
            {}
        };

        struct TEvIncrHugeReadLogResult
            : public TEventLocal<TEvIncrHugeReadLogResult, TEvBlobStorage::EvIncrHugeReadLogResult>
        {
            NKikimrProto::EReplyStatus Status;
            NKikimrVDiskData::TIncrHugeChunks Chunks;
            NKikimrVDiskData::TIncrHugeDelete Deletes;
            ui64 NextLsn;
            bool IssueInitialStartingPoints;
        };

        struct TEvIncrHugeScanResult
            : public TEventLocal<TEvIncrHugeScanResult, TEvBlobStorage::EvIncrHugeScanResult>
        {
            NKikimrProto::EReplyStatus Status;
            TVector<TBlobIndexRecord> Index;
            TChunkIdx ChunkIdx;
            bool IndexOnly;
            TChunkSerNum ChunkSerNum;
            bool IndexValid;
        };

    } // NIncrHuge
} // NKikimr
