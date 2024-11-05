#pragma once

#include "blobstorage_hulldatamerger.h"
#include "hullds_sst.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/vdisk/common/align.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <util/generic/queue.h>

namespace NKikimr {

    class TBufferedChunkWriter;

    enum class EWriterDataType {
        Fresh,
        Comp,
        Replication
    };

    inline const TMemoryConsumer& WriterDataTypeToMemConsumer(const TVDiskContextPtr& vctx, EWriterDataType type, bool data) {
        switch (type) {
            case EWriterDataType::Fresh:       return data ? vctx->CompDataFresh : vctx->CompIndexFresh;
            case EWriterDataType::Comp:        return data ? vctx->CompData : vctx->CompIndex;
            case EWriterDataType::Replication: return vctx->Replication;
            default:                           Y_ABORT("incorrect EWriterDataType provided");
        }
    }

    inline ui8 EWriterDataTypeToPriority(EWriterDataType t) {
        switch (t) {
            case EWriterDataType::Fresh:        return NPriWrite::HullFresh;
            case EWriterDataType::Comp:         return NPriWrite::HullComp;
            case EWriterDataType::Replication:  return NPriWrite::HullComp; // FIXME: add HullRepl priority class
            default:                            Y_ABORT("incorrect EWriterDataType provided");
        }
    }

    class TBufferedChunkWriter : public TThrRefBase {
    public:
        TBufferedChunkWriter(TMemoryConsumer&& consumer, ui8 owner, ui64 ownerRound, ui8 priority, ui32 chunkSize,
                             ui32 appendBlockSize, ui32 writeBlockSize, ui32 chunkIdx,
                             TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue)
            : Consumer(std::move(consumer))
            , Owner(owner)
            , OwnerRound(ownerRound)
            , Priority(priority)
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , WriteBlockSize(writeBlockSize - writeBlockSize % appendBlockSize)
            , ChunkIdx(chunkIdx)
            , Offset(0)
            , Buffer(TMemoryConsumer(Consumer))
            , MsgQueue(msgQueue)
            , DiskPartOffset(0)
            , Finished(false)
            , HasBuffer(false)
        {
            Y_ABORT_UNLESS(chunkIdx != 0);
        }

        void Push(const void *data, size_t len) {
            Y_ABORT_UNLESS(Offset + len <= ChunkSize && !Finished);

            while (len) {
                if (!HasBuffer) {
                    TTrackableBuffer newBuffer(TMemoryConsumer(Consumer), WriteBlockSize);
                    Buffer.Swap(newBuffer);
                    HasBuffer = true;
                }

                size_t bytes = Min(len, WriteBlockSize - Buffer.Size());
                const char *ptr = static_cast<const char *>(data);
                Buffer.Append(ptr, bytes);
                data = ptr + bytes;
                len -= bytes;
                Offset += bytes;

                if (Buffer.Size() == WriteBlockSize || Offset == ChunkSize) {
                   CreateChunkWriteMsg();
                }
            }
        }

        void FinishChunk() {
            Y_ABORT_UNLESS(!Finished);
            Finished = true;

            if (Offset == ChunkSize)
                return;

            size_t sizeBeforeAlign = Buffer.Size();
            AlignUpAppendBlockSize(Buffer, AppendBlockSize);
            Offset += Buffer.Size() - sizeBeforeAlign;

            CreateChunkWriteMsg();
        }

        ui32 GetFreeSpace() const {
            return ChunkSize - Offset;
        }

        ui32 GetChunkIdx() const {
            return ChunkIdx;
        }

        void SetBookmark() {
            DiskPartOffset = Offset;
        }

        TDiskPart GetDiskPartForBookmark() const {
            return {ChunkIdx, DiskPartOffset, Offset - DiskPartOffset};
        }

        ui32 GetBlockSize() const {
            return WriteBlockSize;
        }

    private:
        void CreateChunkWriteMsg() {
            if (Buffer.Size()) {
                ui32 offsetInChunk = Offset - Buffer.Size();
                Y_ABORT_UNLESS(offsetInChunk % AppendBlockSize == 0);
                Y_ABORT_UNLESS(ChunkIdx);
                NPDisk::TEvChunkWrite::TPartsPtr parts(new NPDisk::TEvChunkWrite::TBufBackedUpParts(std::move(Buffer)));
                auto ev = std::make_unique<NPDisk::TEvChunkWrite>(Owner, OwnerRound, ChunkIdx, offsetInChunk, parts, nullptr, true, Priority);
                MsgQueue.push(std::move(ev));
                HasBuffer = false;
            }
        }

    private:
        TMemoryConsumer Consumer;
        const ui8 Owner;
        const ui64 OwnerRound;
        const ui8 Priority;
        const ui32 ChunkSize;
        const ui32 AppendBlockSize;
        const ui32 WriteBlockSize;
        ui32 ChunkIdx;
        ui32 Offset;
        TTrackableBuffer Buffer;
        TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& MsgQueue;
        ui32 DiskPartOffset;
        bool Finished;
        bool HasBuffer;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment<TKey, TMemRec>::TBaseWriter
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // FIXME: don't load index after compaction
    template <class TKey, class TMemRec>
    class TLevelSegment<TKey, TMemRec>::TBaseWriter {
    public:
        TBaseWriter(TMemoryConsumer&& consumer, ui8 owner, ui64 ownerRound, ui8 priority, ui32 chunkSize,
                    ui32 appendBlockSize, ui32 writeBlockSize, TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue,
                    TDeque<TChunkIdx>& rchunks)
            : Consumer(std::move(consumer))
            , Owner(owner)
            , OwnerRound(ownerRound)
            , Priority(priority)
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , WriteBlockSize(writeBlockSize)
            , MsgQueue(msgQueue)
            , RChunks(rchunks)
            , UsedChunks()
        {}

        const TVector<ui32> &GetUsedChunks() const {
            return UsedChunks;
        }

        template<typename TChunkGenerator>
        TMaybe<TDiskPart> AppendAlignedImpl(TChunkGenerator&& generator, size_t len) {
            if (!ChunkWriter) {
                Y_ABORT_UNLESS(!RChunks.empty());
                const TChunkIdx chunkIdx = RChunks.front();
                RChunks.pop_front();

                ChunkWriter = std::make_unique<TBufferedChunkWriter>(TMemoryConsumer(Consumer), Owner, OwnerRound,
                    Priority, ChunkSize, AppendBlockSize, WriteBlockSize, chunkIdx, MsgQueue);

                UsedChunks.push_back(chunkIdx);
            }

            ui32 alignedLen = AlignUp<ui32>(len, 4);
            if (ChunkWriter->GetFreeSpace() < alignedLen)
                return TMaybe<TDiskPart>();

            ChunkWriter->SetBookmark();
            while (TMaybe<std::pair<const char*, size_t>> block = generator()) {
                ChunkWriter->Push(block->first, block->second);
            }
            TDiskPart result = ChunkWriter->GetDiskPartForBookmark();
            static const char padding[3] = {0, 0, 0};
            Y_DEBUG_ABORT_UNLESS(alignedLen - len <= 3);
            ChunkWriter->Push(padding, alignedLen - len);

            return result;
        }

        TMaybe<TDiskPart> AppendAligned(const char *buffer, size_t len) {
            struct TGenerator {
                const char *Buffer;
                size_t Len;

                TMaybe<std::pair<const char*, size_t>> operator ()() {
                    if (Buffer) {
                        auto res = std::make_pair(Buffer, Len);
                        Buffer = nullptr;
                        return res;
                    } else {
                        return {};
                    }
                }
            } generator{buffer, len};
            return AppendAlignedImpl(generator, len);
        }

        TMaybe<TDiskPart> AppendAligned(const TRope& data) {
            struct TGenerator {
                TRope::TConstIterator Iter;

                TMaybe<std::pair<const char*, size_t>> operator ()() {
                    if (Iter.Valid()) {
                        auto res = std::make_pair(Iter.ContiguousData(), Iter.ContiguousSize());
                        Iter.AdvanceToNextContiguousBlock();
                        return res;
                    } else {
                        return {};
                    }
                }
            } generator{data.Begin()};
            return AppendAlignedImpl(generator, data.GetSize());
        }

        ui32 ChunkOffset() const {
            return ChunkSize - (ChunkWriter ? ChunkWriter->GetFreeSpace() : 0);
        }

        void FinishChunk() {
            ChunkWriter->FinishChunk();
            ChunkWriter.reset();
        }

    protected:
        TMemoryConsumer Consumer;
        const ui8 Owner;
        const ui64 OwnerRound;
        const ui8 Priority;
        const ui32 ChunkSize;
        const ui32 AppendBlockSize;
        const ui32 WriteBlockSize;
        TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& MsgQueue;
        TDeque<ui32>& RChunks;
        TVector<ui32> UsedChunks;
        std::unique_ptr<TBufferedChunkWriter> ChunkWriter;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TDataWriterConclusion
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TDataWriterConclusion {
        std::unique_ptr<TBufferedChunkWriter> Writer;
        TVector<ui32> UsedChunks;

        TDataWriterConclusion(std::unique_ptr<TBufferedChunkWriter>&& writer, TVector<ui32>&& usedChunks)
            : Writer(std::move(writer))
            , UsedChunks(std::move(usedChunks))
        {}
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TIndexWriterConclusion
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    template<typename TKey, typename TMemRec>
    struct TIndexWriterConclusion {
        TVector<ui32> UsedChunks;
        TDiskPart Addr; // entry point
        ui32 IndexParts = 0;
        ui32 OutboundItems = 0;
        TIntrusivePtr<TLevelSegment<TKey, TMemRec>> LevelSegment;

        TString ToString() const {
            TStringStream s;
            s << "{Addr: " << Addr.ToString() << " IndexParts: " << IndexParts;
            if (OutboundItems)
                s << " OutboundItems: " << OutboundItems;
            s << " {UsedChunks:";
            for (auto c : UsedChunks)
                s << " " << c;
            s << "}}";
            return s.Str();
        }
    };


    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment<TKey, TMemRec>::TDataWriter
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSegment<TKey, TMemRec>::TDataWriter : public TLevelSegment<TKey, TMemRec>::TBaseWriter {
        typedef typename ::NKikimr::TLevelSegment<TKey, TMemRec>::TBaseWriter TBase;

        using TBase::Owner;
        using TBase::Priority;
        using TBase::ChunkSize;
        using TBase::AppendBlockSize;
        using TBase::RChunks;
        using TBase::UsedChunks;

    public:
        TDataWriter(TVDiskContextPtr vctx, EWriterDataType type, ui8 owner, ui64 ownerRound, ui32 chunkSize,
                    ui32 appendBlockSize, ui32 writeBlockSize, TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue,
                    TDeque<TChunkIdx>& rchunks)
            : TBase(TMemoryConsumer(WriterDataTypeToMemConsumer(vctx, type, true)),
                    owner,
                    ownerRound,
                    EWriterDataTypeToPriority(type),
                    chunkSize,
                    appendBlockSize,
                    writeBlockSize,
                    msgQueue,
                    rchunks)
            , Finished(false)
            , RChunksIndex(0)
            , Offset(0)
        {}

        TDiskPart Preallocate(ui32 size) {
            const ui32 alignedSize = AlignUp(size, 4U);
            if (Offset + alignedSize > ChunkSize) {
                Offset = 0;
                ++RChunksIndex;
                Y_ABORT_UNLESS(RChunksIndex < UsedChunks.size() + RChunks.size());
            }

            const TChunkIdx chunkIdx = RChunksIndex < UsedChunks.size() ? UsedChunks[RChunksIndex]
                : RChunks[RChunksIndex - UsedChunks.size()];
            TDiskPart location(chunkIdx, Offset, size);
            Offset += alignedSize;
            return location;
        }

        TDiskPart Push(const TRope& buffer) {
            Y_DEBUG_ABORT_UNLESS(!Finished);

            TMaybe<TDiskPart> result = TBase::AppendAligned(buffer);
            if (!result) {
                // there is no space to fit in current chunk -- restart base writer with new chunk
                TBase::FinishChunk();
                result = TBase::AppendAligned(buffer);
                Y_ABORT_UNLESS(result);
            }

            return *result;
        }

        TDataWriterConclusion Finish() {
            Y_ABORT_UNLESS(!Finished);
            const ui32 alignedOffset = AlignUpAppendBlockSize(Offset, AppendBlockSize);
            if (const size_t num = alignedOffset - Offset) {
                char *buffer = (char*)alloca(num);
                memset(buffer, 0, num);
                const TMaybe<TDiskPart> part = TBase::AppendAligned(buffer, num);
                Y_ABORT_UNLESS(part);
                Y_ABORT_UNLESS((part->Offset + part->Size) % AppendBlockSize == 0);
            }
            Finished = true;
            return {std::move(TBase::ChunkWriter), std::move(UsedChunks)};
        }

        bool IsFinished() const {
            return Finished;
        }

        void GetUsageAfterPush(ui32 size, ui32 *chunks, ui32 *intermSize) const {
            *chunks = RChunksIndex + (Offset ? 1 : 0);
            *intermSize = Offset ? Offset : ChunkSize;

            if (const ui32 alignedSize = AlignUp(size, 4U)) {
                *intermSize += alignedSize;
                if (*intermSize > ChunkSize) {
                    // if we'd start a new chunk
                    ++*chunks;
                    *intermSize = alignedSize;
                }
            }

            *intermSize = AlignUpAppendBlockSize(*intermSize, AppendBlockSize);
        }

        using TBase::GetUsedChunks;

    private:
        bool Finished;
        ui32 RChunksIndex;
        ui32 Offset;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment<TKey, TMemRec>::TIndexBuilder
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSegment<TKey, TMemRec>::TIndexBuilder {

        static const ui32 RecSize = sizeof(TKey) + sizeof(TMemRec);
        static const ui32 SuffixSize = sizeof(TIdxDiskPlaceHolder);
        static_assert((RecSize >> 2 << 2) == RecSize, "expect (RecSize >> 2 << 2) == RecSize");
        static_assert((SuffixSize >> 2 << 2) == SuffixSize, "expect (SuffixSize >> 2 << 2) == SuffixSize");
        static_assert(sizeof(TIdxDiskLinker) <= sizeof(TIdxDiskPlaceHolder), "expect sizeof(TIdxDiskLinker) <= sizeof(TIdxDiskPlaceHolder)");

    public:
        TIndexBuilder(TVDiskContextPtr vctx, EWriterDataType type, ui8 owner, ui64 ownerRound, ui32 chunkSize,
                      ui32 appendBlockSize, ui32 writeBlockSize, ui64 sstId, bool createdByRepl,
                      TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue, TDeque<TChunkIdx>& rchunks)
            : Consumer(TMemoryConsumer(WriterDataTypeToMemConsumer(vctx, type, false)))
            , Owner(owner)
            , OwnerRound(ownerRound)
            , Priority(EWriterDataTypeToPriority(type))
            , ChunkSize(chunkSize)
            , AppendBlockSize(appendBlockSize)
            , WriteBlockSize(writeBlockSize)
            , MsgQueue(msgQueue)
            , RChunks(rchunks)
            , RecsPos(0)
            , OutboundPos(0)
            , InplaceDataTotalSize(0)
            , HugeDataTotalSize(0)
            , Items(0)
            , ItemsWithInplacedData(0)
            , ItemsWithHugeData(0)
            , Recs(TMemoryConsumer(Consumer))
            , Outbound(TMemoryConsumer(Consumer))
            , SstId(sstId)
            , FirstLsn(0)
            , LastLsn(0)
            , Conclusion()
            , Finished(false)
            , CreatedByRepl(createdByRepl)
            , PendingOp(EPendingOperation::NONE)
            , NumRecsPerChunk((ChunkSize - sizeof(TIdxDiskLinker)) / sizeof(TRec))
            , NumDiskPartsPerChunk((ChunkSize - sizeof(TIdxDiskLinker)) / sizeof(TDiskPart))
            , LevelSegment(new TLevelSegment(vctx))
        {
            Recs.reserve(ChunkSize / sizeof(TRec)); // reserve for one chunk
        }

        bool Empty() const {
            return !Items;
        }

        ui32 GetUsageAfterPush(ui32 chunks, ui32 intermSize, ui32 numAddedOuts) const {
            ui32 items = Items + 1; // + 1 for the record being added
            ui32 outs = Outbound.size() + numAddedOuts;

            // if linker record doesn't fit in current chunk, we start a new one
            if (intermSize + sizeof(TIdxDiskLinker) > ChunkSize) {
                ++chunks;
                intermSize = 0;
            }

            if (intermSize) {
                const ui32 numItems = (ChunkSize - (sizeof(TIdxDiskLinker) + intermSize)) / sizeof(TRec);
                if (items <= numItems) {
                    intermSize += items * sizeof(TRec);
                    items = 0;
                } else {
                    items -= numItems;
                    ++chunks;
                    intermSize = 0;
                }
            }
            if (items) {
                Y_DEBUG_ABORT_UNLESS(!intermSize);
                chunks += items / NumRecsPerChunk;
                intermSize += (items % NumRecsPerChunk) * sizeof(TRec);
            }

            if (intermSize) {
                const ui32 numOuts = (ChunkSize - (sizeof(TIdxDiskLinker) + intermSize)) / sizeof(TDiskPart);
                if (outs <= numOuts) {
                    intermSize += outs * sizeof(TDiskPart);
                    outs = 0;
                } else {
                    outs -= numOuts;
                    ++chunks;
                    intermSize = 0;
                }
            }
            if (outs) {
                Y_DEBUG_ABORT_UNLESS(!intermSize);
                chunks += outs / NumDiskPartsPerChunk;
                intermSize += (outs % NumDiskPartsPerChunk) * sizeof(TDiskPart);
            }

            if (intermSize + sizeof(TIdxDiskPlaceHolder) > ChunkSize) {
                ++chunks;
            }

            return chunks;
        }

        void Push(const TKey &key, const TMemRec &memRec, const TDataMerger *dataMerger) {
            // check that keys are coming in strictly ascending order
            Y_ABORT_UNLESS(Recs.empty() || Recs.back().Key < key);

            switch (memRec.GetType()) {
                case TBlobType::DiskBlob: {
                    InplaceDataTotalSize += memRec.DataSize();
                    ItemsWithInplacedData += !!memRec.DataSize();
                    Recs.push_back(TRec(key, memRec));
                    break;
                }
                case TBlobType::HugeBlob: {
                    const TVector<TDiskPart> &saved = dataMerger->GetHugeBlobMerger().SavedData();
                    Y_ABORT_UNLESS(saved.size() == 1);

                    TMemRec memRecTmp(memRec);
                    memRecTmp.SetHugeBlob(saved.at(0));
                    HugeDataTotalSize += memRecTmp.DataSize();
                    ItemsWithHugeData++;
                    Recs.push_back(TRec(key, memRecTmp));
                    break;
                }
                case TBlobType::ManyHugeBlobs: {
                    auto beg = dataMerger->GetHugeBlobMerger().SavedData().begin();
                    auto end = dataMerger->GetHugeBlobMerger().SavedData().end();

                    Y_DEBUG_ABORT_UNLESS(beg + 1 < end);
                    TMemRec newMemRec(memRec);
                    ui32 idx = ui32(Outbound.size());
                    ui32 num = ui32(end - beg);
                    ui32 size = 0;
                    for (auto it = beg; it != end; ++it) {
                        size += it->Size;
                    }
                    newMemRec.SetManyHugeBlobs(idx, num, size);
                    for (auto it = beg; it != end; ++it) {
                        Outbound.push_back(*it);
                    }
                    HugeDataTotalSize += size + sizeof(TDiskPart) * num;

                    Recs.push_back(TRec(key, newMemRec));
                    ItemsWithHugeData++;
                    break;
                }
                default: Y_ABORT("Impossible case");
            }

            ++Items;
        }

        void PrepareForFlush(ui64 firstLsn, ui64 lastLsn, TDataWriterConclusion&& conclusion) {
            FirstLsn = firstLsn;
            LastLsn = lastLsn;

            Writer = std::move(conclusion.Writer);
            Conclusion.Addr = TDiskPart(0, 0, 0);
            Conclusion.UsedChunks = std::move(conclusion.UsedChunks);
            Conclusion.OutboundItems = Outbound.size();
            Conclusion.LevelSegment = LevelSegment;

            // finish current data chunk if there is no space for at least linker record
            if (Writer && sizeof(TIdxDiskLinker) > Writer->GetFreeSpace()) {
                Writer->FinishChunk();
                Writer.reset();
            }

            // if there is writer, then make bookmark here -- we start index data; otherwise start new chunk
            if (Writer) {
                Writer->SetBookmark();
            } else {
                StartIndexChunk();
            }
        }

        // returns true when done, false means 'continue calling me, I'have more chunks to write'
        bool FlushNext(ui32 maxMsgs) {
            if (Empty()) {
                // no data at all
                return true;
            }

            return PackData(maxMsgs);
        }

        const TIndexWriterConclusion<TKey, TMemRec> &GetConclusion() const {
            return Conclusion;
        }

    private:
        enum class EBlockStatus {
            IN_PROGRESS,
            NOT_ENOUGH_SPACE,
            FINISHED
        };

        enum class EPendingOperation {
            FINISH_PLACEHOLDER,
            FINISH_LINKER,
            NONE
        };

    private:
        TMemoryConsumer Consumer;
        const ui8 Owner;
        const ui64 OwnerRound;
        const ui8 Priority;
        const ui32 ChunkSize;
        const ui32 AppendBlockSize;
        const ui32 WriteBlockSize;
        TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& MsgQueue;
        TDeque<TChunkIdx>& RChunks;
        ui32 RecsPos;
        ui32 OutboundPos;
        ui64 InplaceDataTotalSize;
        ui64 HugeDataTotalSize;
        ui32 Items;
        ui32 ItemsWithInplacedData;
        ui32 ItemsWithHugeData;
        TTrackableVector<TRec> Recs;
        TTrackableVector<TDiskPart> Outbound;

        const ui64 SstId;
        ui64 FirstLsn;
        ui64 LastLsn;
        TIndexWriterConclusion<TKey, TMemRec> Conclusion;
        std::unique_ptr<TBufferedChunkWriter> Writer;
        bool Finished; // just for VERIFY, i.e. internal consistency checking
        bool CreatedByRepl;
        EPendingOperation PendingOp;

        const ui32 NumRecsPerChunk;
        const ui32 NumDiskPartsPerChunk;

        // resulting LevelSegment as if it would be loaded
        TIntrusivePtr<TLevelSegment> LevelSegment;

        void PutLinker() {
            TIdxDiskLinker linker(Conclusion.Addr);
            Writer->Push(&linker, sizeof(linker));
        }

        void PutPlaceHolder() {
            // fill in LevelSegment information structure
            auto& info = LevelSegment->Info;
            info.FirstLsn = FirstLsn;
            info.LastLsn = LastLsn;
            info.InplaceDataTotalSize = InplaceDataTotalSize;
            info.HugeDataTotalSize = HugeDataTotalSize;
            info.IdxTotalSize = sizeof(TRec) * Items;
            info.Chunks = Conclusion.UsedChunks.size();
            info.IndexParts = Conclusion.IndexParts + 1;
            info.Items = Items;
            info.ItemsWithInplacedData = ItemsWithInplacedData;
            info.ItemsWithHugeData = ItemsWithHugeData;
            info.OutboundItems = Outbound.size();
            if (CreatedByRepl) {
                info.SetCreatedByRepl();
            }
            info.CTime = TAppData::TimeProvider->Now();

            // move Recs/Outbound into LevelSegment we are going to use
            Recs.shrink_to_fit();
            Outbound.shrink_to_fit();
            LevelSegment->LoadedIndex = std::move(Recs);
            LevelSegment->LoadedOutbound = std::move(Outbound);

            // fill in all chunks vector used in the sst
            LevelSegment->AllChunks = Conclusion.UsedChunks;

            // set up SST id
            LevelSegment->AssignedSstId = SstId;

            // fill in storage ratio
            auto ratio = MakeIntrusive<NHullComp::TSstRatio>();
            ratio->IndexItemsTotal = ratio->IndexItemsKeep = info.Items;
            ratio->IndexBytesTotal = ratio->IndexBytesKeep = info.IdxTotalSize;
            ratio->InplacedDataTotal = ratio->InplacedDataKeep = info.InplaceDataTotalSize;
            ratio->HugeDataTotal = ratio->HugeDataKeep = info.HugeDataTotalSize;
            LevelSegment->StorageRatio.Set(ratio);

            // write out place holder
            TIdxDiskPlaceHolder placeHolder(SstId);
            placeHolder.Info = info;
            placeHolder.PrevPart = Conclusion.Addr;
            Writer->Push(&placeHolder, sizeof(placeHolder));
        }

        EBlockStatus GenerateIndexBlockData(ui32 maxBlockSize) {
            Y_ABORT_UNLESS(RecsPos <= Items && OutboundPos <= Outbound.size());
            if (ui32 recsLeft = Items - RecsPos) {
                if (ui32 numItems = Min<ui32>(recsLeft, maxBlockSize / sizeof(TRec))) {
                    Writer->Push(&Recs[RecsPos], numItems * sizeof(TRec));
                    RecsPos += numItems;
                    return EBlockStatus::IN_PROGRESS;
                } else {
                    return EBlockStatus::NOT_ENOUGH_SPACE;
                }
            } else if (ui32 outboundLeft = Outbound.size() - OutboundPos) {
                if (ui32 numItems = Min<ui32>(outboundLeft, maxBlockSize / sizeof(TDiskPart))) {
                    Writer->Push(&Outbound[OutboundPos], numItems * sizeof(TDiskPart));
                    OutboundPos += numItems;
                    return EBlockStatus::IN_PROGRESS;
                } else {
                    return EBlockStatus::NOT_ENOUGH_SPACE;
                }
            } else {
                return EBlockStatus::FINISHED;
            }
        }

        void StartIndexChunk() {
            Y_ABORT_UNLESS(!Writer);
            Y_ABORT_UNLESS(!Finished);

            // obtain chunk index from reserved chunks list
            Y_ABORT_UNLESS(RChunks);
            const TChunkIdx chunkIdx = RChunks.front();
            RChunks.pop_front();

            // create new writer
            Writer = std::make_unique<TBufferedChunkWriter>(TMemoryConsumer(Consumer), Owner, OwnerRound, Priority,
                ChunkSize, AppendBlockSize, WriteBlockSize, chunkIdx, MsgQueue);

            // bookmark start of index data
            Writer->SetBookmark();

            // mark chunk as used
            Conclusion.UsedChunks.push_back(chunkIdx);
        }

        void FinishIndexChunk() {
            LevelSegment->LastPartAddr = Conclusion.Addr = Writer->GetDiskPartForBookmark();
            LevelSegment->IndexParts.insert(LevelSegment->IndexParts.begin(), LevelSegment->LastPartAddr);
            Writer->FinishChunk();
            Writer.reset();
            ++Conclusion.IndexParts;
        }

        bool PackData(ui32 maxMsgs) {
            Y_ABORT_UNLESS(!Finished);

            while (MsgQueue.size() < maxMsgs) {
                switch (PendingOp) {
                    case EPendingOperation::FINISH_PLACEHOLDER:
                        FinishIndexChunk();
                        Finished = true;
                        return true;

                    case EPendingOperation::FINISH_LINKER:
                        FinishIndexChunk();
                        StartIndexChunk();
                        PendingOp = EPendingOperation::NONE;
                        continue;

                    case EPendingOperation::NONE:
                        break;
                }

                EBlockStatus status = EBlockStatus::NOT_ENOUGH_SPACE;

                ui32 freeSpace = Writer->GetFreeSpace();
                if (freeSpace > sizeof(TIdxDiskLinker)) {
                    ui32 maxBlockSize = Min<ui32>(freeSpace - sizeof(TIdxDiskLinker), Writer->GetBlockSize());
                    status = GenerateIndexBlockData(maxBlockSize);
                    Y_ABORT_UNLESS(MsgQueue.size() <= maxMsgs);
                    if (MsgQueue.size() == maxMsgs) {
                        // we have now reached maxMsgs limit, but when we will be called next time, the same status
                        // will be returned, so there is no need to preserve it
                        break;
                    }
                }

                switch (status) {
                    case EBlockStatus::IN_PROGRESS:
                        break;

                    case EBlockStatus::FINISHED:
                        if (Writer->GetFreeSpace() >= sizeof(TIdxDiskPlaceHolder)) {
                            PutPlaceHolder();
                            Y_ABORT_UNLESS(MsgQueue.size() <= maxMsgs);
                            if (MsgQueue.size() == maxMsgs) {
                                PendingOp = EPendingOperation::FINISH_PLACEHOLDER;
                                return false;
                            }
                            FinishIndexChunk();
                            Finished = true;
                            return true;
                        }
                        [[fallthrough]];

                    case EBlockStatus::NOT_ENOUGH_SPACE:
                        PutLinker();
                        Y_ABORT_UNLESS(MsgQueue.size() <= maxMsgs);
                        if (MsgQueue.size() == maxMsgs) {
                            PendingOp = EPendingOperation::FINISH_LINKER;
                            return false;
                        }
                        FinishIndexChunk();
                        StartIndexChunk();
                        break;
                }
            }

            // message queue is full, do not continue processing
            return false;
        }
    };


    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment<TKey, TMemRec>::TWriter
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSegment<TKey, TMemRec>::TWriter {
        typedef typename ::NKikimr::TLevelSegment<TKey, TMemRec>::TDataWriter TDataWriter;
        typedef typename ::NKikimr::TLevelSegment<TKey, TMemRec>::TIndexBuilder TIndexBuilder;

    public:
        TWriter(TVDiskContextPtr vctx, EWriterDataType type, ui32 chunksToUse, ui8 owner, ui64 ownerRound,
                ui32 chunkSize, ui32 appendBlockSize, ui32 writeBlockSize, ui64 sstId, bool createdByRepl,
                TDeque<TChunkIdx>& rchunks, TRopeArena& arena, bool addHeader)
            : DataWriter(vctx, type, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize, MsgQueue, rchunks)
            , IndexBuilder(vctx, type, owner, ownerRound, chunkSize, appendBlockSize, writeBlockSize, sstId,
                    createdByRepl, MsgQueue, rchunks)
            , ChunksToUse(chunksToUse)
            , ChunkSize(chunkSize)
            , Arena(arena)
            , AddHeader(addHeader)
        {}

        bool Empty() const {
            return IndexBuilder.Empty();
        }

        bool CheckSpace(ui32 inplacedDataSize, ui32 numAddedOuts) {
            ui32 chunks = 0;
            ui32 intermSize = 0;
            DataWriter.GetUsageAfterPush(inplacedDataSize, &chunks, &intermSize);
            return IndexBuilder.GetUsageAfterPush(chunks, intermSize, numAddedOuts) <= ChunksToUse;
        }

        bool PushIndexOnly(const TKey& key, const TMemRec& memRec, const TDataMerger *dataMerger, ui32 inplacedDataSize,
                TDiskPart *location) {
            // inplacedDataSize must be nonzero for DiskBlob with data and zero in all other cases
            ui32 numAddedOuts = dataMerger->GetHugeBlobMerger().SavedData().size();
            if (!CheckSpace(inplacedDataSize, numAddedOuts)) {
                return false;
            }

            // generate TMemRec to perform actual insertion into index
            TMemRec memRecToAdd(memRec);
            switch (memRec.GetType()) {
                case TBlobType::HugeBlob:
                    Y_DEBUG_ABORT_UNLESS(inplacedDataSize == 0);
                    Y_DEBUG_ABORT_UNLESS(numAddedOuts == 1);
                    break;

                case TBlobType::ManyHugeBlobs:
                    Y_DEBUG_ABORT_UNLESS(inplacedDataSize == 0);
                    Y_DEBUG_ABORT_UNLESS(numAddedOuts > 1);
                    break;

                case TBlobType::DiskBlob:
                    Y_DEBUG_ABORT_UNLESS(numAddedOuts == 0);
                    if (inplacedDataSize) {
                        *location = DataWriter.Preallocate(inplacedDataSize);
                        memRecToAdd.SetDiskBlob(*location);
                    }
                    break;

                default: Y_ABORT("Impossible case");
            }

            IndexBuilder.Push(key, memRecToAdd, dataMerger);

            return true;
        }

        TDiskPart PushDataOnly(TRope&& buffer) {
            return DataWriter.Push(std::move(buffer));
        }

        // return false on no-more-space
        bool Push(const TKey &key, const TMemRec &memRec, const TDataMerger *dataMerger, ui32 inplacedDataSize = 0) {
            // if this is filled disk blob, then we extract data and calculate inplacedDataSize
            TRope data;
            const auto& diskBlobMerger = dataMerger->GetDiskBlobMerger();
            if (memRec.GetType() == TBlobType::DiskBlob && !diskBlobMerger.Empty()) {
                data = diskBlobMerger.CreateDiskBlob(Arena, AddHeader);
                Y_DEBUG_ABORT_UNLESS(!inplacedDataSize);
                inplacedDataSize = data.GetSize();
            }

            TDiskPart preallocatedLocation;
            if (!PushIndexOnly(key, memRec, dataMerger, inplacedDataSize, &preallocatedLocation)) {
                return false;
            }

            // write inplace data if we have some
            if (data) {
                Y_DEBUG_ABORT_UNLESS(data.GetSize() == inplacedDataSize);
                TDiskPart writtenLocation = DataWriter.Push(data);
                Y_DEBUG_ABORT_UNLESS(writtenLocation == preallocatedLocation);
            }

            return true;
        }

        // returns true when done, false means 'continue calling me, I'have more chunks to write'
        bool FlushNext(ui64 firstLsn, ui64 lastLsn, ui32 maxMsgs) {
            if (!DataWriter.IsFinished()) {
                IndexBuilder.PrepareForFlush(firstLsn, lastLsn, DataWriter.Finish());
            }
            return IndexBuilder.FlushNext(maxMsgs);
        }

        std::unique_ptr<NPDisk::TEvChunkWrite> GetPendingMessage() {
            if (!MsgQueue) {
                return nullptr; // no messages to send
            }

            std::unique_ptr<NPDisk::TEvChunkWrite> msg = std::move(MsgQueue.front());
            MsgQueue.pop();
            return msg;
        }

        const TIndexWriterConclusion<TKey, TMemRec> &GetConclusion() const {
            const auto &conclusion = IndexBuilder.GetConclusion();
            Y_ABORT_UNLESS(std::find(conclusion.UsedChunks.begin(), conclusion.UsedChunks.end(), conclusion.Addr.ChunkIdx) !=
                conclusion.UsedChunks.end());
            return conclusion;
        }

    private:
        TDataWriter DataWriter;
        TIndexBuilder IndexBuilder;
        const ui32 ChunksToUse;
        const ui32 ChunkSize;
        TRopeArena& Arena;
        const bool AddHeader;

        // pending messages
        TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>> MsgQueue;
    };

} // NKikimr
