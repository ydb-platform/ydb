#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugedefs.h>
#include <util/generic/noncopyable.h>

namespace NKikimr {

    class TDataMerger : TNonCopyable {
    public:
        struct TCollectTask {
            TDiskBlobMerger BlobMerger; // base blob for compaction (obtained from in-memory records)
            std::vector<std::tuple<TDiskPart, ui8>> Reads; // a set of extra reads _of distinct parts, not blobs_

            void Clear() {
                BlobMerger.Clear();
                Reads.clear();
            }
        };

        struct THugeBlobWrite {
            ui8 PartIdx;
            const TRope *From;
            TDiskPart To;
        };

        struct THugeBlobMove {
            ui8 PartIdx;
            TDiskPart From;
            TDiskPart To;
        };

    private:
        struct TPart {
            const TRope *InMemData = nullptr;
            TDiskPart SmallBlobPart; // a reference to part data, without any headers
            TDiskPart HugeBlob; // a reference to the whole huge blob, with headers added
            ui64 HugeBlobCircaLsn = 0;
            ui32 HugePartSize = 0;
            bool IsMetadataPart = false;

            bool NeedHugeSlot() const {
                return HugeBlob.Empty() && (InMemData || !SmallBlobPart.Empty());
            }
        };

        // immutable fields
        TBlobStorageGroupType GType;
        bool AddHeader = false;

        // clearable fields
        std::vector<TPart> Parts;
        std::vector<TDiskPart> DeletedHugeBlobs;
        NMatrix::TVectorType PartsMask; // mask of all present parts
        std::vector<TDiskPart> SavedHugeBlobs;
        bool Finished = false;
        std::vector<ui32> SlotsToAllocate;
        TCollectTask CollectTask;
        std::vector<THugeBlobWrite> HugeBlobWrites;
        std::vector<THugeBlobMove> HugeBlobMoves;

    public:
        TDataMerger(TBlobStorageGroupType gtype, bool addHeader)
            : GType(gtype)
            , AddHeader(addHeader)
            , Parts(GType.TotalPartCount())
            , PartsMask(0, GType.TotalPartCount())
        {}

        bool Empty() const {
            return PartsMask.Empty();
        }

        void Clear() {
            std::ranges::fill(Parts, TPart());
            DeletedHugeBlobs.clear();
            PartsMask.Clear();
            SavedHugeBlobs.clear();
            Finished = false;
            SlotsToAllocate.clear();
            CollectTask.Clear();
            HugeBlobWrites.clear();
            HugeBlobMoves.clear();
        }

        void Add(const TMemRecLogoBlob& memRec, std::variant<const TRope*, const TDiskPart*> dataOrOutbound,
                ui64 circaLsn, const TLogoBlobID& fullId) {
            const NMatrix::TVectorType parts = memRec.GetLocalParts(GType);

            if (memRec.GetType() == TBlobType::MemBlob) {
                auto data = std::visit<const TRope*>(TOverloaded{
                    [&](const TRope *x) { return x; },
                    [&](const TDiskPart*) { return nullptr; }
                }, dataOrOutbound);

                Y_ABORT_UNLESS(data);
                Y_DEBUG_ABORT_UNLESS(parts.CountBits() == 1); // only single part per record in memory
                for (ui8 partIdx : parts) {
                    Y_DEBUG_ABORT_UNLESS(partIdx < Parts.size());
                    Parts[partIdx].InMemData = data;
                    PartsMask.Set(partIdx);
                }
            } else {
                auto outbound = std::visit<const TDiskPart*>(TOverloaded{
                    [&](const TRope *x) { Y_DEBUG_ABORT_UNLESS(!x); return nullptr; },
                    [&](const TDiskPart *x) { return x; }
                }, dataOrOutbound);

                TDiskDataExtractor extr;
                memRec.GetDiskData(&extr, outbound);

                if (memRec.GetType() == TBlobType::DiskBlob) {
                    const TDiskPart& location = extr.SwearOne();
                    ui32 offset = AddHeader ? TDiskBlob::HeaderSize : 0;
                    for (ui8 partIdx : parts) {
                        const ui32 partSize = GType.PartSize(TLogoBlobID(fullId, partIdx + 1));
                        Y_DEBUG_ABORT_UNLESS(partIdx < Parts.size());
                        if (partSize) {
                            Parts[partIdx].SmallBlobPart = {location.ChunkIdx, location.Offset + offset, partSize};
                        }
                        offset += partSize;
                        PartsMask.Set(partIdx);
                    }
                } else {
                    AddHugeBlob(extr.Begin, extr.End, parts, circaLsn);
                }
            }
        }

        void Finish(bool targetingHugeBlob, const TLogoBlobID& fullId, TBlobType::EType *type, ui32 *inplacedDataSize) {
            Y_DEBUG_ABORT_UNLESS(!Finished);

            if (!Empty()) {
                // scan through all the parts, see what we got
                NMatrix::TVectorType inMemParts(0, GType.TotalPartCount());
                NMatrix::TVectorType smallDiskParts(0, GType.TotalPartCount());
                NMatrix::TVectorType hugeDiskParts(0, GType.TotalPartCount());
                for (ui8 partIdx : PartsMask) {
                    TPart& part = Parts[partIdx];
                    if (part.InMemData) {
                        inMemParts.Set(partIdx);
                    }
                    if (!part.SmallBlobPart.Empty()) {
                        smallDiskParts.Set(partIdx);
                    }
                    if (!part.HugeBlob.Empty()) {
                        hugeDiskParts.Set(partIdx);
                    }
                }

                bool producingHugeBlob = false;

                if (inMemParts.Empty() && smallDiskParts.Empty()) { // we only have huge blobs, so keep it this way
                    producingHugeBlob = true;
                } else {
                    producingHugeBlob = targetingHugeBlob;
                }

                // calculate blob for if we are going to keep it inplace
                *inplacedDataSize = producingHugeBlob
                    ? 0
                    : TDiskBlob::CalculateBlobSize(GType, fullId, PartsMask, AddHeader);

                TDiskBlobMerger merger;

                for (ui8 partIdx : PartsMask) {
                    TPart& part = Parts[partIdx];
                    const ui32 partSize = GType.PartSize(TLogoBlobID(fullId, partIdx + 1));
                    part.IsMetadataPart = !partSize;
                    const NMatrix::TVectorType partMask = NMatrix::TVectorType::MakeOneHot(partIdx, GType.TotalPartCount());

                    if (producingHugeBlob) {
                        SavedHugeBlobs.push_back(part.HugeBlob);
                        if (!part.IsMetadataPart && part.NeedHugeSlot()) {
                            part.HugePartSize = TDiskBlob::CalculateBlobSize(GType, fullId, partMask, AddHeader);
                            SlotsToAllocate.push_back(part.HugePartSize);
                        }
                    } else {
                        if (part.InMemData) { // prefer in-memory data if we have options
                            merger.Add(TDiskBlob(part.InMemData, partMask, GType, fullId));
                        } else if (!part.SmallBlobPart.Empty()) {
                            CollectTask.Reads.emplace_back(part.SmallBlobPart, partIdx);
                        } else if (!part.HugeBlob.Empty()) { // dropping this huge part after compaction
                            TDiskPart location;
                            if (part.HugeBlob.Size == partSize) {
                                location = part.HugeBlob;
                            } else if (part.HugeBlob.Size == partSize + TDiskBlob::HeaderSize) {
                                location = TDiskPart(part.HugeBlob.ChunkIdx, part.HugeBlob.Offset + TDiskBlob::HeaderSize,
                                    part.HugeBlob.Size - TDiskBlob::HeaderSize);
                            } else {
                                Y_ABORT("incorrect huge blob size");
                            }
                            CollectTask.Reads.emplace_back(location, partIdx);
                            DeletedHugeBlobs.push_back(part.HugeBlob);
                        } else { // add metadata part to merger
                            merger.AddPart(TRope(), GType, TLogoBlobID(fullId, partIdx + 1));
                        }
                    }
                }

                Y_DEBUG_ABORT_UNLESS(!producingHugeBlob || merger.Empty());
                CollectTask.BlobMerger = merger;

                *type = !producingHugeBlob ? TBlobType::DiskBlob :
                    PartsMask.CountBits() > 1 ? TBlobType::ManyHugeBlobs : TBlobType::HugeBlob;

                Y_DEBUG_ABORT_UNLESS(SavedHugeBlobs.size() == (producingHugeBlob ? PartsMask.CountBits() : 0));
            }

            Finished = true;
        }

        void FinishFromBlob() {
            Y_DEBUG_ABORT_UNLESS(!Finished);
            Finished = true;
        }

        void AddHugeBlob(const TDiskPart *begin, const TDiskPart *end, const NMatrix::TVectorType& parts, ui64 circaLsn) {
            Y_DEBUG_ABORT_UNLESS(parts.CountBits() == end - begin);
            const TDiskPart *location = begin;
            for (ui8 partIdx : parts) {
                Y_DEBUG_ABORT_UNLESS(partIdx < Parts.size());
                auto& part = Parts[partIdx];
                if (location->ChunkIdx) { // consider only data parts
                    if (part.HugeBlob.Empty() || part.HugeBlobCircaLsn < circaLsn) {
                        if (!part.HugeBlob.Empty()) {
                            DeletedHugeBlobs.push_back(part.HugeBlob);
                        }
                        part.HugeBlob = *location;
                        part.HugeBlobCircaLsn = circaLsn;
                    } else {
                        DeletedHugeBlobs.push_back(*location);
                    }
                }
                ++location;
            }
            Y_DEBUG_ABORT_UNLESS(location == end);
            PartsMask |= parts;
        }

        template<typename T>
        T& CheckFinished(T& value) const {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return value;
        }

        const NMatrix::TVectorType GetParts() const { return CheckFinished(PartsMask); }
        const std::vector<TDiskPart>& GetSavedHugeBlobs() const { return CheckFinished(SavedHugeBlobs); }
        const std::vector<TDiskPart>& GetDeletedHugeBlobs() const { return CheckFinished(DeletedHugeBlobs); }
        const TCollectTask& GetCollectTask() const { return CheckFinished(CollectTask); }
        const auto& GetHugeBlobWrites() const { return CheckFinished(HugeBlobWrites); }
        const auto& GetHugeBlobMoves() const { return CheckFinished(HugeBlobMoves); }

        bool Ready() const {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return CollectTask.Reads.empty()
                && HugeBlobWrites.empty()
                && HugeBlobMoves.empty();
        }

        TRope CreateDiskBlob(TRopeArena& arena) {
            Y_DEBUG_ABORT_UNLESS(Finished);
            Y_DEBUG_ABORT_UNLESS(!CollectTask.BlobMerger.Empty());
            return CollectTask.BlobMerger.CreateDiskBlob(arena, AddHeader);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Huge slots operation

        std::vector<ui32>& GetSlotsToAllocate() {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return SlotsToAllocate;
        }

        void ApplyAllocatedSlots(std::vector<TDiskPart>& allocatedSlots) {
            size_t savedIndex = 0;
            size_t index = 0;
            for (ui8 partIdx : PartsMask) {
                if (const TPart& part = Parts[partIdx]; !part.HugeBlob.Empty()) {
                    Y_DEBUG_ABORT_UNLESS(part.HugeBlob == SavedHugeBlobs[savedIndex]);
                    ++savedIndex;
                } else if (!part.IsMetadataPart && part.NeedHugeSlot()) {
                    TDiskPart& location = SavedHugeBlobs[savedIndex++] = allocatedSlots[index++];
                    Y_ABORT_UNLESS(part.HugePartSize <= location.Size);
                    location.Size = part.HugePartSize;
                    if (part.InMemData) {
                        HugeBlobWrites.push_back({
                            .PartIdx = partIdx,
                            .From = part.InMemData,
                            .To = location,
                        });
                    } else if (!part.SmallBlobPart.Empty()) {
                        HugeBlobMoves.push_back({
                            .PartIdx = partIdx,
                            .From = part.SmallBlobPart,
                            .To = location,
                        });
                    } else {
                        Y_ABORT("impossible case");
                    }
                }
            }
            Y_DEBUG_ABORT_UNLESS(savedIndex == SavedHugeBlobs.size());
            Y_DEBUG_ABORT_UNLESS(index == allocatedSlots.size());
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // THandoffMap transformations; called before any reads were done, metadata-only processing (but within Fresh
        // compation some data might be already available)

        void MakeEmpty() {
            Y_DEBUG_ABORT_UNLESS(Finished);
            DeletedHugeBlobs.insert(DeletedHugeBlobs.end(), SavedHugeBlobs.begin(), SavedHugeBlobs.end());
            auto deletedHugeBlobs = std::move(DeletedHugeBlobs);
            Clear();
            DeletedHugeBlobs = std::move(deletedHugeBlobs);
            Finished = true;
        }

        void FilterLocalParts(NMatrix::TVectorType remainingLocalParts, const TLogoBlobID& fullId,
                TBlobType::EType *type, ui32 *inplacedDataSize) {
            Y_DEBUG_ABORT_UNLESS(Finished);
            const NMatrix::TVectorType partsToRemove = PartsMask & ~remainingLocalParts;
            for (ui8 partIdx : partsToRemove) { // local parts to remove
                if (const auto& part = Parts[partIdx].HugeBlob; !part.Empty()) {
                    DeletedHugeBlobs.push_back(part);
                    const auto it = std::remove(SavedHugeBlobs.begin(), SavedHugeBlobs.end(), part);
                    Y_ABORT_UNLESS(std::next(it) == SavedHugeBlobs.end());
                    SavedHugeBlobs.pop_back();
                } else if (Parts[partIdx].InMemData) {
                    CollectTask.BlobMerger.ClearPart(partIdx);
                }
                Parts[partIdx] = {};
            }

            auto pred = [&](const std::tuple<TDiskPart, ui8>& x) { return partsToRemove.Get(std::get<1>(x)); };
            CollectTask.Reads.erase(std::remove_if(CollectTask.Reads.begin(), CollectTask.Reads.end(), pred),
                CollectTask.Reads.end());

            PartsMask &= remainingLocalParts;

            // recalculate memrec data for this new entry
            *type = SavedHugeBlobs.empty() ? TBlobType::DiskBlob :
                SavedHugeBlobs.size() == 1 ? TBlobType::HugeBlob : TBlobType::ManyHugeBlobs;
            if (!Empty() && *type == TBlobType::DiskBlob) {
                *inplacedDataSize = TDiskBlob::CalculateBlobSize(GType, fullId, PartsMask, AddHeader);
            }
        }

    };

} // NKikimr
