#include "query_readbatch.h"
#include "query_readactor.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <util/generic/algorithm.h>

using namespace NKikimrServices;

namespace NKikimr {

    using namespace NReadBatcher;

    ////////////////////////////////////////////////////////////////////////////
    // TReadBatcher implementation
    ////////////////////////////////////////////////////////////////////////////
    // Traverse data parts for a single key
    void TReadBatcher::StartTraverse(
            const TLogoBlobID& id,
            void *cookie,
            ui8 queryPartId,
            ui32 queryShift,
            ui32 querySize) {
        Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
        Y_DEBUG_ABORT_UNLESS(!Traversing);
        ClearTmpItems();
        CurID = id;
        Cookie = cookie;
        Traversing = true;
        FoundAnything = false;
        TraverseOffs = Result->DataItems.size();
        QueryPartId = queryPartId;
        QueryShift = queryShift;
        QuerySize = querySize;
        FoundDiskItems.clear();
        FoundInMemItems.clear();
    }

    // We have data on disk
    void TReadBatcher::operator () (const TDiskPart &data, NMatrix::TVectorType parts) {
        Y_DEBUG_ABORT_UNLESS(Traversing);
        FoundDiskItems.emplace_back(data, parts);
    }

    void TReadBatcher::ProcessFoundDiskItem(const TDiskPart &data, NMatrix::TVectorType parts) {
        if (QueryPartId && !parts.Get(QueryPartId - 1)) {
            return; // we have no requested part here
        }

        const auto& gtype = Ctx->VCtx->Top->GType;
        ui32 blobSize = 0;
        for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
            blobSize += gtype.PartSize(TLogoBlobID(CurID, i + 1));
        }

        ui32 partOffs = data.Offset;
        if (data.Size == TDiskBlob::HeaderSize + blobSize) { // skip the header, if it is present
            partOffs += TDiskBlob::HeaderSize;
        } else {
            Y_ABORT_UNLESS(blobSize == data.Size);
        }

        for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
            const TLogoBlobID partId(CurID, i + 1);
            const ui32 partSize = gtype.PartSize(partId);
            if (QueryPartId == 0 || QueryPartId == i + 1) {
                FoundAnything = true;
                auto& tmpItem = TmpItems[i];
                if ((QueryShift >= partSize && partSize) || QuerySize > partSize - QueryShift) {
                    tmpItem.UpdateWithError(partId, Cookie);
                } else if (tmpItem.ShouldUpdateWithDisk()) {
                    const ui32 size = QuerySize ? QuerySize : partSize - QueryShift;
                    if (!size) { // for metadata reads
                        tmpItem.UpdateWithMemItem(partId, Cookie, TRope());
                    } else {
                        tmpItem.UpdateWithDiskItem(partId, Cookie, TDiskPart(data.ChunkIdx, partOffs + QueryShift, size));
                    }
                }
            }
            partOffs += partSize;
        }
    }

    // We have diskBlob in memory
    void TReadBatcher::operator () (const TDiskBlob &diskBlob) {
        Y_DEBUG_ABORT_UNLESS(Traversing);
        FoundInMemItems.push_back(diskBlob);
    }

    void TReadBatcher::ProcessFoundInMemItem(const TDiskBlob &diskBlob) {
        if (QueryPartId == 0 || diskBlob.GetParts().Get(QueryPartId - 1)) {
            // put data item iff we gather all parts OR we need a concrete part and parts contain it
            for (TDiskBlob::TPartIterator it = diskBlob.begin(), e = diskBlob.end(); it != e; ++it) {
                const ui8 partId = it.GetPartId();
                Y_ABORT_UNLESS(partId > 0);
                const TLogoBlobID blobId(CurID, partId);
                const ui32 partSize = diskBlob.GetPartSize(partId - 1);
                Y_ABORT_UNLESS(partSize == Ctx->VCtx->Top->GType.PartSize(blobId));
                if (QueryPartId == 0 || QueryPartId == partId) {
                    FoundAnything = true;
                    auto& item = TmpItems[partId - 1];
                    if ((QueryShift >= partSize && partSize) || QuerySize > partSize - QueryShift) {
                        item.UpdateWithError(blobId, Cookie);
                    } else if (item.ShouldUpdateWithMem()) {
                        const ui32 size = QuerySize ? QuerySize : partSize - QueryShift;
                        TRope temp;
                        item.UpdateWithMemItem(blobId, Cookie, it.GetPart(QueryShift, size, &temp));
                    }
                }
            }
        }
    }

    // Finish data traverse for a single key
    void TReadBatcher::FinishTraverse(const TIngress &ingress) {
        Y_DEBUG_ABORT_UNLESS(Traversing);
        Traversing = false;

        // process found items; first, we process disk items; then, we process in-mem items that may possibly
        // overwrite disk ones to prevent read IOPS
        for (const std::tuple<TDiskPart, NMatrix::TVectorType> &diskItem : FoundDiskItems) {
            ProcessFoundDiskItem(std::get<0>(diskItem), std::get<1>(diskItem));
        }
        for (const TDiskBlob &diskBlob : FoundInMemItems) {
            ProcessFoundInMemItem(diskBlob);
        }

        // NOTE: we may have parts that are not replicated yet;
        //       we MUST NOT return NO_DATA for them; but when parts are missing due to finished GC, we report NODATA
        NMatrix::TVectorType mustHave = ingress.PartsWeMustHaveLocally(Ctx->VCtx->Top.get(),
            Ctx->VCtx->ShortSelfVDisk, CurID);
        NMatrix::TVectorType actuallyHave = ingress.LocalParts(Ctx->VCtx->Top->GType);
        NMatrix::TVectorType missingParts = mustHave - actuallyHave;
        for (ui8 i = missingParts.FirstPosition(); i != missingParts.GetSize(); i = missingParts.NextPosition(i)) {
            // NOT_YET
            if (QueryPartId == 0 || i + 1 == QueryPartId) {
                Y_ABORT_UNLESS(TmpItems[i].Empty());
                FoundAnything = true;
                TmpItems[i].UpdateWithNotYet(TLogoBlobID(CurID, i + 1), Cookie);
            }
        }

        // We don't have found any data at all, we even don't have unreplicated parts
        if (FoundAnything) {
            // setup DataItems and read requests finally
            for (auto &x : TmpItems) {
                if (!x.Empty()) {
                    x.SetIngress(ingress);
                    Result->DataItems.push_back(x);
                    if (x.ReadFromDisk()) {
                        Result->DiskDataItemPtrs.push_back(&Result->DataItems.back());
                    }
                }
            }
        } else {
            PutNoData(TLogoBlobID(CurID, QueryPartId), ingress, Cookie);
        }
    }

    void TReadBatcher::AbortTraverse() {
        Y_DEBUG_ABORT_UNLESS(Traversing);
        Traversing = false;
    }

    TGlueRead *TReadBatcher::AddGlueRead(TDataItem *item) {
        Result->GlueReads.push_back(TGlueRead(item->ActualRead));
        item->SetGlueReqIdx(Result->GlueReads.size() - 1);
        return &Result->GlueReads.back();
    }

    void TReadBatcher::PrepareReadPlan() {
        Y_ABORT_UNLESS(!Result->DiskDataItemPtrs.empty() && Result->GlueReads.empty());

        // sort read requests
        Sort(Result->DiskDataItemPtrs.begin(), Result->DiskDataItemPtrs.end(), TDataItem::DiskPartLess);
        Y_ABORT_UNLESS(CheckDiskDataItemsOrdering(true));

        // plan real requests
        TGlueRead *back = nullptr;
        for (TDiskDataItemPtrs::iterator it = Result->DiskDataItemPtrs.begin(), e = Result->DiskDataItemPtrs.end();
                    it != e; ++it) {
            TDataItem *item = *it;
            if (!back || back->Part.ChunkIdx != item->ActualRead.ChunkIdx) {
                back = AddGlueRead(item);
            } else {
                if (back->Part.Includes(item->ActualRead)) {
                    // a special case for duplicate requests; we can get them when reading parts of logoblobs
                    item->SetGlueReqIdx(Result->GlueReads.size() - 1);
                } else {
                    ui32 prevEnd = back->Part.Offset + back->Part.Size;
                    ui32 nextBeg = item->ActualRead.Offset;
                    Y_ABORT_UNLESS(prevEnd <= nextBeg, "back: %s item: %s dataItems: %s",
                           back->Part.ToString().data(), item->ActualRead.ToString().data(), DiskDataItemsToString().data());

                    if (nextBeg <= prevEnd + Ctx->PDiskCtx->Dsk->GlueRequestDistanceBytes) {
                        // glue requests
                        back->Part.Size += (nextBeg - prevEnd) + item->ActualRead.Size;
                        item->SetGlueReqIdx(Result->GlueReads.size() - 1);
                    } else {
                        back = AddGlueRead(item);
                    }
                }
            }
        }
    }

    TString TReadBatcher::DiskDataItemsToString() const {
        TStringStream str;
        for (const auto &i : Result->DiskDataItemPtrs) {
            str << i->ActualRead.ToString() << " ";
        }
        return str.Str();
    }

    bool TReadBatcher::CheckDiskDataItemsOrdering(bool printOnFail) const {
        TDiskPart prevPart;
        bool first = true;
        for (const auto &i : Result->DiskDataItemPtrs) {
            if (first || prevPart.ChunkIdx != i->ActualRead.ChunkIdx) {
                first = false;
            } else {
                bool good = (prevPart == i->ActualRead) || (prevPart.Offset + prevPart.Size) <= (i->ActualRead.Offset);
                if (!good) {
                    if (printOnFail)
                        Cerr << "Items: " << DiskDataItemsToString()
                            << " OriginalQuery: " << Ctx->OrigEv->Get()->ToString() << "\n";
                    return false;
                }
            }
            prevPart = i->ActualRead;
        }
        return true;
    }

    IActor *TReadBatcher::CreateAsyncDataReader(const TActorId &notifyID, ui8 priority, NWilson::TTraceId traceId,
            bool isRepl) {
        if (Result->DiskDataItemPtrs.empty()) {
            return nullptr;
        } else {
            // prepare read plan
            PrepareReadPlan();
            Y_DEBUG_ABORT_UNLESS(!Result->GlueReads.empty());
            // evaluate total read size
            const ui32 blockSize = Ctx->PDiskCtx->Dsk->AppendBlockSize;
            for (const auto& item : Result->GlueReads) {
                const auto& part = item.Part;
                // adjust offset to append block size value
                ui32 size = part.Size;
                size += part.Offset % blockSize; // adjust read to the beginning of the block
                if (const ui32 rem = size % blockSize) {
                    size += blockSize - rem; // adjust read to the end of the block
                }
                // count calculated size in blocks
                PDiskReadBytes += size;
            }
            // start reader
            return CreateReadBatcherActor(Ctx, notifyID, Result, priority, std::move(traceId), isRepl);
        }
    }

} // NKikimr
