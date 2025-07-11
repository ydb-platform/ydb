#include "scrub_actor_impl.h"
#include "scrub_actor_sst_blob_merger.h"

namespace NKikimr {

    void TScrubCoroImpl::ScrubSst(TLevelSegmentPtr sst) {
        SstId = sst->AssignedSstId;
        STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS03, VDISKP(LogPrefix, "starting to scrub SST"), (SstId, SstId));
        auto blobsOnDisk = MakeBlobList(sst);
        ReadOutAndResilverIndex(sst);
        ReadOutSelectedBlobs(std::move(blobsOnDisk));
        SstId.reset();
        ++MonGroup.SstProcessed();
    }

    std::vector<TScrubCoroImpl::TBlobOnDisk> TScrubCoroImpl::MakeBlobList(TLevelSegmentPtr sst) {
        std::vector<TScrubCoroImpl::TBlobOnDisk> res;
        TSstBlobMerger merger(*Snap, GetBarriersEssence());
        TLevelSegment::TMemIterator iter(sst.Get());
        for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
            iter.PutToMerger(&merger);
            const TLogoBlobID& id = iter.GetCurKey().LogoBlobID();
            if (merger.Keep(id)) {
                res.insert(res.end(), merger.BlobsOnDisk.begin(), merger.BlobsOnDisk.end());
            } else {
                DropGarbageBlob(id);
            }
            merger.Clear();
        }
        return res;
    }

    void TScrubCoroImpl::ReadOutAndResilverIndex(TLevelSegmentPtr sst) {
        TTrackableVector<TLevelSegment::TRec> linearIndex(TMemoryConsumer(VCtx->SstIndex));
        sst->SaveLinearIndex(&linearIndex);

        TDiskPart prevPart;
        bool first = true;
        ui32 remainOutboundSize = sst->LoadedOutbound.size() * sizeof(TDiskPart);
        ui32 remainIndexSize = linearIndex.size() * sizeof(TLevelSegment::TRec);
        for (TDiskPart part : sst->IndexParts) {
            TString regen = TString::Uninitialized(part.Size);
            ui32 destLen = regen.size();
            char *dest = regen.Detach() + destLen;
            const TString& logPrefix = LogPrefix;
            auto prepend = [&destLen, &dest, &logPrefix](const void *data, ui32 len) {
                if (len) {
                    Y_VERIFY_S(len <= destLen, logPrefix);
                    destLen -= len;
                    dest -= len;
                    memcpy(dest, data, len);
                }
            };

            // first step: fill in the header
            if (first) {
                TIdxDiskPlaceHolder header(sst->AssignedSstId);
                header.PrevPart = prevPart;
                header.Info = sst->Info;
                if (sst->Info.IsCreatedByRepl()) {
                    header.Info.FirstLsn = header.Info.LastLsn = 0;
                }
                prepend(&header, sizeof(header));
            } else {
                TIdxDiskLinker header;
                header.PrevPart = prevPart;
                prepend(&header, sizeof(header));
            }

            // second step: fill outbound (if some data remains)
            const ui32 osize = Min(remainOutboundSize, destLen);
            remainOutboundSize -= osize;
            prepend(reinterpret_cast<const char*>(sst->LoadedOutbound.data()) + remainOutboundSize, osize);

            // third step: the index
            const ui32 isize = Min(remainIndexSize, destLen);
            remainIndexSize -= isize;
            prepend(reinterpret_cast<const char*>(linearIndex.data()) + remainIndexSize, isize);

            // fourth step: sanity check
            Y_VERIFY_S(!destLen, LogPrefix);

            std::optional<TRcBuf> data = Read(part);
            if (!data) {
                STLOGX(GetActorContext(), PRI_WARN, BS_VDISK_SCRUB, VDS13, VDISKP(LogPrefix, "index is corrupt, restoring"),
                    (SstId, sst->AssignedSstId), (Location, part));

                ui32 offset = part.Offset - part.Offset % ScrubCtx->PDiskCtx->Dsk->AppendBlockSize;
                if (const ui32 prefixLen = part.Offset - offset) {
                    // restore prefixLen bytes of data before the index
                    std::optional<TRcBuf> data = Read(TDiskPart(part.ChunkIdx, offset, prefixLen));
                    if (data) {
                        regen = TStringBuf(*data) + regen;
                        part.Offset = offset;
                        part.Size += prefixLen;
                    } else {
                        STLOGX(GetActorContext(), PRI_CRIT, BS_VDISK_SCRUB, VDS38, VDISKP(LogPrefix, "index is corrupt and can't be restored"),
                            (SstId, sst->AssignedSstId));
                        Success = false;
                        return;
                    }
                }

                if (regen) {
                    Write(part, regen);
                    Checkpoints |= TEvScrubNotify::INDEX_RESTORED;
                }
            } else {
                Y_VERIFY_S(regen.size() == data->size(), LogPrefix << "index size differs from one stored in memory");
                const size_t headerLen = first ? sizeof(TIdxDiskPlaceHolder) : sizeof(TIdxDiskLinker);
                Y_VERIFY_S(memcmp(regen.data(), data->data(), part.Size - headerLen) == 0,
                    LogPrefix << "index data differs from one stored in memory"); // compare index data up to header
                auto compare = [&](auto a, auto b) {
                    Y_VERIFY_S(sizeof(a) == headerLen && sizeof(b) == headerLen, LogPrefix);
                    memcpy(&a, regen.data() + regen.size() - headerLen, headerLen); // to prevent unaligned access
                    memcpy(&b, data->data() + data->size() - headerLen, headerLen);
                    Y_VERIFY_S(a == b, LogPrefix << "index header differs from one stored in memory");
                };
                if (first) {
                    compare(TIdxDiskPlaceHolder(0), TIdxDiskPlaceHolder(0));
                } else {
                    compare(TIdxDiskLinker(), TIdxDiskLinker());
                }
            }

            prevPart = part;
            first = false;
        }
        Y_VERIFY_S(!remainOutboundSize && !remainIndexSize, LogPrefix);
    }

    void TScrubCoroImpl::ReadOutSelectedBlobs(std::vector<TBlobOnDisk>&& blobsOnDisk) {
        STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS14, VDISKP(LogPrefix, "reading out SST"), (SstId, SstId),
            (NumBlobs, blobsOnDisk.size()));

        // scan all the blobs and sort them out -- huge blobs can be checked directly by reading them, small blobs
        // are split into chunks with intervals
        std::map<TChunkIdx, std::vector<TBlobOnDisk*>> chunks;
        for (TBlobOnDisk& blob : blobsOnDisk) {
            chunks[blob.Part.ChunkIdx].push_back(&blob);
        }
        for (auto& [chunkIdx, blobs] : chunks) {
            std::sort(blobs.begin(), blobs.end(), [](const auto *x, const auto *y) { return x->Part.Offset < y->Part.Offset; });
        }

        // scan small blob chunks
        struct TBlobToCheck {
            TLogoBlobID Id;
            NMatrix::TVectorType Needed;
            TDiskPart CorruptedPart;
        };
        std::vector<TBlobOnDisk*> pendingBlobs;
        std::vector<TBlobToCheck> blobsToCheck;
        for (const auto& [chunkIdx, blobs] : chunks) {
            const auto chunkIdx_{chunkIdx};
            STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS08, VDISKP(LogPrefix, "reading out chunk"), (SstId, SstId),
                (ChunkIdx, chunkIdx_));
            TDiskPart interval;
            auto doCheck = [&] {
                if (interval != TDiskPart()) {
                    const bool intervalReadable = IsReadable(interval);
                    STLOGX(GetActorContext(), intervalReadable ? PRI_DEBUG : PRI_ERROR, BS_VDISK_SCRUB, VDS04,
                        VDISKP(LogPrefix, "small blob interval checked"), (Interval, interval),
                        (IsReadable, intervalReadable), (NumBlobsOfInterest, pendingBlobs.size()));
                    ++MonGroup.SmallBlobIntervalsRead();
                    MonGroup.SmallBlobIntervalBytesRead() += interval.Size;

                    for (TBlobOnDisk *blob : pendingBlobs) {
                        const bool blobReadable = intervalReadable || IsReadable(blob->Part);
                        if (!intervalReadable) {
                            STLOGX(GetActorContext(), blobReadable ? PRI_INFO : PRI_ERROR, BS_VDISK_SCRUB, VDS12,
                                VDISKP(LogPrefix, "small blob from unreadable interval checked"),
                                (Key, blob->Id), (Location, blob->Part), (IsReadable, blobReadable));
                            ++MonGroup.SmallBlobsRead();
                            MonGroup.SmallBlobBytesRead() += interval.Size;
                        }
                        if (blobReadable) {
                            UpdateReadableParts(blob->Id, blob->Local);
                        } else {
                            blobsToCheck.push_back({blob->Id, blob->Local, blob->Part});
                            Checkpoints |= TEvScrubNotify::SMALL_BLOB_SCRUBBED;
                        }
                    }
                }
                pendingBlobs.clear();
            };
            if (ScrubCtx->EnableDeepScrubbing) {
                for (TBlobOnDisk *blob : blobs) {
                    CheckIntegrity(blob->Id, false);
                    UpdateReadableParts(blob->Id, blob->Local);
                }
            } else {
                for (TBlobOnDisk *blob : blobs) {
                    const TDiskPart& part = blob->Part;
                    const ui32 end = part.Offset + part.Size;
                    Y_VERIFY_S(part.ChunkIdx == chunkIdx, LogPrefix);
                    if (interval == TDiskPart()) {
                        interval = blob->Part;
                    } else if (end - interval.Offset <= ScrubCtx->PDiskCtx->Dsk->ReadBlockSize) {
                        interval.Size = end - interval.Offset;
                    } else {
                        doCheck();
                        interval = blob->Part;
                    }
                    pendingBlobs.push_back(blob);
                }
                doCheck();
            }
        }

        if (blobsToCheck.empty()) {
            return;
        }

        // find unreadable blobs which can't be read from other places
        TLevelIndexSnapshot::TForwardIterator iter(Snap->HullCtx, &Snap->LogoBlobsSnap);
        TBlobLocationExtractorMerger merger(Info->Type);
        for (const TBlobToCheck& blob : blobsToCheck) {
            iter.Seek(blob.Id);
            if (iter.Valid() && iter.GetCurKey() == blob.Id) {
                iter.PutToMerger(&merger);

                NMatrix::TVectorType needed = blob.Needed;
                Y_VERIFY_S(!needed.Empty(), LogPrefix);

                STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS11, VDISKP(LogPrefix, "reading out blob"), (SstId, SstId),
                    (Id, blob.Id));

                for (const TBlobOnDisk& replica : merger.BlobsOnDisk) {
                    if (!(replica.Local & needed).Empty()) {
                        const bool blobReadable = IsReadable(replica.Part);
                        STLOGX(GetActorContext(), blobReadable ? PRI_DEBUG : PRI_ERROR, BS_VDISK_SCRUB, VDS16,
                            VDISKP(LogPrefix, "read replica"), (SstId, SstId), (Id, blob.Id), (Location, replica.Part),
                            (Local, replica.Local), (IsReadable, blobReadable));
                        if (blobReadable) {
                            needed &= ~replica.Local;
                        }
                    }
                }

                UpdateUnreadableParts(blob.Id, needed, blob.CorruptedPart);
                merger.Clear();
            }
        }
    }

} // NKikimr
