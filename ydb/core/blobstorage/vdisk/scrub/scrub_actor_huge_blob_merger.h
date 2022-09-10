#pragma once

#include "defs.h"
#include "scrub_actor_impl.h"

namespace NKikimr {

    class TScrubCoroImpl::THugeBlobMerger {
        const TString& LogPrefix;
        NMatrix::TVectorType Local;
        NMatrix::TVectorType ReadableLocal;
        std::vector<TDiskPart> CorruptedParts;
        std::function<std::optional<TString>(const TDiskPart&)> Read;
        const TBlobStorageGroupType GType;
        TDataPartSet PartSet;
        TScrubCoroImpl *Impl;

    public:
        template<typename TRead>
        THugeBlobMerger(const TString& logPrefix, const TBlobStorageGroupType& gtype, TRead&& read, TScrubCoroImpl *impl)
            : LogPrefix(logPrefix)
            , Local(0, gtype.TotalPartCount())
            , ReadableLocal(0, gtype.TotalPartCount())
            , Read(std::move(read))
            , GType(gtype)
            , Impl(impl)
        {
            PartSet.FullDataSize = 0;
            PartSet.PartsMask = 0;
            PartSet.Parts.resize(gtype.TotalPartCount());
        }

        static bool HaveToMergeData() { return true; }

        void Begin(const TLogoBlobID& id) {
            PartSet.FullDataSize = id.BlobSize();
        }

        // process on-disk data
        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key, ui64 /*sstId*/) {
            switch (memRec.GetType()) {
                // ignore non-huge blobs
                case TBlobType::MemBlob:
                case TBlobType::DiskBlob:
                    break;

                case TBlobType::HugeBlob:
                case TBlobType::ManyHugeBlobs: {
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, outbound);
                    const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
                    Y_VERIFY(extr.End - extr.Begin == local.CountBits());
                    const TDiskPart *part = extr.Begin;
                    for (ui32 i = local.FirstPosition(); i != local.GetSize(); i = local.NextPosition(i), ++part) {
                        if (part->ChunkIdx && part->Size) {
                            std::optional<TString> data = Read(*part);
                            STLOGX(Impl->GetActorContext(), data ? PRI_DEBUG : PRI_ERROR, BS_VDISK_SCRUB, VDS21,
                                VDISKP(LogPrefix, "huge blob read"), (Id, key.LogoBlobID()), (Local, local),
                                (Location, *part), (IsReadable, data.has_value()));
                            Local.Set(i);
                            if (data) {
                                ReadableLocal.Set(i);
                                TRope rope(*data);
                                TDiskBlob blob(&rope, NMatrix::TVectorType::MakeOneHot(i, local.GetSize()), GType,
                                    key.LogoBlobID());
                                TRope holder;
                                TRope part = blob.GetPart(i, &holder);
                                PartSet.Parts[i].ReferenceTo(part);
                            } else {
                                CorruptedParts.push_back(*part);
                            }
                        }
                    }
                    break;
                }
            }
        }

        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* /*data*/, const TKeyLogoBlob& key, ui64 /*lsn*/) {
            AddFromSegment(memRec, nullptr, key, Max<ui64>());
        }

        void Clear() {
            Local.Clear();
            ReadableLocal.Clear();
            CorruptedParts.clear();
            PartSet.FullDataSize = 0;
            PartSet.PartsMask = 0;
            std::fill(PartSet.Parts.begin(), PartSet.Parts.end(), TPartFragment());
        }

        NMatrix::TVectorType GetPartsToRestore() const {
            return Local & ~ReadableLocal;
        }

        TDiskPart GetCorruptedPart() const {
            return CorruptedParts.empty() ? TDiskPart() : CorruptedParts.front();
        }

        TDataPartSet GetPartSet() {
            return std::move(PartSet);
        }
    };

} // NKikimr
