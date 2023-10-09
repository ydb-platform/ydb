#pragma once

#include "defs.h"

namespace NKikimr {

    struct TEvRecoverBlob : TEventLocal<TEvRecoverBlob, TEvBlobStorage::EvRecoverBlob> {
        struct TItem {
            TLogoBlobID BlobId;
            TStackVec<TRope, 8> Parts;
            ui32 PartsMask = 0;
            NMatrix::TVectorType Needed; // needed parts
            TDiskPart CorruptedPart;
            ui64 Cookie;

            TItem(const TItem&) = default;
            TItem(TItem&&) = default;

            TItem(TLogoBlobID blobId, TStackVec<TRope, 8>&& parts, ui32 partsMask, NMatrix::TVectorType needed, TDiskPart corruptedPart, ui64 cookie = 0)
                : BlobId(blobId)
                , Parts(std::move(parts))
                , PartsMask(partsMask)
                , Needed(needed)
                , CorruptedPart(corruptedPart)
                , Cookie(cookie)
            {}

            TItem(TLogoBlobID blobId, NMatrix::TVectorType needed, const TBlobStorageGroupType& gtype, TDiskPart corruptedPart, ui64 cookie = 0)
                : BlobId(blobId)
                , Parts(gtype.TotalPartCount())
                , PartsMask(0)
                , Needed(needed)
                , CorruptedPart(corruptedPart)
                , Cookie(cookie)
            {}

            void SetPartData(TLogoBlobID id, TRope&& data) {
                Y_ABORT_UNLESS(id.FullID() == BlobId);
                Y_ABORT_UNLESS(id.PartId());
                const ui32 partIdx = id.PartId() - 1;
                if (PartsMask & (1 << partIdx)) {
                    Y_ABORT_UNLESS(GetPartData(id) == data);
                } else {
                    PartsMask |= 1 << partIdx;
                    Parts[partIdx] = std::move(data);
                }
            }

            const TRope& GetPartData(TLogoBlobID id) const {
                Y_ABORT_UNLESS(id.FullID() == BlobId);
                Y_ABORT_UNLESS(id.PartId());
                const ui32 partIdx = id.PartId() - 1;
                Y_ABORT_UNLESS(PartsMask & (1 << partIdx));
                return Parts[partIdx];
            }

            NMatrix::TVectorType GetAvailableParts() const {
                NMatrix::TVectorType res(0, Needed.GetSize());
                for (size_t i = 0; i < Parts.size(); ++i) {
                    if (PartsMask & (1 << i)) {
                        res.Set(i);
                    }
                }
                return res;
            }
        };
        TInstant Deadline;
        std::deque<TItem> Items;

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }

        void Output(IOutputStream& s) const {
            s << "{Deadline# " << Deadline << " Items# [";
            bool first = true;
            for (const TItem& item : Items) {
                s << (std::exchange(first, false) ? "" : " ") << "{BlobId# " << item.BlobId.ToString() << " Needed# "
                    << item.Needed.ToString() << " PartsMask# " << Sprintf("%02" PRIx32, item.PartsMask) << "}";
            }
            s << "]}";
        }
    };

    struct TEvRecoverBlobResult : TEventLocal<TEvRecoverBlobResult, TEvBlobStorage::EvRecoverBlobResult> {
        struct TItem : TEvRecoverBlob::TItem {
            NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;

            TItem(TEvRecoverBlob::TItem&& item)
                : TEvRecoverBlob::TItem(std::move(item))
            {}
        };
        TInstant Deadline;
        std::deque<TItem> Items;
    };

    IActor *CreateBlobRecoveryActor(TIntrusivePtr<TVDiskContext> vctx, TIntrusivePtr<TBlobStorageGroupInfo> info,
            ::NMonitoring::TDynamicCounterPtr counters);

} // NKikimr
