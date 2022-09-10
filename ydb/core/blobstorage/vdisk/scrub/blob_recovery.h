#pragma once

#include "defs.h"

namespace NKikimr {

    struct TEvRecoverBlob : TEventLocal<TEvRecoverBlob, TEvBlobStorage::EvRecoverBlob> {
        struct TItem {
            TLogoBlobID BlobId;
            TDataPartSet PartSet; // available data
            NMatrix::TVectorType Needed; // needed parts
            TDiskPart CorruptedPart;
            ui64 Cookie;

            TItem(const TItem&) = default;
            TItem(TItem&&) = default;

            TItem(TLogoBlobID blobId, TDataPartSet&& partSet, NMatrix::TVectorType needed, TDiskPart corruptedPart, ui64 cookie = 0)
                : BlobId(blobId)
                , PartSet(std::move(partSet))
                , Needed(needed)
                , CorruptedPart(corruptedPart)
                , Cookie(cookie)
            {}

            TItem(TLogoBlobID blobId, NMatrix::TVectorType needed, const TBlobStorageGroupType& gtype, TDiskPart corruptedPart, ui64 cookie = 0)
                : BlobId(blobId)
                , PartSet{blobId.BlobSize(), 0, {gtype.TotalPartCount(), TPartFragment()}, TPartFragment(), 0u, false}
                , Needed(needed)
                , CorruptedPart(corruptedPart)
                , Cookie(cookie)
            {}

            void SetPartData(TLogoBlobID id, TString data) {
                Y_VERIFY(id.FullID() == BlobId);
                Y_VERIFY(id.PartId());
                const ui32 partIdx = id.PartId() - 1;
                if (PartSet.PartsMask & (1 << partIdx)) {
                    Y_VERIFY(GetPartData(id) == data);
                } else {
                    PartSet.PartsMask |= 1 << partIdx;
                    PartSet.Parts[partIdx].ReferenceTo(data);
                }
            }

            TString GetPartData(TLogoBlobID id) const {
                Y_VERIFY(id.FullID() == BlobId);
                Y_VERIFY(id.PartId());
                const ui32 partIdx = id.PartId() - 1;
                Y_VERIFY(PartSet.PartsMask & (1 << partIdx));
                return PartSet.Parts[partIdx].OwnedString.ConvertToString();
            }

            NMatrix::TVectorType GetAvailableParts() const {
                NMatrix::TVectorType res(0, Needed.GetSize());
                for (size_t i = 0; i < PartSet.Parts.size(); ++i) {
                    if (PartSet.PartsMask & (1 << i)) {
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
                    << item.Needed.ToString() << " PartsMask# " << Sprintf("%02" PRIx32, item.PartSet.PartsMask) << "}";
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
