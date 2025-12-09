#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {
    using TCompactionTokenId = ui64;

    struct TEvCompactionTokenRequest : public TEventLocal<TEvCompactionTokenRequest, TEvBlobStorage::EvCompactionTokenRequest> {
        TString PDiskId;
        TVDiskIdShort VDiskId;
        double Ratio;

        TEvCompactionTokenRequest(const TString& pdiskId, const TVDiskIdShort& vdiskId, double ratio)
            : PDiskId(pdiskId), VDiskId(vdiskId), Ratio(ratio) {}

        TString ToString() const {
            TStringStream str;
            str << "{EvCompactionTokenRequest PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " Ratio# " << Ratio << "}";
            return str.Str();
        }
    };

    struct TEvUpdateCompactionTokenRequest : public TEventLocal<TEvUpdateCompactionTokenRequest, TEvBlobStorage::EvUpdateCompactionTokenRequest> {
        TString PDiskId;
        TVDiskIdShort VDiskId;
        double Ratio;

        TEvUpdateCompactionTokenRequest(const TString& pdiskId, const TVDiskIdShort& vdiskId, double ratio)
            : PDiskId(pdiskId), VDiskId(vdiskId), Ratio(ratio) {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvUpdateCompactionTokenRequest PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " Ratio# " << Ratio << "}";
            return str.Str();
        }
    };

    struct TEvCompactionTokenResult : TEventLocal<TEvCompactionTokenResult, TEvBlobStorage::EvCompactionTokenResult> {
        TCompactionTokenId Token;

        TEvCompactionTokenResult(const TCompactionTokenId& token) : Token(token) {}

        TString ToString() const {
            TStringStream str;
            str << "{EvCompactionTokenResult Token# " << Token << "}";
            return str.Str();
        }
    };

    struct TEvReleaseCompactionToken : TEventLocal<TEvReleaseCompactionToken, TEvBlobStorage::EvReleaseCompactionToken> {
        TString PDiskId;
        TVDiskIdShort VDiskId;
        TCompactionTokenId Token;
        TEvReleaseCompactionToken(const TString& pdiskId, const TVDiskIdShort& vdiskId, TCompactionTokenId token) : PDiskId(pdiskId), VDiskId(vdiskId), Token(token) {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvReleaseCompactionToken PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " Token# " << Token << "}";
            return str.Str();
        }
    };

    extern IActor *CreateCompBrokerActor();

} // NKikimr
