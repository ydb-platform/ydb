#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

namespace NKikimr {
    using TCompactionTokenId = ui64;

    struct TEvCompactionTokenRequest : public TEventLocal<TEvCompactionTokenRequest, TEvBlobStorage::EvCompactionTokenRequest> {
        TString PDiskId;
        TString VDiskId;
        double Ratio;

        TEvCompactionTokenRequest(const TString& pdiskId, const TString& vdiskId, double ratio)
            : PDiskId(pdiskId), VDiskId(vdiskId), Ratio(ratio) {}

        TString ToString() const {
            TStringStream str;
            str << "{EvCompactionTokenRequest PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " Ratio# " << Ratio << "}";
            return str.Str();
        }
    };

    struct TEvUpdateCompactionTokenRequest : public TEventLocal<TEvUpdateCompactionTokenRequest, TEvBlobStorage::EvUpdateCompactionTokenRequest> {
        TString PDiskId;
        TString VDiskId;
        double Ratio;

        TEvUpdateCompactionTokenRequest(const TString& pdiskId, const TString& vdiskId, double ratio)
            : PDiskId(pdiskId), VDiskId(vdiskId), Ratio(ratio) {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvUpdateCompactionTokenRequest PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " Ratio# " << Ratio << "}";
            return str.Str();
        }
    };

    struct TEvCompactionTokenResult : TEventLocal<TEvCompactionTokenResult, TEvBlobStorage::EvCompactionTokenResult> {
        TCompactionTokenId Token;
        TString VDiskId;

        TEvCompactionTokenResult(const TCompactionTokenId& token, const TString& vdiskId) : Token(token), VDiskId(vdiskId) {}

        TString ToString() const {
            TStringStream str;
            str << "{EvCompactionTokenResult Token# " << Token << " VDiskId# " << VDiskId << "}";
            return str.Str();
        }
    };

    struct TEvReleaseCompactionToken : TEventLocal<TEvReleaseCompactionToken, TEvBlobStorage::EvReleaseCompactionToken> {
        TString PDiskId;
        TString VDiskId;
        TCompactionTokenId Token;
        TEvReleaseCompactionToken(const TString& pdiskId, const TString& vdiskId, TCompactionTokenId token) : PDiskId(pdiskId), VDiskId(vdiskId), Token(token) {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvReleaseCompactionToken PDiskId# " << PDiskId << " VDiskId# " << VDiskId << " Token# " << Token << "}";
            return str.Str();
        }
    };

    extern IActor *CreateCompBrokerActor(
        const TControlWrapper& maxActiveCompactionsPerPDisk,
        const TControlWrapper& longWaitingThresholdSec,
        const TControlWrapper& longWorkingThresholdSec,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

} // NKikimr
