#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

namespace NKikimr {
    using TCompactionTokenId = ui64;

    struct TEvCompactionTokenRequest : public TEventLocal<TEvCompactionTokenRequest, TEvBlobStorage::EvCompactionTokenRequest> {
        ui32 PDiskId;
        TGroupId GroupId;
        TVDiskIdShort VDiskId;
        double Ratio;

        TEvCompactionTokenRequest(ui32 pdiskId, const TGroupId& groupId, const TVDiskIdShort& vdiskId, double ratio)
            : PDiskId(pdiskId), GroupId(groupId), VDiskId(vdiskId), Ratio(ratio) {}

        TString ToString() const {
            TStringStream str;
            str << "{EvCompactionTokenRequest PDiskId# " << PDiskId << " GroupId# " << GroupId << " VDiskId# " << VDiskId.ToString() << " Ratio# " << Ratio << "}";
            return str.Str();
        }
    };

    struct TEvCompactionTokenResult : TEventLocal<TEvCompactionTokenResult, TEvBlobStorage::EvCompactionTokenResult> {
        TCompactionTokenId Token;
        TGroupId GroupId;
        TVDiskIdShort VDiskId;

        TEvCompactionTokenResult(const TCompactionTokenId& token, const TGroupId& groupId, const TVDiskIdShort& vdiskId) 
            : Token(token), GroupId(groupId), VDiskId(vdiskId) {}

        TString ToString() const {
            TStringStream str;
            str << "{EvCompactionTokenResult Token# " << Token << " GroupId# " << GroupId << " VDiskId# " << VDiskId.ToString() << "}";
            return str.Str();
        }
    };

    struct TEvReleaseCompactionToken : TEventLocal<TEvReleaseCompactionToken, TEvBlobStorage::EvReleaseCompactionToken> {
        ui32 PDiskId;
        TGroupId GroupId;
        TVDiskIdShort VDiskId;
        TCompactionTokenId Token;
        TEvReleaseCompactionToken(ui32 pdiskId, const TGroupId& groupId, const TVDiskIdShort& vdiskId, TCompactionTokenId token) 
            : PDiskId(pdiskId), GroupId(groupId), VDiskId(vdiskId), Token(token) {}

        TString ToString() const {
            TStringStream str;
            str << "{TEvReleaseCompactionToken PDiskId# " << PDiskId << " GroupId# " << GroupId << " VDiskId# " << VDiskId.ToString() << " Token# " << Token << "}";
            return str.Str();
        }
    };

    extern IActor *CreateCompBrokerActor(
        const TControlWrapper& maxActiveCompactionsPerPDisk,
        const TControlWrapper& longWaitingThresholdSec,
        const TControlWrapper& longWorkingThresholdSec,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

} // NKikimr
