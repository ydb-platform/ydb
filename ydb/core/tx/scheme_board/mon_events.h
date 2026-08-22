#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/scheme_board_mon.pb.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/json/json_value.h>

namespace NKikimr {

struct TEvCommonProgress {
    ui32 TotalPaths = 0;
    ui32 ProcessedPaths = 0;

    explicit TEvCommonProgress(ui32 totalPaths, ui32 processedPaths)
        : TotalPaths(totalPaths)
        , ProcessedPaths(processedPaths)
    {
    }

    TString ToString(const TString& header) const {
        return TStringBuilder() << header << " {"
            << " ProcessedPaths: " << ProcessedPaths
            << " TotalPaths: " << TotalPaths
        << " }";
    }
};

struct TEvCommonResult {
    TMaybe<TString> Error = Nothing();
    TMaybe<TString> Warning = Nothing();

    TEvCommonResult() = default;

    explicit TEvCommonResult(TString error)
        : Error(std::move(error))
    {
    }

    TEvCommonResult(TMaybe<TString> error, TMaybe<TString> warning)
        : Error(std::move(error))
        , Warning(std::move(warning))
    {}

    TString ToString(const TString& header) const {
        return TStringBuilder() << header << " {"
            << " " << (Error ? *Error : "Success")
        << " }";
    }
};

struct TSchemeBoardMonEvents {
    enum EEv {
        EvRegister = EventSpaceBegin(TKikimrEvents::ES_SCHEME_BOARD_MON),
        EvUnregister,

        EvInfoRequest,
        EvInfoResponse,

        EvDescribeRequest,
        EvDescribeResponse,

        EvBackupProgress,
        EvBackupResult,
        EvRestoreProgress,
        EvRestoreResult,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SCHEME_BOARD_MON), "expect End < EventSpaceEnd(ES_SCHEME_BOARD_MON)");

    struct TEvRegister: public TEventLocal<TEvRegister, EvRegister> {
        const NKikimrServices::TActivity::EType ActivityType;
        const NJson::TJsonMap Attributes;

        explicit TEvRegister(
                NKikimrServices::TActivity::EType activityType,
                const NJson::TJsonMap& attributes)
            : ActivityType(activityType)
            , Attributes(attributes)
        {
        }
    };

    struct TEvUnregister: public TEventLocal<TEvUnregister, EvUnregister> {
    };

    struct TEvBackupProgress: public TEventLocal<TEvBackupProgress, EvBackupProgress>, TEvCommonProgress {
        using TEvCommonProgress::TEvCommonProgress;

        TString ToString() const override {
            return TEvCommonProgress::ToString(ToStringHeader());
        }
    };

    struct TEvBackupResult: public TEventLocal<TEvBackupResult, EvBackupResult>, TEvCommonResult {
        using TEvCommonResult::TEvCommonResult;

        TString ToString() const override {
            return TEvCommonResult::ToString(ToStringHeader());
        }
    };

    struct TEvRestoreProgress: public TEventLocal<TEvRestoreProgress, EvRestoreProgress>, TEvCommonProgress {
        using TEvCommonProgress::TEvCommonProgress;

        TString ToString() const override {
            return TEvCommonProgress::ToString(ToStringHeader());
        }
    };

    struct TEvRestoreResult: public TEventLocal<TEvRestoreResult, EvRestoreResult>, TEvCommonResult {
        using TEvCommonResult::TEvCommonResult;

        TString ToString() const override {
            return TEvCommonResult::ToString(ToStringHeader());
        }
    };

    struct TEvInfoRequest: public TEventPB<TEvInfoRequest, NKikimrSchemeBoardMon::TEvInfoRequest, EvInfoRequest> {
        TEvInfoRequest() = default;

        explicit TEvInfoRequest(ui32 limit) {
            Record.SetLimitRepeatedFields(limit);
        }
    };

    struct TEvInfoResponse: public TEventPB<TEvInfoResponse, NKikimrSchemeBoardMon::TEvInfoResponse, EvInfoResponse> {
        TEvInfoResponse() = default;

        explicit TEvInfoResponse(const TActorId& self, const TString& activityType) {
            ActorIdToProto(self, Record.MutableSelf());
            Record.SetActivityType(activityType);
        }

        explicit TEvInfoResponse(const TActorId& self, NKikimrServices::TActivity::EType activityType)
            : TEvInfoResponse(self, NKikimrServices::TActivity::EType_Name(activityType))
        {
        }

        void SetTruncated(bool value = true) {
            Record.SetTruncated(value);
        }
    };

    struct TEvDescribeRequest: public TEventPB<TEvDescribeRequest, NKikimrSchemeBoardMon::TEvDescribeRequest, EvDescribeRequest> {
        TEvDescribeRequest() = default;

        explicit TEvDescribeRequest(const TString& path) {
            Record.SetPath(path);
        }

        explicit TEvDescribeRequest(const TPathId& pathId) {
            Record.MutablePathId()->SetOwnerId(pathId.OwnerId);
            Record.MutablePathId()->SetLocalPathId(pathId.LocalPathId);
        }
    };

    struct TEvDescribeResponse: public TEventPB<TEvDescribeResponse, NKikimrSchemeBoardMon::TEvDescribeResponse, EvDescribeResponse> {
        TEvDescribeResponse() = default;

        explicit TEvDescribeResponse(const TString& json) {
            Record.SetJson(json);
        }
    };

}; // TSchemeBoardMonEvents

} // NKikimr
