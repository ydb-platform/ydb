#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/scheme_board_mon.pb.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/json/json_value.h>

namespace NKikimr {

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

    struct TEvBackupProgress: public TEventLocal<TEvBackupProgress, EvBackupProgress> {
        ui32 TotalPaths = 0;
        ui32 CompletedPaths = 0;

        explicit TEvBackupProgress(ui32 totalPaths, ui32 completedPaths)
            : TotalPaths(totalPaths)
            , CompletedPaths(completedPaths)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " CompletedPaths: " << CompletedPaths
                << " TotalPaths: " << TotalPaths
            << " }";
        }
    };

    struct TEvBackupResult: public TEventLocal<TEvBackupResult, EvBackupResult> {
        TMaybe<TString> Error = Nothing();

        TEvBackupResult() = default;

        explicit TEvBackupResult(TString error)
            : Error(std::move(error))
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " " << (Error.Defined() ? *Error : "Success")
            << " }";
        }
    };

    struct TEvRestoreProgress : public TEventLocal<TEvRestoreProgress, EvRestoreProgress> {
        ui32 TotalPaths = 0;
        ui32 ProcessedPaths = 0;

        TEvRestoreProgress(ui32 total, ui32 processed)
            : TotalPaths(total)
            , ProcessedPaths(processed)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " ProcessedPaths: " << ProcessedPaths
                << " TotalPaths: " << TotalPaths
            << " }";
        }
    };

    struct TEvRestoreResult : public TEventLocal<TEvRestoreResult, EvRestoreResult> {
        TMaybe<TString> Error = Nothing();

        TEvRestoreResult() = default;

        explicit TEvRestoreResult(const TString& error)
            : Error(error)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " " << (Error.Defined() ? *Error : "Success")
            << " }";
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
