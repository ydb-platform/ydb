#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_local.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NPQ::NDeferredPublish {

struct TEvDeferredPublish {
    enum EEv : ui32 {
        EvBeginPublicationRequest = EventSpaceBegin(TKikimrEvents::ES_PQ_DEFERRED_PUBLISH),
        EvBeginPublicationResponse,
        EvTablesCreationFinished,
        EvInsertPublicationFinished,
    };

    struct TEvBeginPublicationRequest : public NActors::TEventLocal<TEvBeginPublicationRequest, EvBeginPublicationRequest> {
        TString Database;
        TString ExtPublicationId;
        TMaybe<TString> WriterIdentity;
        TString CreatedBy;
    };

    struct TEvBeginPublicationResponse : public NActors::TEventLocal<TEvBeginPublicationResponse, EvBeginPublicationResponse> {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
        ui64 IntPublicationId = 0;
    };

    struct TEvTablesCreationFinished : public NActors::TEventLocal<TEvTablesCreationFinished, EvTablesCreationFinished> {
        TString Database;
        bool Success = false;
        NYql::TIssues Issues;
    };

    struct TEvInsertPublicationFinished : public NActors::TEventLocal<TEvInsertPublicationFinished, EvInsertPublicationFinished> {
        NActors::TActorId ReplyTo;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
        ui64 IntPublicationId = 0;
    };
};

using TEvBeginPublicationRequest = TEvDeferredPublish::TEvBeginPublicationRequest;
using TEvBeginPublicationResponse = TEvDeferredPublish::TEvBeginPublicationResponse;
using TEvTablesCreationFinished = TEvDeferredPublish::TEvTablesCreationFinished;
using TEvInsertPublicationFinished = TEvDeferredPublish::TEvInsertPublicationFinished;

} // namespace NKikimr::NPQ::NDeferredPublish
