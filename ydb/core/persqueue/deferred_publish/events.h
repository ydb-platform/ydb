#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_local.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/datetime/base.h>

namespace NKikimr::NPQ::NDeferredPublish {

struct TPublicationSummary {
    ui64 IntPublicationId = 0;
    TString ExtPublicationId;
    TMaybe<TString> WriterIdentity;
};

struct TDescribeDestination {
    TString TopicPath;
    TVector<i64> PartitionIds;
};

struct TDescribePublicationData {
    TString ExtPublicationId;
    TMaybe<TString> WriterIdentity;
    TInstant CreatedAt;
    TMaybe<TString> CreatedBy;
    TVector<TDescribeDestination> Destinations;
};

struct TEvDeferredPublish {
    enum EEv : ui32 {
        EvBeginPublicationRequest = EventSpaceBegin(TKikimrEvents::ES_PQ_DEFERRED_PUBLISH),
        EvBeginPublicationResponse,
        EvTablesCreationFinished,
        EvInsertPublicationFinished,
        EvListPublicationsResponse,
        EvDescribePublicationResponse,
        EvUpsertDestinationResponse,
        EvDeletePublicationResponse,
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

    struct TEvListPublicationsResponse : public NActors::TEventLocal<TEvListPublicationsResponse, EvListPublicationsResponse> {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
        TVector<TPublicationSummary> Publications;
    };

    struct TEvDescribePublicationResponse : public NActors::TEventLocal<TEvDescribePublicationResponse, EvDescribePublicationResponse> {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
        TMaybe<TDescribePublicationData> Publication;
    };

    struct TEvUpsertDestinationResponse : public NActors::TEventLocal<TEvUpsertDestinationResponse, EvUpsertDestinationResponse> {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
    };

    struct TEvDeletePublicationResponse : public NActors::TEventLocal<TEvDeletePublicationResponse, EvDeletePublicationResponse> {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
    };
};

using TEvBeginPublicationRequest = TEvDeferredPublish::TEvBeginPublicationRequest;
using TEvBeginPublicationResponse = TEvDeferredPublish::TEvBeginPublicationResponse;
using TEvTablesCreationFinished = TEvDeferredPublish::TEvTablesCreationFinished;
using TEvInsertPublicationFinished = TEvDeferredPublish::TEvInsertPublicationFinished;
using TEvListPublicationsResponse = TEvDeferredPublish::TEvListPublicationsResponse;
using TEvDescribePublicationResponse = TEvDeferredPublish::TEvDescribePublicationResponse;
using TEvUpsertDestinationResponse = TEvDeferredPublish::TEvUpsertDestinationResponse;
using TEvDeletePublicationResponse = TEvDeferredPublish::TEvDeletePublicationResponse;

} // namespace NKikimr::NPQ::NDeferredPublish
