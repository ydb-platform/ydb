#pragma once

#include "defs.h"

#include <ydb/core/protos/set_column_constraint.pb.h>

namespace NKikimr {
namespace NSchemeShard {

// Forward declarations
class TSchemeShard;
struct TSetColumnConstraintOperationInfo;

TString SerializeSetColumnConstraintColumnNames(const std::vector<TString>& columns);
std::vector<TString> DeserializeSetColumnConstraintColumnNames(const TString& serialized);

// Common helper functions for filling proto from operation info
float CalcSetColumnConstraintValidationProgress(const TSetColumnConstraintOperationInfo& operationInfo);
void FillSetColumnConstraint(
    NKikimrSetColumnConstraint::TSetColumnConstraint& proto,
    const TSetColumnConstraintOperationInfo& operationInfo,
    TSchemeShard* self);


struct TEvSetColumnConstraint {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_SET_COLUMN_CONSTRAINT),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,
        EvListRequest,
        EvListResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_SET_COLUMN_CONSTRAINT),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_SET_COLUMN_CONSTRAINT)"
        );

    struct TEvCreateRequest: public TEventPB<TEvCreateRequest, NKikimrSetColumnConstraint::TEvCreateRequest, EvCreateRequest> {
        TEvCreateRequest() = default;

        explicit TEvCreateRequest(
            const ui64 txId,
            const TString& dbName,
            NKikimrSetColumnConstraint::TSetColumnConstraintSettings settings)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            *Record.MutableSettings() = std::move(settings);
        }
    };

    struct TEvCreateResponse: public TEventPB<TEvCreateResponse, NKikimrSetColumnConstraint::TEvCreateResponse, EvCreateResponse> {
        TEvCreateResponse() = default;

        explicit TEvCreateResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvGetRequest: public TEventPB<TEvGetRequest, NKikimrSetColumnConstraint::TEvGetRequest, EvGetRequest> {
        TEvGetRequest() = default;

        explicit TEvGetRequest(const TString& dbName, ui64 operationId) {
            Record.SetDatabaseName(dbName);
            Record.SetOperationId(operationId);
        }
    };

    struct TEvGetResponse: public TEventPB<TEvGetResponse, NKikimrSetColumnConstraint::TEvGetResponse, EvGetResponse> {};

    struct TEvListRequest: public TEventPB<TEvListRequest, NKikimrSetColumnConstraint::TEvListRequest, EvListRequest> {
        TEvListRequest() = default;

        explicit TEvListRequest(const TString& dbName, ui64 pageSize, const TString& pageToken) {
            Record.SetDatabaseName(dbName);
            Record.SetPageSize(pageSize);
            Record.SetPageToken(pageToken);
        }
    };

    struct TEvListResponse: public TEventPB<TEvListResponse, NKikimrSetColumnConstraint::TEvListResponse, EvListResponse> {
        TEvListResponse() = default;
    };
}; // TEvSetColumnConstraint

} // NSchemeShard
} // NKikimr

