#pragma once

#include "defs.h"

#include <ydb/core/protos/set_column_constraint.pb.h>

namespace NKikimr {
namespace NSchemeShard {

TString SerializeSetColumnConstraintColumnNames(const std::vector<TString>& columns);
std::vector<TString> DeserializeSetColumnConstraintColumnNames(const TString& serialized);


struct TEvSetColumnConstraint {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_SET_COLUMN_CONSTRAINT),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,

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
}; // TEvSetColumnConstraint

} // NSchemeShard
} // NKikimr

