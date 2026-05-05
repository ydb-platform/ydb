#pragma once

#include "defs.h"

#include <ydb/core/protos/forced_compaction.pb.h>

namespace NKikimr::NSchemeShard {

struct TEvForcedCompaction {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_FORCED_COMPACTION),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,
        EvCancelRequest,
        EvCancelResponse,
        EvForgetRequest,
        EvForgetResponse,
        EvListRequest,
        EvListResponse,

        EvEnd
    };

    struct TEvCreateRequest: public TEventPB<TEvCreateRequest, NKikimrForcedCompaction::TEvCreateRequest, EvCreateRequest> {
        TEvCreateRequest() = default;

        explicit TEvCreateRequest(
            const ui64 txId,
            const TString& dbName,
            NKikimrForcedCompaction::TForcedCompactionSettings settings)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            *Record.MutableSettings() = std::move(settings);
        }
    };

    struct TEvCreateResponse: public TEventPB<TEvCreateResponse, NKikimrForcedCompaction::TEvCreateResponse, EvCreateResponse> {
        TEvCreateResponse() = default;

        explicit TEvCreateResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvGetRequest: public TEventPB<TEvGetRequest, NKikimrForcedCompaction::TEvGetRequest, EvGetRequest> {
        TEvGetRequest() = default;

        explicit TEvGetRequest(const TString& dbName, const ui64 forcedCompactionId) {
            Record.SetDatabaseName(dbName);
            Record.SetForcedCompactionId(forcedCompactionId);
        }
    };

    struct TEvGetResponse: public TEventPB<TEvGetResponse, NKikimrForcedCompaction::TEvGetResponse, EvGetResponse> {
        TEvGetResponse() = default;
    };

    struct TEvCancelRequest: public TEventPB<TEvCancelRequest, NKikimrForcedCompaction::TEvCancelRequest, EvCancelRequest> {
        TEvCancelRequest() = default;

        explicit TEvCancelRequest(
            const ui64 txId,
            const TString& dbName,
            ui64 forcedCompactionId)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.SetForcedCompactionId(forcedCompactionId);
        }
    };

    struct TEvCancelResponse: public TEventPB<TEvCancelResponse, NKikimrForcedCompaction::TEvCancelResponse, EvCancelResponse> {
        TEvCancelResponse() = default;

        explicit TEvCancelResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvForgetRequest: public TEventPB<TEvForgetRequest, NKikimrForcedCompaction::TEvForgetRequest, EvForgetRequest> {
        TEvForgetRequest() = default;

        explicit TEvForgetRequest(
            const ui64 txId,
            const TString& dbName,
            ui64 forcedCompactionId)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.SetForcedCompactionId(forcedCompactionId);
        }
    };

    struct TEvForgetResponse: public TEventPB<TEvForgetResponse, NKikimrForcedCompaction::TEvForgetResponse, EvForgetResponse> {
        TEvForgetResponse() = default;

        explicit TEvForgetResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvListRequest: public TEventPB<TEvListRequest, NKikimrForcedCompaction::TEvListRequest, EvListRequest> {
        TEvListRequest() = default;

        explicit TEvListRequest(const TString& dbName, ui64 pageSize, TString pageToken) {
            Record.SetDatabaseName(dbName);
            Record.SetPageSize(pageSize);
            Record.SetPageToken(pageToken);
        }
    };

    struct TEvListResponse: public TEventPB<TEvListResponse, NKikimrForcedCompaction::TEvListResponse, EvListResponse> {
        TEvListResponse() = default;
    };

};

} // namespace NKikimr::NSchemeShard
