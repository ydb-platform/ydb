#pragma once

#include "defs.h"

#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TEvIndexBuilder {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_INDEX_BUILD),
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

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_INDEX_BUILD),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_INDEX_BUILD)"
        );

    struct TEvCreateRequest: public TEventPB<TEvCreateRequest, NKikimrIndexBuilder::TEvCreateRequest, EvCreateRequest> {
        TEvCreateRequest() = default;

        explicit TEvCreateRequest(
            const ui64 txId,
            const TString& dbName,
            NKikimrIndexBuilder::TIndexBuildSettings settings)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            *Record.MutableSettings() = std::move(settings);
        }
    };

    struct TEvCreateResponse: public TEventPB<TEvCreateResponse, NKikimrIndexBuilder::TEvCreateResponse, EvCreateResponse> {
        TEvCreateResponse() = default;

        explicit TEvCreateResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvGetRequest: public TEventPB<TEvGetRequest, NKikimrIndexBuilder::TEvGetRequest, EvGetRequest> {

        TEvGetRequest() = default;

        explicit TEvGetRequest(const TString& dbName, ui64 buildIndexId) {
            Record.SetDatabaseName(dbName);
            Record.SetIndexBuildId(buildIndexId);
        }
    };

    struct TEvGetResponse: public TEventPB<TEvGetResponse, NKikimrIndexBuilder::TEvGetResponse, EvGetResponse> {
    };

    struct TEvCancelRequest: public TEventPB<TEvCancelRequest, NKikimrIndexBuilder::TEvCancelRequest, EvCancelRequest> {
        TEvCancelRequest() = default;

        explicit TEvCancelRequest(
            const ui64 txId,
            const TString& dbName,
            ui64 buildIndexId)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.SetIndexBuildId(buildIndexId);
        }
    };

    struct TEvCancelResponse: public TEventPB<TEvCancelResponse, NKikimrIndexBuilder::TEvCancelResponse, EvCancelResponse> {
        TEvCancelResponse() = default;

        explicit TEvCancelResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvForgetRequest: public TEventPB<TEvForgetRequest, NKikimrIndexBuilder::TEvForgetRequest, EvForgetRequest> {
        TEvForgetRequest() = default;

        explicit TEvForgetRequest(
            const ui64 txId,
            const TString& dbName,
            ui64 buildIndexId
            ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.SetIndexBuildId(buildIndexId);
        }
    };

    struct TEvForgetResponse: public TEventPB<TEvForgetResponse, NKikimrIndexBuilder::TEvForgetResponse, EvForgetResponse> {
        TEvForgetResponse() = default;

        explicit TEvForgetResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvListRequest: public TEventPB<TEvListRequest, NKikimrIndexBuilder::TEvListRequest, EvListRequest> {
        TEvListRequest() = default;

        explicit TEvListRequest(const TString& dbName, ui64 pageSize, TString pageToken) {
            Record.SetDatabaseName(dbName);
            Record.SetPageSize(pageSize);
            Record.SetPageToken(pageToken);
        }
    };

    struct TEvListResponse: public TEventPB<TEvListResponse, NKikimrIndexBuilder::TEvListResponse, EvListResponse> {
    };

}; // TEvIndexBuilder

} // NSchemeShard
} // NKikimr
