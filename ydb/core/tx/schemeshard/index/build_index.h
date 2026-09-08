#pragma once

#include <ydb/core/tx/schemeshard/index/build_index_fwd.h>

#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr {
namespace NSchemeShard {

#define DEFINE_INDEX_BUILDER_EVENT(event, id) \
    struct TEvIndexBuilder::event: public TEventPB<event, NKikimrIndexBuilder::event, TEvIndexBuilder::id>

    DEFINE_INDEX_BUILDER_EVENT(TEvCreateRequest, EvCreateRequest) {
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

    DEFINE_INDEX_BUILDER_EVENT(TEvCreateResponse, EvCreateResponse) {
        TEvCreateResponse() = default;

        explicit TEvCreateResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvGetRequest, EvGetRequest) {

        TEvGetRequest() = default;

        explicit TEvGetRequest(const TString& dbName, ui64 buildIndexId) {
            Record.SetDatabaseName(dbName);
            Record.SetIndexBuildId(buildIndexId);
        }
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvGetResponse, EvGetResponse) {
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvCancelRequest, EvCancelRequest) {
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

    DEFINE_INDEX_BUILDER_EVENT(TEvCancelResponse, EvCancelResponse) {
        TEvCancelResponse() = default;

        explicit TEvCancelResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvForgetRequest, EvForgetRequest) {
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

    DEFINE_INDEX_BUILDER_EVENT(TEvForgetResponse, EvForgetResponse) {
        TEvForgetResponse() = default;

        explicit TEvForgetResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvListRequest, EvListRequest) {
        TEvListRequest() = default;

        explicit TEvListRequest(const TString& dbName, ui64 pageSize, TString pageToken) {
            Record.SetDatabaseName(dbName);
            Record.SetPageSize(pageSize);
            Record.SetPageToken(pageToken);
        }
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvListResponse, EvListResponse) {
    };

    DEFINE_INDEX_BUILDER_EVENT(TEvUploadSampleKResponse, EvUploadSampleKResponse) {
    };

#undef DEFINE_INDEX_BUILDER_EVENT

} // NSchemeShard
} // NKikimr
