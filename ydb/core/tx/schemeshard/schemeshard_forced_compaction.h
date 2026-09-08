#pragma once

#include "schemeshard_forced_compaction_fwd.h"

#include <ydb/core/protos/forced_compaction.pb.h>

namespace NKikimr::NSchemeShard {

struct TEvForcedCompaction::TEvCreateRequest: public TEventPB<TEvCreateRequest, NKikimrForcedCompaction::TEvCreateRequest, EvCreateRequest> {
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

struct TEvForcedCompaction::TEvCreateResponse: public TEventPB<TEvCreateResponse, NKikimrForcedCompaction::TEvCreateResponse, EvCreateResponse> {
        TEvCreateResponse() = default;

        explicit TEvCreateResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

struct TEvForcedCompaction::TEvGetRequest: public TEventPB<TEvGetRequest, NKikimrForcedCompaction::TEvGetRequest, EvGetRequest> {
        TEvGetRequest() = default;

        explicit TEvGetRequest(const TString& dbName, const ui64 forcedCompactionId) {
            Record.SetDatabaseName(dbName);
            Record.SetForcedCompactionId(forcedCompactionId);
        }
    };

struct TEvForcedCompaction::TEvGetResponse: public TEventPB<TEvGetResponse, NKikimrForcedCompaction::TEvGetResponse, EvGetResponse> {
        TEvGetResponse() = default;
    };

struct TEvForcedCompaction::TEvCancelRequest: public TEventPB<TEvCancelRequest, NKikimrForcedCompaction::TEvCancelRequest, EvCancelRequest> {
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

struct TEvForcedCompaction::TEvCancelResponse: public TEventPB<TEvCancelResponse, NKikimrForcedCompaction::TEvCancelResponse, EvCancelResponse> {
        TEvCancelResponse() = default;

        explicit TEvCancelResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

struct TEvForcedCompaction::TEvForgetRequest: public TEventPB<TEvForgetRequest, NKikimrForcedCompaction::TEvForgetRequest, EvForgetRequest> {
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

struct TEvForcedCompaction::TEvForgetResponse: public TEventPB<TEvForgetResponse, NKikimrForcedCompaction::TEvForgetResponse, EvForgetResponse> {
        TEvForgetResponse() = default;

        explicit TEvForgetResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

struct TEvForcedCompaction::TEvListRequest: public TEventPB<TEvListRequest, NKikimrForcedCompaction::TEvListRequest, EvListRequest> {
        TEvListRequest() = default;

        explicit TEvListRequest(const TString& dbName, ui64 pageSize, TString pageToken) {
            Record.SetDatabaseName(dbName);
            Record.SetPageSize(pageSize);
            Record.SetPageToken(pageToken);
        }
    };

struct TEvForcedCompaction::TEvListResponse: public TEventPB<TEvListResponse, NKikimrForcedCompaction::TEvListResponse, EvListResponse> {
        TEvListResponse() = default;
    };

} // namespace NKikimr::NSchemeShard
