#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>

#include <variant>

namespace NKikimr::NPQ {

struct TEvPartitionWriter {
    enum EEv {
        EvInitResult = EventSpaceBegin(TKikimrEvents::ES_PQ_PARTITION_WRITER),
        EvWriteRequest,
        EvWriteAccepted,
        EvWriteResponse,
        EvDisconnected,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_PARTITION_WRITER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_PARTITION_WRITER)");

    struct TEvInitResult: public TEventLocal<TEvInitResult, EvInitResult> {
        using TSourceIdInfo = NKikimrClient::TPersQueuePartitionResponse::TCmdGetMaxSeqNoResult::TSourceIdInfo;

        struct TSuccess {
            TString OwnerCookie;
            TSourceIdInfo SourceIdInfo;
            TString ToString() const;
        };

        struct TError {
            TString Reason;
            NKikimrClient::TResponse Response;
            TString ToString() const;
        };

        std::variant<TSuccess, TError> Result;

        explicit TEvInitResult(const TString& ownerCookie, const TSourceIdInfo& sourceIdInfo)
            : Result(TSuccess{ownerCookie, sourceIdInfo})
        {
        }

        explicit TEvInitResult(const TString& reason, NKikimrClient::TResponse&& response)
            : Result(TError{reason, std::move(response)})
        {
        }

        bool IsSuccess() const { return Result.index() == 0; }
        const TSuccess& GetResult() const { return std::get<0>(Result); }
        const TError& GetError() const { return std::get<1>(Result); }
        TString ToString() const override;
    };

    struct TEvWriteRequest: public TEventPB<TEvWriteRequest, NKikimrClient::TPersQueueRequest, EvWriteRequest> {
        // Only Cookie & CmdWrite must be set, other fields can be overwritten
        TEvWriteRequest() = default;

        explicit TEvWriteRequest(ui64 cookie) {
            Record.MutablePartitionRequest()->SetCookie(cookie);
        }
    };

    struct TEvWriteAccepted: public TEventLocal<TEvWriteAccepted, EvWriteAccepted> {
        ui64 Cookie;

        explicit TEvWriteAccepted(ui64 cookie)
            : Cookie(cookie)
        {
        }

        TString ToString() const override;
    };

    struct TEvWriteResponse: public TEventPB<TEvWriteResponse, NKikimrClient::TResponse, EvWriteResponse> {
        enum EErrors {
            InternalError,
            // Partition located on other node.
            PartitionNotLocal,
            // Partitition restarted.
            PartitionDisconnected
        };

        struct TSuccess {
        };

        struct TError {
            EErrors Code;
            TString Reason;
        };

        std::variant<TSuccess, TError> Result;

        TEvWriteResponse() = default;

        explicit TEvWriteResponse(NKikimrClient::TResponse&& response)
            : Result(TSuccess{})
        {
            Record = std::move(response);
        }

        explicit TEvWriteResponse(const EErrors code, const TString& reason, NKikimrClient::TResponse&& response)
            : Result(TError{code, reason})
        {
            Record = std::move(response);
        }

        bool IsSuccess() const { return Result.index() == 0; }
        const TError& GetError() const { return std::get<1>(Result); }
        TString DumpError() const;
        TString ToString() const override;
    };

    struct TEvDisconnected: public TEventLocal<TEvDisconnected, EvDisconnected> {
    };

}; // TEvPartitionWriter

struct TPartitionWriterOpts {
    bool CheckState = false;
    bool AutoRegister = false;
    bool UseDeduplication = true;

    TPartitionWriterOpts& WithCheckState(bool value) { CheckState = value; return *this; }
    TPartitionWriterOpts& WithAutoRegister(bool value) { AutoRegister = value; return *this; }
    TPartitionWriterOpts& WithDeduplication(bool value) { UseDeduplication = value; return *this; }
};

IActor* CreatePartitionWriter(const TActorId& client, ui64 tabletId, ui32 partitionId, TMaybe<ui32> expectedGeneration, const TString& sourceId,
                              const TPartitionWriterOpts& opts = {});
}
