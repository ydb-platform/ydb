#pragma once

#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/actors/util/rope.h>

#include <util/generic/buffer.h>

namespace NKikimr::NKqp {

struct TEvKqpSpilling {
    struct TEvWrite : public TEventLocal<TEvWrite, TKqpSpillingEvents::EvWrite> {
        ui64 BlobId;
        TRope Blob;
        TMaybe<TDuration> Timeout;

        TEvWrite(ui64 blobId, TRope&& blob, TMaybe<TDuration> timeout = {})
            : BlobId(blobId), Blob(std::move(blob)), Timeout(timeout) {}
    };

    struct TEvWriteResult : public TEventLocal<TEvWriteResult, TKqpSpillingEvents::EvWriteResult> {
        ui64 BlobId;

        TEvWriteResult(ui64 blobId)
            : BlobId(blobId) {}
    };

    struct TEvRead : public TEventLocal<TEvRead, TKqpSpillingEvents::EvRead> {
        ui64 BlobId;
        bool RemoveBlob;
        TMaybe<TDuration> Timeout;

        TEvRead(ui64 blobId, bool removeBlob = false, TMaybe<TDuration> timeout = {})
            : BlobId(blobId), RemoveBlob(removeBlob), Timeout(timeout) {}
    };

    struct TEvReadResult : public TEventLocal<TEvReadResult, TKqpSpillingEvents::EvReadResult> {
        ui64 BlobId;
        TBuffer Blob;

        TEvReadResult(ui64 blobId, TBuffer&& blob)
            : BlobId(blobId), Blob(std::move(blob)) {}
    };

    struct TEvError : public TEventLocal<TEvError, TKqpSpillingEvents::EvError> {
        TString Message;

        TEvError(const TString& message)
            : Message(message) {}
    };
};

} // namespace NKikimr::NKqp
