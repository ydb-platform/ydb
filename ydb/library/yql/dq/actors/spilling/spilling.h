#pragma once

#include <ydb/library/yql/dq/actors/dq_events_ids.h>

#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/core/event_local.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>

namespace NYql::NDq {

struct TEvDqSpilling {
    struct TEvWrite : public NActors::TEventLocal<TEvWrite, TDqSpillingEvents::EvWrite> {
        ui64 BlobId;
        TRope Blob;
        TMaybe<TDuration> Timeout;

        TEvWrite(ui64 blobId, TRope&& blob, TMaybe<TDuration> timeout = {})
            : BlobId(blobId), Blob(std::move(blob)), Timeout(timeout) {}
    };

    struct TEvWriteResult : public NActors::TEventLocal<TEvWriteResult, TDqSpillingEvents::EvWriteResult> {
        ui64 BlobId;

        TEvWriteResult(ui64 blobId)
            : BlobId(blobId) {}
    };

    struct TEvRead : public NActors::TEventLocal<TEvRead, TDqSpillingEvents::EvRead> {
        ui64 BlobId;
        bool RemoveBlob;
        TMaybe<TDuration> Timeout;

        TEvRead(ui64 blobId, bool removeBlob = false, TMaybe<TDuration> timeout = {})
            : BlobId(blobId), RemoveBlob(removeBlob), Timeout(timeout) {}
    };

    struct TEvReadResult : public NActors::TEventLocal<TEvReadResult, TDqSpillingEvents::EvReadResult> {
        ui64 BlobId;
        TBuffer Blob;

        TEvReadResult(ui64 blobId, TBuffer&& blob)
            : BlobId(blobId), Blob(std::move(blob)) {}
    };

    struct TEvError : public NActors::TEventLocal<TEvError, TDqSpillingEvents::EvError> {
        TString Message;

        TEvError(const TString& message)
            : Message(message) {}
    };
};

} // namespace NYql::NDq
