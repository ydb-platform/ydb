#pragma once
#include <ydb/core/tx/data_events/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/async/cancellation.h>
#include <ydb/library/actors/async/event.h>
#include <util/generic/intrlist.h>
#include <util/digest/multi.h>

namespace NKikimr::NDataShard {

    struct TLockRowsRequestId {
        TActorId Sender;
        ui64 RequestId = 0;

        TLockRowsRequestId() = default;

        TLockRowsRequestId(const TActorId& sender, ui64 requestId)
            : Sender(sender)
            , RequestId(requestId)
        {}

        size_t Hash() const {
            return MultiHash(Sender, RequestId);
        }

        bool operator<=>(const TLockRowsRequestId& rhs) const = default;
    };

    struct TLockRowsRequestPipeServerListTag {};

    struct TLockRowsRequestState
        : public TIntrusiveListItem<TLockRowsRequestState, TLockRowsRequestPipeServerListTag>
    {
        TLockRowsRequestId RequestId;
        std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult> Result;
        NActors::TAsyncCancellationScope Scope;
        NActors::TAsyncEvent Finished;
        bool Waiting = false;

        TLockRowsRequestState(const TLockRowsRequestId& requestId)
            : RequestId(requestId)
        {}
    };

} // namespace NKikimr::NDataShard

namespace std {
    template<>
    struct hash<::NKikimr::NDataShard::TLockRowsRequestId> {
        size_t operator()(const ::NKikimr::NDataShard::TLockRowsRequestId& value) const {
            return value.Hash();
        }
    };
}

template<>
struct THash<NKikimr::NDataShard::TLockRowsRequestId> {
    size_t operator()(const NKikimr::NDataShard::TLockRowsRequestId& value) const {
        return value.Hash();
    }
};

