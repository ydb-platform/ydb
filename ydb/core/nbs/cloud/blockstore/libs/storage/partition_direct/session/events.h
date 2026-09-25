#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// Local control requests. An unhandled/dropped event completes its future with
// an error, including actor-system cleanup, instead of leaving an RPC pending.
struct TEvPartitionSession
{
    enum EEvents
    {
        // Separate from public NBS events and partition_direct's offset 1000.
        EvMount =
            EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2_SERVICE) + 1500,
        // Revoke a matching session on the same partition incarnation.
        EvUnmount,
    };

    struct TEvMount final: NActors::TEventLocal<TEvMount, EvMount>
    {
        TString ClientId;
        NThreading::TPromise<TResultOrError<TString>> Result;

        explicit TEvMount(TString clientId);
        ~TEvMount() override;
    };

    struct TEvUnmount final: NActors::TEventLocal<TEvUnmount, EvUnmount>
    {
        TString ClientId;
        TString SessionId;
        NThreading::TPromise<NProto::TError> Result;

        TEvUnmount(TString clientId, TString sessionId);
        ~TEvUnmount() override;
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
