#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/base.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Long-lived BSController pipe owned by a child actor: the parent sends TEvSend
// and gets the native allocate result (or a synthesized pipe-failure result)
// on its mailbox with the same cookie. One reused pipe, inflight keyed by
// cookie. Poison closes the pipe and drops inflight without synthesizing.
class TBscProxy final: public NActors::TActor<TBscProxy>
{
public:
    enum EEv
    {
        // Parent asks the proxy to SendData this request on the BSC pipe.
        EvSend = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        // Past-the-end sentinel for the private event space.
        EvEnd
    };

    // Forwards Request through the reused BSC pipe. Cookie on the handle
    // identifies this inflight for the matching result.
    struct TEvSend: NActors::TEventLocal<TEvSend, EvSend>
    {
        THolder<NActors::IEventBase> Request;

        explicit TEvSend(THolder<NActors::IEventBase> request);
    };

    // Status on a synthesized result when the BSC pipe fails with inflight
    // requests. Not a BSController application error; the caller should retry.
    static constexpr NKikimrProto::EReplyStatus PipeFailureStatus =
        NKikimrProto::TRYLATER;

    // owner receives native allocate results and synthesized pipe-failure
    // results. logTitle is copied so the child can outlive the parent after
    // Poison.
    TBscProxy(NActors::TActorId owner, TLogTitle logTitle);

private:
    STFUNC(StateWork);

    void HandleSend(TEvSend::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleAllocateResult(
        NKikimr::TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
            TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleConnect(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleDisconnect(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void FailInFlight(const NActors::TActorContext& ctx, const TString& reason);
    void ClosePipe(const NActors::TActorContext& ctx);
    void PassAway() override;

    const NActors::TActorId Owner;
    const TLogTitle LogTitle;
    NActors::TActorId PipeClient;
    // cookie -> request event type (to build the matching *Result on failure).
    THashMap<ui64, ui32> InFlight;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
