#include "sequenceshard_impl.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <util/stream/output.h>

namespace NKikimr {
namespace NSequenceShard {

    TSequenceShard::TSequenceShard(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , LogPrefix(this)
    {
        TabletCountersPtr.Reset(new TProtobufTabletCounters<
            ESimpleCounters_descriptor,
            ECumulativeCounters_descriptor,
            EPercentileCounters_descriptor,
            ETxTypes_descriptor>());
        TabletCounters = TabletCountersPtr.Get();

        ResetState();
    }

    TSequenceShard::~TSequenceShard() {
        // nothing
    }

    void TSequenceShard::OnDetach(const TActorContext& ctx) {
        Die(ctx);
    }

    void TSequenceShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
        Die(ctx);
    }

    void TSequenceShard::OnActivateExecutor(const TActorContext& ctx) {
        SLOG_T("OnActivateExecutor");

        Executor()->RegisterExternalTabletCounters(TabletCountersPtr.Release());
        RunTxInitSchema(ctx);
    }

    void TSequenceShard::DefaultSignalTabletActive(const TActorContext&) {
        // ignore
    }

    void TSequenceShard::ResetState() {
        CurrentSchemeShardId = 0;
        ProcessingParams.reset();
        Sequences.clear();
        SchemeShardRounds.clear();
    }

    void TSequenceShard::ResetCounters() {
        // nothing yet
    }

    bool TSequenceShard::CheckPipeRequest(const TActorId& serverId) const noexcept {
        auto it = PipeInfos.find(serverId);
        if (it == PipeInfos.end()) {
            return false; // pipe already closed
        }

        ui64 schemeShardId = it->second.SchemeShardId;
        if (schemeShardId == 0) {
            return true; // pipe not marked as schemeshard
        }

        auto pipeValue = std::make_pair(it->second.Generation, it->second.Round);
        auto currentValue = SchemeShardRounds.at(schemeShardId);

        // will return false for outdated pipes
        return currentValue <= pipeValue;
    }

    STFUNC(TSequenceShard::StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(TSequenceShard::StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvServerConnected, Handle);
            hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);

            HFunc(TEvSequenceShard::TEvMarkSchemeShardPipe, Handle);
            HFunc(TEvSequenceShard::TEvCreateSequence, Handle);
            HFunc(TEvSequenceShard::TEvAllocateSequence, Handle);
            HFunc(TEvSequenceShard::TEvDropSequence, Handle);
            HFunc(TEvSequenceShard::TEvUpdateSequence, Handle);
            HFunc(TEvSequenceShard::TEvFreezeSequence, Handle);
            HFunc(TEvSequenceShard::TEvRestoreSequence, Handle);
            HFunc(TEvSequenceShard::TEvRedirectSequence, Handle);
            HFunc(TEvSequenceShard::TEvGetSequence, Handle);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    Y_ABORT("Unexpected event 0x%x", ev->GetTypeRewrite());
                }
                break;
        }
    }

    void TSequenceShard::SwitchToWork(const TActorContext& ctx) {
        SignalTabletActive(ctx);
        Become(&TThis::StateWork);
    }

    void TSequenceShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev) {
        auto* msg = ev->Get();
        Y_DEBUG_ABORT_UNLESS(!PipeInfos.contains(msg->ServerId), "Unexpected duplicate pipe server");
        auto& info = PipeInfos[msg->ServerId];
        info.ServerId = msg->ServerId;
        info.ClientId = msg->ClientId;
    }

    void TSequenceShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
        auto* msg = ev->Get();
        Y_DEBUG_ABORT_UNLESS(PipeInfos.contains(msg->ServerId), "Unexpected missing pipe server");
        PipeInfos.erase(msg->ServerId);
    }

} // namespace NSequenceShard
} // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NSequenceShard::TSequenceShard::TLogPrefix, stream, value) {
    stream << "[sequenceshard " << value.Self->TabletID() << "] ";
}
