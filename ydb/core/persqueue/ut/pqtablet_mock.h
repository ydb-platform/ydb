#pragma once

#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NHelpers {

class TPQTabletMock : public TActor<TPQTabletMock>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    struct TSendReadSetParams {
        ui64 Step;
        ui64 TxId;
        ui64 Target;
        NKikimrTx::TReadSetData::EDecision Decision;
    };

    struct TSendReadSetAckParams {
        ui64 Step;
        ui64 TxId;
        ui64 Source;
    };

    TPQTabletMock(const TActorId& tablet, TTabletStorageInfo* info);

    void SendReadSet(NActors::TTestActorRuntime& runtime, const TSendReadSetParams& params);
    void SendReadSetAck(NActors::TTestActorRuntime& runtime, const TSendReadSetAckParams& params);

    TMaybe<NKikimrTx::TEvReadSet> ReadSet;
    TMaybe<NKikimrTx::TEvReadSetAck> ReadSetAck;

    THashMap<std::pair<ui64, ui64>, TVector<NKikimrTx::TEvReadSet>> ReadSets;

private:
    struct TEvPQTablet {
        enum EEv {
            EvSendReadSet = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSendReadSetAck,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvSendReadSet : public TEventLocal<TEvSendReadSet, EvSendReadSet> {
            TSendReadSetParams Params;
        };

        struct TEvSendReadSetAck : public TEventLocal<TEvSendReadSetAck, EvSendReadSetAck> {
            TSendReadSetAckParams Params;
        };
    };

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            // TX
            HFunc(TEvTxProcessing::TEvReadSet, Handle);
            HFunc(TEvTxProcessing::TEvReadSetAck, Handle);
            HFunc(TEvPQTablet::TEvSendReadSet, Handle);
            HFunc(TEvPQTablet::TEvSendReadSetAck, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
        }
    }

    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQTablet::TEvSendReadSet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQTablet::TEvSendReadSetAck::TPtr& ev, const TActorContext& ctx);

    static NTabletPipe::TClientConfig GetPipeClientConfig();

    TIntrusivePtr<NTabletPipe::TBoundedClientCacheConfig> PipeClientCacheConfig;
    THolder<NTabletPipe::IClientCache> PipeClientCache;
};

TPQTabletMock* CreatePQTabletMock(const TActorId& tablet, TTabletStorageInfo *info);

}
