#pragma once

#include "defs.h"
#include "events.h"
#include "types.h"

namespace NKikimr::NBlobDepot {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    class TBlobDepot
        : public TActor<TBlobDepot>
        , public TTabletExecutedFlat
    {
        struct TEvPrivate {
            enum {
                EvCheckExpiredAgents = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
        };

    public:
        TBlobDepot(TActorId tablet, TTabletStorageInfo *info);
        ~TBlobDepot();

        void HandlePoison() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT09, "HandlePoison", (TabletId, TabletID()));
            Become(&TThis::StateZombie);
            Send(Tablet(), new TEvents::TEvPoison);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr TDuration ExpirationTimeout = TDuration::Minutes(1);

        struct TAgent {
            std::optional<TActorId> ConnectedAgent;
            ui32 ConnectedNodeId;
            TInstant ExpirationTimestamp;

            struct TChannelKind {
                TGivenIdRange GivenIdRanges; // updated on AllocateIds and when BlobSeqIds are found in any way
            };

            THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;
        };

        THashMap<TActorId, std::optional<ui32>> PipeServerToNode;
        THashMap<ui32, TAgent> Agents; // NodeId -> Agent

        struct TChannelKind
            : NBlobDepot::TChannelKind
        {
            ui64 NextBlobSeqId = 0;
            TGivenIdRange GivenIdRanges; // for all agents, including disconnected ones
        };

        THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;
        std::vector<NKikimrBlobDepot::TChannelKind::E> ChannelToKind;

        struct TPerChannelRecord {
            std::set<std::tuple<ui32, ui32>> GivenStepIndex;
        };
        THashMap<ui8, TPerChannelRecord> PerChannelRecords;

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev);
        void OnAgentDisconnect(TAgent& agent);
        void Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev);
        void OnAgentConnect(TAgent& agent);
        void Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev);
        TAgent& GetAgent(const TActorId& pipeServerId);
        TAgent& GetAgent(ui32 nodeId);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::deque<std::unique_ptr<IEventHandle>> InitialEventsQ;

        void Enqueue(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            InitialEventsQ.emplace_back(ev.Release());
        }

        void OnActivateExecutor(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT10, "OnActivateExecutor", (TabletId, TabletID()));

            ExecuteTxInitSchema();

            Become(&TThis::StateWork);
            for (auto&& ev : std::exchange(InitialEventsQ, {})) {
                TActivationContext::Send(ev.release());
            }
        }

        void OnDetach(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT11, "OnDetach", (TabletId, TabletID()));

            // TODO: what does this callback mean
            PassAway();
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& /*ev*/, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT12, "OnTabletDead", (TabletId, TabletID()));
            PassAway();
        }

        void InitChannelKinds();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        STFUNC(StateInit) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Poison) {
                HandlePoison();
            } else {
                StateInitImpl(ev, ctx);
            }
        }

        STFUNC(StateZombie) {
            StateInitImpl(ev, ctx);
        }

        void StateWork(STFUNC_SIG);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        bool ReassignChannelsEnabled() const override {
            return true;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void Execute(std::unique_ptr<NTabletFlatExecutor::TTransactionBase<TBlobDepot>> tx) {
            Executor()->Execute(tx.release(), TActivationContext::AsActorContext());
        }

        void ExecuteTxInitSchema();
        void ExecuteTxLoad();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration

        NKikimrBlobDepot::TBlobDepotConfig Config;

        void Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Blocks

        class TBlocksManager;
        std::unique_ptr<TBlocksManager> BlocksManager;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Garbage collection

        class TBarrierServer;
        std::unique_ptr<TBarrierServer> BarrierServer;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Data operations

        class TData;
        std::unique_ptr<TData> Data;

        void Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev);
        void Handle(TEvBlobDepot::TEvResolve::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring

        class TTxMonData;
        
        bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;

        void RenderMainPage(IOutputStream& s);
    };

} // NKikimr::NBlobDepot
