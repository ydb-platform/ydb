#pragma once

#include "defs.h"
#include "events.h"
#include "types.h"
#include "schema.h"

namespace NKikimr::NTesting {

    class TGroupOverseer;

} // NKikimr::NTesting

namespace NKikimr::NBlobDepot {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    class TBlobDepot
        : public TActor<TBlobDepot>
        , public TTabletExecutedFlat
    {
        struct TEvPrivate {
            enum {
                EvCheckExpiredAgents = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvCommitCertainKeys,
                EvDoGroupMetricsExchange,
                EvKickSpaceMonitor,
            };
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_ACTOR;
        }

        TBlobDepot(TActorId tablet, TTabletStorageInfo *info);
        ~TBlobDepot();

        void HandlePoison() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT23, "HandlePoison", (Id, GetLogId()));
            Become(&TThis::StateZombie);
            Send(Tablet(), new TEvents::TEvPoison);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr TDuration ExpirationTimeout = TDuration::Minutes(1);

        struct TToken {};
        std::shared_ptr<TToken> Token = std::make_shared<TToken>();

        // when in decommission mode and not all blocks are yet recovered, then we postpone agent registration
        THashMap<TActorId, std::deque<std::unique_ptr<IEventHandle>>> RegisterAgentQ;

        struct TAgent {
            std::optional<TActorId> PipeServerId;
            std::optional<TActorId> AgentId;
            ui32 ConnectedNodeId;
            TInstant ExpirationTimestamp;
            std::optional<ui64> AgentInstanceId;

            THashMap<ui8, TGivenIdRange> GivenIdRanges;

            THashMap<ui8, ui32> InvalidatedStepInFlight;
            THashMap<ui64, THashMap<ui8, ui32>> InvalidateStepRequests;
            THashMap<ui64, std::function<void(TEvBlobDepot::TEvPushNotifyResult::TPtr)>> PushCallbacks;
            ui64 LastRequestId = 0;

            NKikimrBlobStorage::TPDiskSpaceColor::E LastPushedSpaceColor = {};
            float LastPushedApproximateFreeSpaceShare = 0.0f;
        };

        THashMap<TActorId, std::optional<ui32>> PipeServerToNode;
        THashMap<ui32, TAgent> Agents; // NodeId -> Agent

        THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;

        struct TChannelInfo {
            ui8 Index;
            NKikimrBlobDepot::TChannelKind::E ChannelKind;
            TChannelKind *KindPtr;
            TGivenIdRange GivenIdRanges; // accumulated through all agents
            ui64 NextBlobSeqId = 0;
        };
        std::vector<TChannelInfo> Channels;

        struct TGroupInfo {
            ui64 AllocatedBytes = 0;
        };
        THashMap<ui32, TGroupInfo> Groups;

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev);
        void OnAgentDisconnect(TAgent& agent);
        void Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev);
        void OnAgentConnect(TAgent& agent);
        void Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev);
        TAgent& GetAgent(const TActorId& pipeServerId);
        TAgent& GetAgent(ui32 nodeId);
        void ResetAgent(TAgent& agent);
        void Handle(TEvBlobDepot::TEvPushNotifyResult::TPtr ev);
        void OnSpaceColorChange(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor, float approximateFreeSpaceShare);

        void ProcessRegisterAgentQ();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void Enqueue(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            Y_FAIL("unexpected event Type# %08" PRIx32, ev->GetTypeRewrite());
        }

        void DefaultSignalTabletActive(const TActorContext&) override {} // signalled explicitly after load is complete

        void OnActivateExecutor(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT24, "OnActivateExecutor", (Id, GetLogId()));
            ExecuteTxInitSchema();
        }

        void OnLoadFinished() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT25, "OnLoadFinished", (Id, GetLogId()));
            Become(&TThis::StateWork);
            SignalTabletActive(TActivationContext::AsActorContext());
        }

        void StartOperation() {
            InitChannelKinds();
            DoGroupMetricsExchange();
            ProcessRegisterAgentQ();
            KickSpaceMonitor();
            StartDataLoad();
        }

        void StartDataLoad();
        void OnDataLoadComplete();

        void OnDetach(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT26, "OnDetach", (Id, GetLogId()));

            // TODO: what does this callback mean
            PassAway();
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& /*ev*/, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT27, "OnTabletDead", (Id, GetLogId()));
            PassAway();
        }

        void PassAway() override;

        void InitChannelKinds();

        TString GetLogId() const {
            if (Config.HasVirtualGroupId()) {
                return TStringBuilder() << "{" << TabletID() << "@" << Config.GetVirtualGroupId() << "}";
            } else {
                return TStringBuilder() << "{" << TabletID() << "}";
            }
        }

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

        bool Configured = false;
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
        void Handle(TEvBlobDepot::TEvDiscardSpoiledBlobSeq::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Space monitoring

        class TSpaceMonitor;
        std::unique_ptr<TSpaceMonitor> SpaceMonitor;

        void Handle(TEvBlobStorage::TEvStatusResult::TPtr ev);
        void KickSpaceMonitor();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring

        class TTxMonData;

        bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;

        void RenderMainPage(IOutputStream& s);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group assimilation

        TActorId GroupAssimilatorId;
        EDecommitState DecommitState = EDecommitState::Default;
        std::optional<TString> AssimilatorState;

        class TGroupAssimilator;

        void StartGroupAssimilator();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group metrics exchange

        void DoGroupMetricsExchange();
        void Handle(TEvBlobStorage::TEvControllerGroupMetricsExchange::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Validation

        void Validate(NTesting::TGroupOverseer& overseer) const;
        void OnSuccessfulGetResult(TLogoBlobID id) const;
    };

} // NKikimr::NBlobDepot
