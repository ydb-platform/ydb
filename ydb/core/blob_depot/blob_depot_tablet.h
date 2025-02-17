#pragma once

#include "defs.h"
#include "events.h"
#include "types.h"
#include "schema.h"
#include "mon_main.h"

#include <ydb/core/protos/blob_depot_config.pb.h>

namespace NKikimr::NTesting {

    class TGroupOverseer;

} // NKikimr::NTesting

namespace NKikimr::NBlobDepot {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    struct TToken {};

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
                EvUpdateThroughputs,
                EvDeliver,
                EvJsonTimer,
                EvJsonUpdate,
            };
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_ACTOR;
        }

        TBlobDepot(TActorId tablet, TTabletStorageInfo *info);
        ~TBlobDepot();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TAutoPtr<TTabletCountersBase> TabletCountersPtr;
        TTabletCountersBase* const TabletCounters;

        static constexpr TDuration ExpirationTimeout = TDuration::Minutes(1);

        std::shared_ptr<TToken> Token = std::make_shared<TToken>();

        struct TAgent {
            struct TConnection {
                TActorId PipeServerId;
                TActorId AgentId;
                ui32 NodeId;
            };

            std::optional<TConnection> Connection;
            TInstant ExpirationTimestamp;
            std::optional<ui64> AgentInstanceId;

            THashMap<ui8, TGivenIdRange> GivenIdRanges;

            THashMap<ui8, ui32> InvalidatedStepInFlight;
            THashMap<ui64, THashMap<ui8, ui32>> InvalidateStepRequests;

            THashMap<ui64, std::tuple<ui32, ui64, TActorId>> BlockToDeliver; // TabletId -> (BlockedGeneration, IssuerGuid, ActorId)

            THashMap<ui64, std::function<void(TEvBlobDepot::TEvPushNotifyResult::TPtr)>> PushCallbacks;
            ui64 LastRequestId = 0;

            NKikimrBlobStorage::TPDiskSpaceColor::E LastPushedSpaceColor = {};
            float LastPushedApproximateFreeSpaceShare = 0.0f;
        };

        struct TPipeServerContext {
            std::optional<ui32> NodeId; // as reported by RegisterAgent
            ui64 NextExpectedMsgId = 1;
            std::deque<std::unique_ptr<IEventHandle>> PostponeQ;
            size_t InFlightDeliveries = 0;
        };

        THashMap<TActorId, TPipeServerContext> PipeServers;
        THashMap<ui32, TAgent> Agents; // NodeId -> Agent

        struct TChannelKind : NBlobDepot::TChannelKind {
            std::vector<std::tuple<ui32, ui64>> GroupAccumWeights; // last one is the total weight
        };

        THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;

        struct TChannelInfo {
            ui8 Index;
            ui32 GroupId;
            NKikimrBlobDepot::TChannelKind::E ChannelKind;
            TChannelKind *KindPtr;
            TGivenIdRange GivenIdRanges; // accumulated through all agents
            ui64 NextBlobSeqId = 0;
            std::set<ui64> SequenceNumbersInFlight; // of blobs being committed
            std::set<ui64> AssimilatedBlobsInFlight;
            std::optional<TBlobSeqId> LastReportedLeastId;

            // Obtain the least BlobSeqId that is not yet committed, but may be written by any agent
            TBlobSeqId GetLeastExpectedBlobId(ui32 generation) {
                const auto result = TBlobSeqId::FromSequentalNumber(Index, generation, Min(NextBlobSeqId,
                    GivenIdRanges.IsEmpty() ? Max<ui64>() : GivenIdRanges.GetMinimumValue(),
                    SequenceNumbersInFlight.empty() ? Max<ui64>() : *SequenceNumbersInFlight.begin(),
                    AssimilatedBlobsInFlight.empty() ? Max<ui64>() : *AssimilatedBlobsInFlight.begin()));
                // this value can't decrease, because it may lead to data loss
                Y_VERIFY_S(!LastReportedLeastId || *LastReportedLeastId <= result,
                    "decreasing LeastExpectedBlobId"
                    << " LastReportedLeastId# " << LastReportedLeastId->ToString()
                    << " result# " << result.ToString()
                    << " NextBlobSeqId# " << NextBlobSeqId
                    << " GivenIdRanges# " << GivenIdRanges.ToString()
                    << " SequenceNumbersInFlight# " << FormatList(SequenceNumbersInFlight)
                    << " AssimilatedBlobsInFlight# " << FormatList(AssimilatedBlobsInFlight));
                LastReportedLeastId.emplace(result);
                return result;
            }
        };
        std::vector<TChannelInfo> Channels;

        struct TGroupInfo {
            THashMap<NKikimrBlobDepot::TChannelKind::E, std::vector<ui8>> Channels;
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

        bool ReadyForAgentQueries() const {
            return Configured && (!Config.GetIsDecommittingGroup() || DecommitState >= EDecommitState::BlocksFinished);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void Enqueue(TAutoPtr<IEventHandle>& ev) override {
            Y_ABORT("unexpected event Type# %08" PRIx32, ev->GetTypeRewrite());
        }

        void DefaultSignalTabletActive(const TActorContext&) override {} // signalled explicitly after load is complete

        void OnActivateExecutor(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT24, "OnActivateExecutor", (Id, GetLogId()));
            Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
            ExecuteTxInitSchema();
        }

        void OnLoadFinished() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT25, "OnLoadFinished", (Id, GetLogId()));
            Become(&TThis::StateWork);
            SignalTabletActive(TActivationContext::AsActorContext());
        }

        void StartOperation() {
            JsonHandler.Setup(SelfId(), Executor()->Generation());
            InitChannelKinds();
            DoGroupMetricsExchange();
            ProcessRegisterAgentQ();
            KickSpaceMonitor();
            StartDataLoad();
            UpdateThroughputs();
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
        void InvalidateGroupForAllocation(ui32 groupId);
        bool PickChannels(NKikimrBlobDepot::TChannelKind::E kind, std::vector<ui8>& channels);

        TString GetLogId() const {
            const auto *executor = Executor();
            const ui32 generation = executor ? executor->Generation() : 0;
            TStringBuilder sb;
            sb << '{' << TabletID();
            if (Config.HasVirtualGroupId()) {
                sb << '@' << Config.GetVirtualGroupId();
            }
            sb << '}';
            if (generation) {
                sb << ':' << generation;
            }
            return sb;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        void HandleFromAgent(STATEFN_SIG);
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

        TJsonHandler JsonHandler;

        bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;

        void RenderMainPage(IOutputStream& s);
        NJson::TJsonValue RenderJson(bool pretty);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group assimilation

        TActorId GroupAssimilatorId;
        EDecommitState DecommitState = EDecommitState::Default;
        std::optional<TString> AssimilatorState;
        struct TAsStats {
            std::optional<ui64> SkipBlocksUpTo;
            std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
            std::optional<TLogoBlobID> SkipBlobsUpTo;
            TInstant LatestErrorGet;
            TInstant LatestOkGet;
            TInstant LatestErrorPut;
            TInstant LatestOkPut;
            TLogoBlobID LastReadBlobId;
            ui64 BytesToCopy = 0;
            ui64 BytesCopied = 0;
            ui64 CopySpeed = 0;
            TDuration CopyTimeRemaining = TDuration::Max();
            ui64 BlobsReadOk = 0;
            ui64 BlobsReadNoData = 0;
            ui64 BlobsReadError = 0;
            ui64 BlobsPutOk = 0;
            ui64 BlobsPutError = 0;
            ui32 CopyIteration = 0;

            void ToJson(NJson::TJsonValue& json, bool pretty) const;
        } AsStats;

        class TGroupAssimilator;

        void StartGroupAssimilator();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group metrics exchange

        ui64 BytesRead = 0;
        ui64 BytesWritten = 0;
        std::deque<std::tuple<TMonotonic, ui64, ui64>> MetricsQ;
        ui64 ReadThroughput = 0;
        ui64 WriteThroughput = 0;

        void DoGroupMetricsExchange();
        void Handle(TEvBlobStorage::TEvControllerGroupMetricsExchange::TPtr ev);
        void Handle(TEvBlobDepot::TEvPushMetrics::TPtr ev);
        void UpdateThroughputs(bool reschedule = true);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Validation

        void Validate(NTesting::TGroupOverseer& overseer) const;
        void OnSuccessfulGetResult(TLogoBlobID id) const;
    };

} // NKikimr::NBlobDepot
