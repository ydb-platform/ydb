#pragma once

#include "defs.h"
#include "events.h"
#include "types.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    class TBlobDepot
        : public TActor<TBlobDepot>
        , public TTabletExecutedFlat
    {
        struct TEvPrivate {
            enum {
                EvCheckExpiredAgents = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvAssimilatedData,
                EvAssimilatedDataConfirm,
            };
        };

        struct TBlock {
            ui64 TabletId;
            ui32 BlockedGeneration;
            ui64 IssuerGuid = 0;

            TBlock() = default;

            TBlock(const NKikimrBlobStorage::TEvVAssimilateResult::TBlock& item)
                : TabletId(item.GetTabletId())
                , BlockedGeneration(item.GetBlockedGeneration())
            {}

            template<typename T>
            static TBlock FromRow(T&& row) {
                TBlock block;
                block.TabletId = row.template GetValue<Schema::Blocks::TabletId>();
                block.BlockedGeneration = row.template GetValue<Schema::Blocks::BlockedGeneration>();
                block.IssuerGuid = row.template GetValue<Schema::Blocks::IssuerGuid>();
                return block;
            }

            void Merge(TBlock& other) {
                Y_VERIFY_DEBUG(other.TabletId == TabletId);
                BlockedGeneration = Max(BlockedGeneration, other.BlockedGeneration);
            }

            TString ToString() const {
                return TStringBuilder() << "{" << TabletId << ":" << BlockedGeneration << "}";
            }
        };

        struct TBarrier {
            struct TValue {
                TGenStep GenCtr;
                TGenStep Collect;

                TValue() = default;

                TValue(const NKikimrBlobStorage::TEvVAssimilateResult::TBarrier::TValue& value)
                    : GenCtr(value.GetRecordGeneration(), value.GetPerGenerationCounter())
                    , Collect(value.GetCollectGeneration(), value.GetCollectStep())
                {}

                void Merge(TValue& other) {
                    if (GenCtr < other.GenCtr) {
                        *this = other;
                    }
                }

                TString ToString() const {
                    return TStringBuilder() << "{" << GenCtr.ToString() << "=>" << Collect.ToString() << "}";
                }
            };

            ui64 TabletId;
            ui8 Channel;
            TValue Hard;
            TValue Soft;

            TBarrier() = default;

            TBarrier(const NKikimrBlobStorage::TEvVAssimilateResult::TBarrier& item)
                : TabletId(item.GetTabletId())
                , Channel(item.GetChannel())
                , Hard(item.HasHard() ? TValue(item.GetHard()) : TValue())
                , Soft(item.HasSoft() ? TValue(item.GetSoft()) : TValue())
            {}

            template<typename TRow>
            static TBarrier FromRow(TRow&& row) {
                using T = Schema::Barriers;
                TBarrier barrier;
                barrier.TabletId = row.template GetValue<T::TabletId>();
                barrier.Channel = row.template GetValue<T::Channel>();
                if (row.template HaveValue<T::HardGenCtr>() && row.template HaveValue<T::Hard>()) {
                    barrier.Hard.GenCtr = TGenStep(row.template GetValue<T::HardGenCtr>());
                    barrier.Hard.Collect = TGenStep(row.template GetValue<T::Hard>());
                }
                if (row.template HaveValue<T::SoftGenCtr>() && row.template HaveValue<T::Soft>()) {
                    barrier.Soft.GenCtr = TGenStep(row.template GetValue<T::SoftGenCtr>());
                    barrier.Soft.Collect = TGenStep(row.template GetValue<T::Soft>());
                }
                return barrier;
            }

            void Merge(TBarrier& other) {
                Y_VERIFY_DEBUG(TabletId == other.TabletId && Channel == other.Channel);
                Hard.Merge(other.Hard);
                Soft.Merge(other.Soft);
            }

            TString ToString() const {
                return TStringBuilder() << "{" << TabletId << ":" << int(Channel) << "@" << Hard.ToString() << "/"
                    << Soft.ToString() << "}";
            }
        };

        struct TBlob {
            TLogoBlobID Id;
            ui64 Ingress;
            bool Keep = false;

            TBlob() = default;

            TBlob(const NKikimrBlobStorage::TEvVAssimilateResult::TBlob& item, const TLogoBlobID& id)
                : Id(id)
                , Ingress(item.GetIngress())
            {}

            void Merge(TBlob& other) {
                Y_VERIFY_DEBUG(Id == other.Id);
                Ingress |= other.Ingress;
            }

            TString ToString() const {
                return TStringBuilder() << "{" << Id.ToString() << "/" << Ingress << "}";
            }
        };

        struct TEvAssimilatedData : TEventLocal<TEvAssimilatedData, TEvPrivate::EvAssimilatedData> {
            std::deque<TBlock> Blocks;
            bool BlocksFinished = false;
            std::deque<TBarrier> Barriers;
            bool BarriersFinished = false;
            std::deque<TBlob> Blobs;
            bool BlobsFinished = false;
            TString AssimilatorState;
        };

    public:
        TBlobDepot(TActorId tablet, TTabletStorageInfo *info);
        ~TBlobDepot();

        void HandlePoison() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT23, "HandlePoison", (Id, GetLogId()));
            Become(&TThis::StateZombie);
            Send(Tablet(), new TEvents::TEvPoison);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr TDuration ExpirationTimeout = TDuration::Minutes(1);

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
            StartGroupAssimilator();
            ProcessRegisterAgentQ();
        }

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
            if (Config.HasDecommitGroupId()) {
                return TStringBuilder() << "{" << TabletID() << "@" << Config.GetDecommitGroupId() << "}";
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

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring

        class TTxMonData;

        bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;

        void RenderMainPage(IOutputStream& s);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group assimilation

        struct TToken {};
        std::shared_ptr<TToken> Token = std::make_shared<TToken>();

        TActorId RunningGroupAssimilator;
        TActorId CopierId;
        EDecommitState DecommitState = EDecommitState::Default;
        std::optional<TString> AssimilatorState;

        class TGroupAssimilator;
        class TGroupAssimilatorFetchMachine;

        class TGroupAssimilatorCopierActor;

        void StartGroupAssimilator();
        void HandleGone(TAutoPtr<IEventHandle> ev);
        void Handle(TEvAssimilatedData::TPtr ev);
        void ProcessAssimilatedData(TEvAssimilatedData& msg);
    };

} // NKikimr::NBlobDepot
