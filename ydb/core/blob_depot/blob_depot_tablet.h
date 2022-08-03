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
                EvAssimilatedData,
            };
        };

        struct TBlock {
            ui64 TabletId;
            ui32 BlockedGeneration;

            TBlock() = default;

            TBlock(const NKikimrBlobStorage::TEvVAssimilateResult::TBlock& item)
                : TabletId(item.GetTabletId())
                , BlockedGeneration(item.GetBlockedGeneration())
            {}

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
                ui32 RecordGeneration;
                ui32 PerGenerationCounter;
                ui32 CollectGeneration;
                ui32 CollectStep;

                TValue(const NKikimrBlobStorage::TEvVAssimilateResult::TBarrier::TValue& value)
                    : RecordGeneration(value.GetRecordGeneration())
                    , PerGenerationCounter(value.GetPerGenerationCounter())
                    , CollectGeneration(value.GetCollectGeneration())
                    , CollectStep(value.GetCollectStep())
                {}

                void Merge(TValue& other) {
                    if (KeyAsTuple() < other.KeyAsTuple()) {
                        *this = other;
                    }
                }

                TString ToString() const {
                    return TStringBuilder() << "{" << RecordGeneration << ":" << PerGenerationCounter
                        << "=>" << CollectGeneration << ":" << CollectStep << "}";
                }

                std::tuple<ui32, ui32> KeyAsTuple() const {
                    return {RecordGeneration, PerGenerationCounter};
                }
            };

            ui64 TabletId;
            ui8 Channel;
            std::optional<TValue> Hard;
            std::optional<TValue> Soft;

            TBarrier() = default;

            TBarrier(const NKikimrBlobStorage::TEvVAssimilateResult::TBarrier& item)
                : TabletId(item.GetTabletId())
                , Channel(item.GetChannel())
                , Hard(item.HasHard() ? std::make_optional(TValue(item.GetHard())) : std::nullopt)
                , Soft(item.HasSoft() ? std::make_optional(TValue(item.GetSoft())) : std::nullopt)
            {}

            void Merge(TBarrier& other) {
                Y_VERIFY_DEBUG(TabletId == other.TabletId && Channel == other.Channel);
                if (Hard && other.Hard) {
                    Hard->Merge(*other.Hard);
                } else if (other.Hard) {
                    Hard = std::move(other.Hard);
                }
                if (Soft && other.Soft) {
                    Soft->Merge(*other.Soft);
                } else if (other.Soft) {
                    Soft = std::move(other.Soft);
                }
            }

            TString ToString() const {
                return TStringBuilder() << "{" << TabletId << ":" << int(Channel) << "@" << (Hard ? Hard->ToString() : "")
                    << "/" << (Soft ? Soft->ToString() : "") << "}";
            }
        };

        struct TBlob {
            TLogoBlobID Id;
            ui64 Ingress;

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
            const ui32 GroupId;
            std::deque<TBlock> Blocks;
            bool BlocksFinished = false;
            std::deque<TBarrier> Barriers;
            bool BarriersFinished = false;
            std::deque<TBlob> Blobs;
            bool BlobsFinished = false;

            TEvAssimilatedData(ui32 groupId)
                : GroupId(groupId)
            {}
        };

    public:
        TBlobDepot(TActorId tablet, TTabletStorageInfo *info);
        ~TBlobDepot();

        void HandlePoison() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT23, "HandlePoison", (TabletId, TabletID()));
            Become(&TThis::StateZombie);
            Send(Tablet(), new TEvents::TEvPoison);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr TDuration ExpirationTimeout = TDuration::Minutes(1);

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

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void Enqueue(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            Y_FAIL("unexpected event Type# %08" PRIx32, ev->GetTypeRewrite());
        }

        void DefaultSignalTabletActive(const TActorContext&) override {} // signalled explicitly after load is complete

        void OnActivateExecutor(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT24, "OnActivateExecutor", (TabletId, TabletID()));
            ExecuteTxInitSchema();
        }

        void OnLoadFinished() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT25, "OnLoadFinished", (TabletId, TabletID()));
            Become(&TThis::StateWork);
            SignalTabletActive(TActivationContext::AsActorContext());
        }

        void StartOperation() {
            InitChannelKinds();
            StartGroupAssimilators();
        }

        void OnDetach(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT26, "OnDetach", (TabletId, TabletID()));

            // TODO: what does this callback mean
            PassAway();
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& /*ev*/, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT27, "OnTabletDead", (TabletId, TabletID()));
            PassAway();
        }

        void PassAway() override;

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

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Monitoring

        class TTxMonData;
        
        bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;

        void RenderMainPage(IOutputStream& s);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group assimilation

        THashMap<ui32, TActorId> RunningGroupAssimilators;

        class TGroupAssimilator;
        class TGroupAssimilatorFetchMachine;

        void StartGroupAssimilators();
        void StartGroupAssimilator(ui32 groupId);
        void HandleGone(TAutoPtr<IEventHandle> ev);
        void Handle(TEvAssimilatedData::TPtr ev);
    };

} // NKikimr::NBlobDepot
