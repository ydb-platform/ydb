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
        TBlobDepot(TActorId tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
            , BlocksManager(CreateBlocksManager())
            , GarbageCollectionManager(CreateGarbageCollectionManager())
        {}

        void HandlePoison() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT09, "HandlePoison", (TabletId, TabletID()));
            Become(&TThis::StateZombie);
            Send(Tablet(), new TEvents::TEvPoison);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static constexpr TDuration ExpirationTimeout = TDuration::Minutes(1);
        static constexpr ui32 PreallocatedIdCount = 100;

        struct TAgentInfo {
            std::optional<TActorId> ConnectedAgent;
            ui32 ConnectedNodeId;
            TInstant ExpirationTimestamp;
        };

        THashMap<TActorId, std::optional<ui32>> PipeServerToNode;
        THashMap<ui32, TAgentInfo> Agents; // NodeId -> Agent

        struct TChannelKind
            : NBlobDepot::TChannelKind
        {
            ui64 NextBlobSeqId = 0;
        };

        THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev);
        void OnAgentDisconnect(TAgentInfo& agent);
        void Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev);
        void OnAgentConnect(TAgentInfo& agent);
        void Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev);
        TAgentInfo& GetAgent(const TActorId& pipeServerId);

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

        STFUNC(StateWork) {
            switch (const ui32 type = ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, HandlePoison);

                hFunc(TEvBlobDepot::TEvApplyConfig, Handle);
                hFunc(TEvBlobDepot::TEvRegisterAgent, Handle);
                hFunc(TEvBlobDepot::TEvAllocateIds, Handle);
                hFunc(TEvBlobDepot::TEvCommitBlobSeq, Handle);
                hFunc(TEvBlobDepot::TEvResolve, Handle);

                hFunc(TEvBlobDepot::TEvBlock, Handle);
                hFunc(TEvBlobDepot::TEvQueryBlocks, Handle);

                hFunc(TEvBlobDepot::TEvCollectGarbage, Handle);

                hFunc(TEvTabletPipe::TEvServerConnected, Handle);
                hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);

                default:
                    if (!HandleDefaultEvents(ev, ctx)) {
                        Y_FAIL("unexpected event Type# 0x%08" PRIx32, type);
                    }
                    break;
            }
        }

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
        struct TBlocksManagerDeleter { void operator ()(TBlocksManager *object) const; };
        using TBlocksManagerPtr = std::unique_ptr<TBlocksManager, TBlocksManagerDeleter>;
        TBlocksManagerPtr BlocksManager;

        TBlocksManagerPtr CreateBlocksManager();

        void AddBlockOnLoad(ui64 tabletId, ui32 blockedGeneration);

        void Handle(TEvBlobDepot::TEvBlock::TPtr ev);
        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Garbage collection

        class TGarbageCollectionManager;
        struct TGarbageCollectionManagerDeleter { void operator ()(TGarbageCollectionManager *object) const; };
        using TGarbageCollectionManagerPtr = std::unique_ptr<TGarbageCollectionManager, TGarbageCollectionManagerDeleter>;
        TGarbageCollectionManagerPtr GarbageCollectionManager;

        TGarbageCollectionManagerPtr CreateGarbageCollectionManager();

        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Key operation

        struct TDataValue {
            TString Meta;
            TCGSI Location;
            ui32 Checksum;
            ui64 TotalDataLen;
            EKeepState KeepState;
            bool Public;
        };

        std::map<TString, TDataValue> Data;

        void Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev);

        void Handle(TEvBlobDepot::TEvResolve::TPtr ev);
    };

} // NKikimr::NBlobDepot
