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
            , DataManager(CreateDataManager())
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
        TAgentInfo& GetAgent(ui32 nodeId);

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
            try {
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
            } catch (...) {
                Y_FAIL_S("unexpected exception# " << CurrentExceptionMessage());
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
        using TBlocksManagerPtr = std::unique_ptr<TBlocksManager, std::function<void(TBlocksManager*)>>;
        TBlocksManagerPtr BlocksManager;

        TBlocksManagerPtr CreateBlocksManager();

        void AddBlockOnLoad(ui64 tabletId, ui32 blockedGeneration);

        void Handle(TEvBlobDepot::TEvBlock::TPtr ev);
        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Garbage collection

        class TGarbageCollectionManager;
        using TGarbageCollectionManagerPtr = std::unique_ptr<TGarbageCollectionManager, std::function<void(TGarbageCollectionManager*)>>;
        TGarbageCollectionManagerPtr GarbageCollectionManager;

        TGarbageCollectionManagerPtr CreateGarbageCollectionManager();

        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);

        bool CheckBlobForBarrier(TLogoBlobID id) const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Data operations

        class TDataManager;
        using TDataManagerPtr = std::unique_ptr<TDataManager, std::function<void(TDataManager*)>>;
        TDataManagerPtr DataManager;

        TDataManagerPtr CreateDataManager();

        using TValueChain = NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TValueChain>;

        struct TDataValue {
            TString Meta;
            TValueChain ValueChain;
            NKikimrBlobDepot::EKeepState KeepState;
            bool Public;
        };

        enum EScanFlags : ui32 {
            INCLUDE_BEGIN = 1,
            INCLUDE_END = 2,
            REVERSE = 4,
        };

        Y_DECLARE_FLAGS(TScanFlags, EScanFlags)

        std::optional<TDataValue> FindKey(TStringBuf key);
        void ScanRange(const std::optional<TStringBuf>& begin, const std::optional<TStringBuf>& end, TScanFlags flags,
            const std::function<bool(TStringBuf, const TDataValue&)>& callback);
        void DeleteKey(TStringBuf key);
        void PutKey(TString key, TDataValue&& data);
        void AddDataOnLoad(TString key, TString value);
        std::optional<TString> UpdateKeepState(TStringBuf key, NKikimrBlobDepot::EKeepState keepState);

        void Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev);
        void Handle(TEvBlobDepot::TEvResolve::TPtr ev);
    };

    Y_DECLARE_OPERATORS_FOR_FLAGS(TBlobDepot::TScanFlags)

} // NKikimr::NBlobDepot
