#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

#define ENUMERATE_INCOMING_EVENTS(XX) \
        XX(EvPut) \
        XX(EvGet) \
        XX(EvBlock) \
        XX(EvDiscover) \
        XX(EvRange) \
        XX(EvCollectGarbage) \
        XX(EvStatus) \
        XX(EvPatch) \
        XX(EvAssimilate) \
        // END

    class TBlobDepotAgent;

    struct TRequestContext {
        virtual ~TRequestContext() = default;

        template<typename T>
        T& Obtain() {
            T *sp = static_cast<T*>(this);
            Y_VERIFY_DEBUG(sp && sp == dynamic_cast<T*>(this));
            return *sp;
        }

        using TPtr = std::shared_ptr<TRequestContext>;
    };

    struct TTabletDisconnected {};
    struct TKeyResolved { const TResolvedValueChain* ValueChain; };

    class TRequestSender {
        THashMap<ui64, TRequestContext::TPtr> RequestsInFlight;

    protected:
        TBlobDepotAgent& Agent;

    public:
        using TResponse = std::variant<
            // internal events
            TTabletDisconnected,
            TKeyResolved,

            // tablet responses
            TEvBlobDepot::TEvRegisterAgentResult*,
            TEvBlobDepot::TEvAllocateIdsResult*,
            TEvBlobDepot::TEvBlockResult*,
            TEvBlobDepot::TEvQueryBlocksResult*,
            TEvBlobDepot::TEvCollectGarbageResult*,
            TEvBlobDepot::TEvCommitBlobSeqResult*,
            TEvBlobDepot::TEvResolveResult*,

            // underlying DS proxy responses
            TEvBlobStorage::TEvGetResult*,
            TEvBlobStorage::TEvPutResult*
        >;

        static TString ToString(const TResponse& response);

    public:
        TRequestSender(TBlobDepotAgent& agent);
        virtual ~TRequestSender();
        void RegisterRequest(ui64 id, TRequestContext::TPtr context);
        void OnRequestComplete(ui64 id, TResponse response);

    protected:
        virtual void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) = 0;
    };

    class TBlobDepotAgent
        : public TActorBootstrapped<TBlobDepotAgent>
        , public TRequestSender
    {
        const ui32 VirtualGroupId;
        const TActorId ProxyId;
        const ui64 AgentInstanceId;
        ui64 TabletId = Max<ui64>();
        TActorId PipeId;

    private:
        struct TEvPrivate {
            enum {
                EvQueryWatchdog = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_AGENT_ACTOR;
        }

        TBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId);
        ~TBlobDepotAgent();

        void Bootstrap();

#define FORWARD_STORAGE_PROXY(TYPE) fFunc(TEvBlobStorage::TYPE, HandleStorageProxy);
        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Poison, PassAway);
            hFunc(TEvBlobStorage::TEvConfigureProxy, Handle);

            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            hFunc(TEvBlobDepot::TEvPushNotify, Handle);

            hFunc(TEvBlobDepot::TEvRegisterAgentResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvAllocateIdsResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvBlockResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvQueryBlocksResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvCollectGarbageResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvCommitBlobSeqResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvResolveResult, HandleTabletResponse);

            hFunc(TEvBlobStorage::TEvGetResult, HandleOtherResponse);
            hFunc(TEvBlobStorage::TEvPutResult, HandleOtherResponse);

            ENUMERATE_INCOMING_EVENTS(FORWARD_STORAGE_PROXY)
            hFunc(TEvBlobStorage::TEvBunchOfEvents, Handle);

            cFunc(TEvPrivate::EvQueryWatchdog, HandleQueryWatchdog);
        );
#undef FORWARD_STORAGE_PROXY

        void PassAway() override {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeId);
            TActor::PassAway();
        }

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
            if (const auto& info = ev->Get()->Info) {
                Y_VERIFY(info->BlobDepotId);
                if (TabletId != *info->BlobDepotId) {
                    TabletId = *info->BlobDepotId;
                    if (TabletId && TabletId != Max<ui64>()) {
                        ConnectToBlobDepot();
                    }
                
                    for (auto& ev : std::exchange(PendingEventQ, {})) {
                        TActivationContext::Send(ev.release());
                    }
                }
                if (!info->GetTotalVDisksNum()) {
                    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, ProxyId, {}, nullptr, 0));
                    return;
                }
            }

            TActivationContext::Send(ev->Forward(ProxyId));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Request/response delivery logic

        struct TRequestInFlight {
            using TCancelCallback = std::function<void()>;

            TRequestSender *Sender;
            TCancelCallback CancelCallback;
        };

        using TRequestsInFlight = THashMap<ui64, TRequestInFlight>;

        ui64 NextRequestId = 1;
        TRequestsInFlight TabletRequestInFlight;
        TRequestsInFlight OtherRequestInFlight;

        void RegisterRequest(ui64 id, TRequestSender *sender, TRequestContext::TPtr context,
            TRequestInFlight::TCancelCallback cancelCallback, bool toBlobDepotTablet);

        template<typename TEvent>
        void HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        template<typename TEvent>
        void HandleOtherResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        void OnRequestComplete(ui64 id, TRequestSender::TResponse response, TRequestsInFlight& map);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TAllocateIdsContext : TRequestContext {
            NKikimrBlobDepot::TChannelKind::E ChannelKind;

            TAllocateIdsContext(NKikimrBlobDepot::TChannelKind::E channelKind)
                : ChannelKind(channelKind)
            {}
        };

        ui32 BlobDepotGeneration = 0;
        std::optional<ui32> DecommitGroupId;
        NKikimrBlobStorage::TPDiskSpaceColor::E SpaceColor = {};
        float ApproximateFreeSpaceShare = 0.0f;

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void ConnectToBlobDepot();
        void OnDisconnect();

        void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override;
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvRegisterAgentResult& msg);
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvAllocateIdsResult& msg);

        template<typename T, typename = typename TEvBlobDepot::TEventFor<T>::Type>
        void Issue(T msg, TRequestSender *sender, TRequestContext::TPtr context);

        void Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestContext::TPtr context);

        void Handle(TEvBlobDepot::TEvPushNotify::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TExecutingQueries {};
        struct TPendingBlockChecks {};
        struct TPendingId {};

        class TQuery
            : public TIntrusiveListItem<TQuery, TExecutingQueries>
            , public TIntrusiveListItem<TQuery, TPendingBlockChecks>
            , public TIntrusiveListItem<TQuery, TPendingId>
            , public TRequestSender
        {
        protected:
            std::unique_ptr<IEventHandle> Event; // original query event
            const ui64 QueryId;
            const TMonotonic StartTime;
            std::multimap<TMonotonic, TQuery*>::iterator QueryWatchdogMapIter;
            NLog::EPriority WatchdogPriority = NLog::PRI_WARN;

            static constexpr TDuration WatchdogDuration = TDuration::Seconds(10);

        public:
            TQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event);
            virtual ~TQuery();

            void CheckQueryExecutionTime(TMonotonic now);

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason);
            void EndWithSuccess(std::unique_ptr<IEventBase> response);
            TString GetName() const;
            ui64 GetQueryId() const { return QueryId; }
            virtual ui64 GetTabletId() const { return 0; }
            virtual void Initiate() = 0;

            virtual void OnUpdateBlock(bool /*success*/) {}
            virtual void OnRead(ui64 /*tag*/, NKikimrProto::EReplyStatus /*status*/, TString /*dataOrErrorReason*/) {}
            virtual void OnIdAllocated() {}
            virtual void OnDestroy(bool /*success*/) {}

        public:
            struct TDeleter {
                static void Destroy(TQuery *query) { delete query; }
            };
        };

        template<typename TEvent>
        class TBlobStorageQuery : public TQuery {
        public:
            TBlobStorageQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event)
                : TQuery(agent, std::move(event))
                , Request(*Event->Get<TEvent>())
            {}

        protected:
            TEvent& Request;
        };

        std::deque<std::unique_ptr<IEventHandle>> PendingEventQ;
        TIntrusiveListWithAutoDelete<TQuery, TQuery::TDeleter, TExecutingQueries> ExecutingQueries;
        std::multimap<TMonotonic, TQuery*> QueryWatchdogMap;

        void HandleQueryWatchdog();
        void HandleStorageProxy(TAutoPtr<IEventHandle> ev);
        void Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev);
        TQuery *CreateQuery(TAutoPtr<IEventHandle> ev);
        template<ui32 EventType> TQuery *CreateQuery(std::unique_ptr<IEventHandle> ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TChannelKind
            : NBlobDepot::TChannelKind
        {
            const NKikimrBlobDepot::TChannelKind::E Kind;

            bool IdAllocInFlight = false;

            THashMap<ui8, TGivenIdRange> GivenIdRangePerChannel;
            ui32 NumAvailableItems = 0;

            std::set<TBlobSeqId> WritesInFlight;

            TIntrusiveList<TQuery, TPendingId> QueriesWaitingForId;

            TChannelKind(NKikimrBlobDepot::TChannelKind::E kind)
                : Kind(kind)
            {}

            void IssueGivenIdRange(const NKikimrBlobDepot::TGivenIdRange& proto);
            ui32 GetNumAvailableItems() const;
            std::optional<TBlobSeqId> Allocate(TBlobDepotAgent& agent);
            std::tuple<TLogoBlobID, ui32> MakeBlobId(TBlobDepotAgent& agent, const TBlobSeqId& blobSeqId, EBlobType type,
                    ui32 part, ui32 size) const;
            void Trim(ui8 channel, ui32 generation, ui32 invalidatedStep);
            void RebuildHeap();

            void EnqueueQueryWaitingForId(TQuery *query);
            void ProcessQueriesWaitingForId();
        };

        THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;
        THashMap<ui8, TChannelKind*> ChannelToKind;

        void IssueAllocateIdsIfNeeded(TChannelKind& kind);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // DS proxy interaction

        void SendToProxy(ui32 groupId, std::unique_ptr<IEventBase> event, TRequestSender *sender, TRequestContext::TPtr context);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Blocks

        class TBlocksManager;
        std::unique_ptr<TBlocksManager> BlocksManagerPtr;
        TBlocksManager& BlocksManager;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Reading

        struct TReadContext;
        struct TReadArg {
            const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TResolvedValueChain>& Values;
            NKikimrBlobStorage::EGetHandleClass GetHandleClass;
            bool MustRestoreFirst = false;
            TQuery *Query = nullptr;
            ui64 Offset = 0;
            ui64 Size = 0;
            ui64 Tag = 0;
            std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> ReaderTabletData;
        };

        bool IssueRead(const TReadArg& arg, TString& error);

        void HandleGetResult(const TRequestContext::TPtr& context, TEvBlobStorage::TEvGetResult& msg);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Blob mapping cache

        class TBlobMappingCache;
        std::unique_ptr<TBlobMappingCache> BlobMappingCachePtr;
        TBlobMappingCache& BlobMappingCache;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Status flags

        TStorageStatusFlags GetStorageStatusFlags() const;
        float GetApproximateFreeSpaceShare() const;
    };

} // NKikimr::NBlobDepot
