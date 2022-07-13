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

    using TValueChain = NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TValueChain>;

    struct TTabletDisconnected {};
    struct TKeyResolved { const TValueChain* ValueChain; };

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
        : public TActor<TBlobDepotAgent>
        , public TRequestSender
    {
        const ui32 VirtualGroupId;
        const ui64 AgentInstanceId;
        ui64 TabletId = Max<ui64>();
        TActorId PipeId;

    public:
        TBlobDepotAgent(ui32 virtualGroupId);
        ~TBlobDepotAgent();

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
        );
#undef FORWARD_STORAGE_PROXY

        void PassAway() override {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeId);
            TActor::PassAway();
        }

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
            const auto& info = ev->Get()->Info;
            Y_VERIFY(info);
            Y_VERIFY(info->BlobDepotId);
            TabletId = *info->BlobDepotId;
            if (TabletId) {
                ConnectToBlobDepot();
            }
            
            for (auto& ev : std::exchange(PendingEventQ, {})) {
                TActivationContext::Send(ev.release());
            }
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

        bool Registered = false;
        ui32 BlobDepotGeneration = 0;

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void ConnectToBlobDepot();
        void OnDisconnect();

        void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override;
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvRegisterAgentResult& msg);
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvAllocateIdsResult& msg);

        void Issue(NKikimrBlobDepot::TEvBlock msg, TRequestSender *sender, TRequestContext::TPtr context);
        void Issue(NKikimrBlobDepot::TEvResolve msg, TRequestSender *sender, TRequestContext::TPtr context);
        void Issue(NKikimrBlobDepot::TEvQueryBlocks msg, TRequestSender *sender, TRequestContext::TPtr context);
        void Issue(NKikimrBlobDepot::TEvCollectGarbage msg, TRequestSender *sender, TRequestContext::TPtr context);
        void Issue(NKikimrBlobDepot::TEvCommitBlobSeq msg, TRequestSender *sender, TRequestContext::TPtr context);

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

        public:
            TQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event)
                : TRequestSender(agent)
                , Event(std::move(event))
                , QueryId(RandomNumber<ui64>())
            {}

            virtual ~TQuery() = default;

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason);
            void EndWithSuccess(std::unique_ptr<IEventBase> response);
            TString GetName() const;
            ui64 GetQueryId() const { return QueryId; }
            virtual void Initiate() = 0;

            virtual void OnUpdateBlock(bool /*success*/) {}
            virtual void OnRead(ui64 /*tag*/, NKikimrProto::EReplyStatus /*status*/, TString /*dataOrErrorReason*/) {}
            virtual void OnIdAllocated() {}

        public:
            struct TDeleter {
                static void Destroy(TQuery *query) { delete query; }
            };
        };

        std::deque<std::unique_ptr<IEventHandle>> PendingEventQ;
        TIntrusiveListWithAutoDelete<TQuery, TQuery::TDeleter, TExecutingQueries> ExecutingQueries;

        void HandleStorageProxy(TAutoPtr<IEventHandle> ev);
        TQuery *CreateQuery(TAutoPtr<IEventHandle> ev);
        template<ui32 EventType> TQuery *CreateQuery(std::unique_ptr<IEventHandle> ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TChannelKind
            : NBlobDepot::TChannelKind
        {
            const NKikimrBlobDepot::TChannelKind::E Kind;

            bool IdAllocInFlight = false;

            struct TGivenIdRangeHeapComp;
            using TGivenIdRangePerChannel = THashMap<ui8, TGivenIdRange>;
            TGivenIdRangePerChannel GivenIdRangePerChannel;
            std::vector<TGivenIdRangePerChannel::value_type*> GivenIdRangeHeap;
            ui32 NumAvailableItems = 0;

            std::set<TBlobSeqId> WritesInFlight;

            TIntrusiveList<TQuery, TPendingId> QueriesWaitingForId;

            TChannelKind(NKikimrBlobDepot::TChannelKind::E kind)
                : Kind(kind)
            {}

            void IssueGivenIdRange(const NKikimrBlobDepot::TGivenIdRange& proto);
            ui32 GetNumAvailableItems() const;
            std::optional<TBlobSeqId> Allocate(TBlobDepotAgent& agent);
            std::pair<TLogoBlobID, ui32> MakeBlobId(TBlobDepotAgent& agent, const TBlobSeqId& blobSeqId, EBlobType type,
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

        bool IssueRead(const TValueChain& values, ui64 offset, ui64 size, NKikimrBlobStorage::EGetHandleClass getHandleClass,
            bool mustRestoreFirst, TQuery *query, ui64 tag, bool vg, TString *error);

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
