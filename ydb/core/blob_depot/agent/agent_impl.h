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
            Y_VERIFY_DEBUG(sp == dynamic_cast<T*>(this));
            return *sp;
        }

        using TPtr = std::shared_ptr<TRequestContext>;
    };

    struct TTabletDisconnected {};

    class TRequestSender {
        THashMap<ui64, TRequestContext::TPtr> RequestsInFlight;

    protected:
        TBlobDepotAgent& Agent;

    public:
        using TResponse = std::variant<
            // internal events
            TTabletDisconnected,

            // tablet responses
            TEvBlobDepot::TEvRegisterAgentResult*,
            TEvBlobDepot::TEvAllocateIdsResult*,
            TEvBlobDepot::TEvBlockResult*,
            TEvBlobDepot::TEvQueryBlocksResult*,
            TEvBlobDepot::TEvCommitBlobSeqResult*,
            TEvBlobDepot::TEvResolveResult*,

            // underlying DS proxy responses
            TEvBlobStorage::TEvGetResult*
        >;

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
        ui64 TabletId = Max<ui64>();
        TActorId PipeId;

    public:
        TBlobDepotAgent(ui32 virtualGroupId)
            : TActor(&TThis::StateFunc)
            , TRequestSender(*this)
            , VirtualGroupId(virtualGroupId)
            , BlocksManager(CreateBlocksManager())
        {
            Y_VERIFY(TGroupID(VirtualGroupId).ConfigurationType() == EGroupConfigurationType::Virtual);
        }

#define FORWARD_STORAGE_PROXY(TYPE) fFunc(TEvBlobStorage::TYPE, HandleStorageProxy);
        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Poison, PassAway);
            hFunc(TEvBlobStorage::TEvConfigureProxy, Handle);

            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            hFunc(TEvBlobDepot::TEvRegisterAgentResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvAllocateIdsResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvBlockResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvQueryBlocksResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvCommitBlobSeqResult, HandleTabletResponse);
            hFunc(TEvBlobDepot::TEvResolveResult, HandleTabletResponse);

            hFunc(TEvBlobStorage::TEvGetResult, Handle);

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
        // BlobDepot and other actor communications

        ui64 NextRequestId = 1;
        THashMap<ui64, TRequestSender*> TabletRequestInFlight;
        THashMap<ui64, TRequestSender*> OtherRequestInFlight;

        void RegisterRequest(ui64 id, TRequestSender *sender, TRequestContext::TPtr context, bool toBlobDepotTablet);

        template<typename TEvent>
        void HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        template<typename TEvent>
        void HandleOtherResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        void HandleResponse(TAutoPtr<IEventHandle> ev, TRequestSender::TResponse response, THashMap<ui64, TRequestSender*>& map);

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
        void HandleRegisterAgentResult(TRequestContext::TPtr context, TEvBlobDepot::TEvRegisterAgentResult& msg);
        void HandleAllocateIdsResult(TRequestContext::TPtr context, TEvBlobDepot::TEvAllocateIdsResult& msg);

        void Issue(NKikimrBlobDepot::TEvBlock msg, TRequestSender *sender, TRequestContext::TPtr context);
        void Issue(NKikimrBlobDepot::TEvResolve msg, TRequestSender *sender, TRequestContext::TPtr context);
        void Issue(NKikimrBlobDepot::TEvQueryBlocks msg, TRequestSender *sender, TRequestContext::TPtr context);

        void Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestContext::TPtr context);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TChannelKind
            : NBlobDepot::TChannelKind
        {
            struct TAllocatedId {
                ui32 Generation;
                ui64 Begin;
                ui64 End;
            };

            std::vector<std::pair<ui8, ui32>> ChannelGroups;

            bool IdAllocInFlight = false;
            std::deque<TAllocatedId> IdQ;
            static constexpr size_t PreallocatedIdCount = 2;

            std::pair<TLogoBlobID, ui32> Allocate(TBlobDepotAgent& agent, EBlobType type, ui32 part, ui32 size) {
                if (IdQ.empty()) {
                    return {};
                }

                auto& item = IdQ.front();
                auto cgsi = TCGSI::FromBinary(item.Generation, *this, item.Begin++);
                if (item.Begin == item.End) {
                    IdQ.pop_front();
                }
                auto id = cgsi.MakeBlobId(agent.TabletId, type, part, size);
                const auto [channel, groupId] = ChannelGroups[ChannelToIndex[cgsi.Channel]];
                Y_VERIFY_DEBUG(channel == cgsi.Channel);
                return {id, groupId};
            }
        };

        THashMap<NKikimrBlobDepot::TChannelKind::E, TChannelKind> ChannelKinds;

        void IssueAllocateIdsIfNeeded(NKikimrBlobDepot::TChannelKind::E channelKind);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TExecutingQueries {};
        struct TPendingBlockChecks {};

        class TQuery
            : public TIntrusiveListItem<TQuery, TExecutingQueries>
            , public TIntrusiveListItem<TQuery, TPendingBlockChecks>
            , public TRequestSender
        {
            friend class TBlobDepotAgent;

            struct TReadContext {
                TQuery *Query;
                ui64 Tag;
                TString Buffer;
                ui64 Size;
                NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
                THashMap<ui64, ui64> ReadsToOffset;
            };
            std::list<TReadContext> Reads;

        protected:
            std::unique_ptr<IEventHandle> Event; // original query event
            const ui64 QueryId;

        public:
            TQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event)
                : TRequestSender(agent)
                , Event(std::move(event))
                , QueryId(RandomNumber<ui64>())
            {}

            virtual ~TQuery() {
                for (const auto& read : Reads) {
                    for (const auto& [id, _] : read.ReadsToOffset) {
                        Agent.ReadsInFlight.erase(id);
                    }
                }
            }

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason);
            void EndWithSuccess(std::unique_ptr<IEventBase> response);
            TString GetName() const;
            ui64 GetQueryId() const { return QueryId; }
            virtual void Initiate() = 0;

            virtual void OnUpdateBlock(bool /*success*/) {}
            virtual void OnRead(ui64 /*tag*/, NKikimrProto::EReplyStatus /*status*/, TString /*dataOrErrorReason*/) {}

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
        // Blocks

        class TBlocksManager;
        struct TBlocksManagerDeleter { void operator ()(TBlocksManager*) const; };
        using TBlocksManagerPtr = std::unique_ptr<TBlocksManager, TBlocksManagerDeleter>;
        TBlocksManagerPtr BlocksManager;

        TBlocksManagerPtr CreateBlocksManager();

        NKikimrProto::EReplyStatus CheckBlockForTablet(ui64 tabletId, ui32 generation, TQuery *query,
            ui32 *blockedGeneration = nullptr);

        ui32 GetBlockForTablet(ui64 tabletId);

        void SetBlockForTablet(ui64 tabletId, ui32 blockedGeneration, TMonotonic expirationTimestamp);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Reading

        ui64 NextReadId = 1;
        THashMap<ui64, std::list<TQuery::TReadContext>::iterator> ReadsInFlight;

        bool IssueRead(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TValueChain>& values, ui64 offset, ui64 size,
            NKikimrBlobStorage::EGetHandleClass getHandleClass, bool mustRestoreFirst, TQuery *query, ui64 tag,
            bool vg, TString *error);

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Status flags

        TStorageStatusFlags GetStorageStatusFlags() const;
    };

} // NKikimr::NBlobDepot
