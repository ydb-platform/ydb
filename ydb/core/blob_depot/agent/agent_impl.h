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

    class TBlobDepotAgent : public TActor<TBlobDepotAgent> {
        const ui32 VirtualGroupId;
        ui64 TabletId = Max<ui64>();
        TActorId PipeId;

    public:
        TBlobDepotAgent(ui32 virtualGroupId)
            : TActor(&TThis::StateFunc)
            , VirtualGroupId(virtualGroupId)
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
        // BlobDepot communications

        using TRequestCompleteCallback = std::function<bool(IEventBase*)>;

        class TRequestSender {
            THashSet<ui64> IdsInFlight;

        protected:
            TBlobDepotAgent& Agent;

        public:
            TRequestSender(TBlobDepotAgent& agent)
                : Agent(agent)
            {}

            ~TRequestSender() {
                for (const ui64 id : IdsInFlight) {
                    const size_t num = Agent.RequestInFlight.erase(id);
                    Y_VERIFY(num);
                }
            }

            void RegisterRequest(ui64 id) {
                const auto [_, inserted] = IdsInFlight.insert(id);
                Y_VERIFY(inserted);
            }

            void OnRequestComplete(ui64 id) {
                const size_t num = IdsInFlight.erase(id);
                Y_VERIFY(num);
            }
        };

        struct TRequestInFlight {
            TRequestSender *Sender;
            TRequestCompleteCallback Callback;
        };

        ui64 NextRequestId = 1;
        THashMap<ui64, TRequestInFlight> RequestInFlight;

        void RegisterRequest(ui64 id, TRequestSender *sender, TRequestCompleteCallback callback);

        template<typename TEvent>
        void HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        bool Registered = false;
        ui32 BlobDepotGeneration = 0;

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void ConnectToBlobDepot();
        void OnDisconnect();

        void Issue(NKikimrBlobDepot::TEvBlock msg, TRequestSender *sender, TRequestCompleteCallback callback);
        void Issue(NKikimrBlobDepot::TEvResolve msg, TRequestSender *sender, TRequestCompleteCallback callback);
        void Issue(NKikimrBlobDepot::TEvQueryBlocks msg, TRequestSender *sender, TRequestCompleteCallback callback);

        void Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestCompleteCallback callback);

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

            std::pair<TLogoBlobID, ui32> Allocate(TBlobDepotAgent& agent, ui32 size, ui32 type) {
                if (IdQ.empty()) {
                    return {};
                }

                auto& item = IdQ.front();
                auto cgsi = TCGSI::FromBinary(item.Generation, *this, item.Begin++);
                if (item.Begin == item.End) {
                    IdQ.pop_front();
                }
                static constexpr ui32 typeBits = 24 - TCGSI::IndexBits;
                Y_VERIFY(type < (1 << typeBits));
                const ui32 cookie = cgsi.Index << typeBits | type;
                const TLogoBlobID id(agent.TabletId, cgsi.Generation, cgsi.Step, cgsi.Channel, size, cookie);
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

        class TExecutingQuery
            : public TIntrusiveListItem<TExecutingQuery, TExecutingQueries>
            , public TIntrusiveListItem<TExecutingQuery, TPendingBlockChecks>
            , public TRequestSender
        {
        protected:
            std::unique_ptr<IEventHandle> Event; // original query event
            const ui64 QueryId;

        public:
            TExecutingQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event)
                : TRequestSender(agent)
                , Event(std::move(event))
                , QueryId(RandomNumber<ui64>())
            {}

            virtual ~TExecutingQuery() = default;

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason);
            void EndWithSuccess(std::unique_ptr<IEventBase> response);
            TString GetName() const;
            ui64 GetQueryId() const { return QueryId; }
            virtual void Initiate() = 0;

            virtual void OnUpdateBlock() {}

        public:
            struct TDeleter {
                static void Destroy(TExecutingQuery *query) { delete query; }
            };
        };

        std::deque<std::unique_ptr<IEventHandle>> PendingEventQ;
        TIntrusiveListWithAutoDelete<TExecutingQuery, TExecutingQuery::TDeleter, TExecutingQueries> ExecutingQueries;

        void HandleStorageProxy(TAutoPtr<IEventHandle> ev);
        TExecutingQuery *CreateExecutingQuery(TAutoPtr<IEventHandle> ev);
        template<ui32 EventType> TExecutingQuery *CreateExecutingQuery(std::unique_ptr<IEventHandle> ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Blocks

        struct TBlockInfo {
            ui32 BlockedGeneration;
            TMonotonic ExpirationTimestamp; // not valid after
            bool RefreshInFlight = false;
            TIntrusiveList<TExecutingQuery, TPendingBlockChecks> PendingBlockChecks;
        };

        std::unordered_map<ui64, TBlockInfo> Blocks;

        NKikimrProto::EReplyStatus CheckBlockForTablet(ui64 tabletId, ui32 generation, TExecutingQuery *query);
    };

} // NKikimr::NBlobDepot
