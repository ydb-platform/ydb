#pragma once

#include "defs.h"
#include "resolved_value.h"

#include <ydb/core/protos/blob_depot_config.pb.h>

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
            Y_DEBUG_ABORT_UNLESS(sp && sp == dynamic_cast<T*>(this));
            return *sp;
        }

        using TPtr = std::shared_ptr<TRequestContext>;
    };

    struct TTabletDisconnected {};

    struct TKeyResolved {
        struct TSuccess {
            const TResolvedValue *Value;
        };
        struct TError {
            TString ErrorReason;
        };
        std::variant<TSuccess, TError> Outcome;

        TKeyResolved(const TResolvedValue *value)
            : Outcome(TSuccess{value})
        {}

        static constexpr struct TResolutionError {} ResolutionError{};

        TKeyResolved(TResolutionError, TString errorReason)
            : Outcome(TError{std::move(errorReason)})
        {}

        bool Error() const { return std::holds_alternative<TError>(Outcome); }
        bool Success() const { return std::holds_alternative<TSuccess>(Outcome); }
        const TResolvedValue *GetResolvedValue() const { return std::get<TSuccess>(Outcome).Value; }

        void Output(IOutputStream& s) const {
            if (auto *success = std::get_if<TSuccess>(&Outcome)) {
                s << (success->Value ? success->Value->ToString() : "<no data>");
            } else if (auto *error = std::get_if<TError>(&Outcome)) {
                s << "Error# '" << EscapeC(error->ErrorReason) << '\'';
            } else {
                Y_ABORT();
            }
        }

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }
    };

    class TRequestSender;

    struct TRequestInFlight
        : TIntrusiveListItem<TRequestInFlight>
        , TNonCopyable
    {
        using TCancelCallback = std::function<void()>;

        const ui64 Id;
        TRequestSender* const Sender = {};
        TRequestContext::TPtr Context;
        TCancelCallback CancelCallback;
        const bool ToBlobDepotTablet = {};

        TRequestInFlight(ui64 id)
            : Id(id)
        {}

        TRequestInFlight(ui64 id, TRequestSender *sender, TRequestContext::TPtr context, TCancelCallback cancelCallback,
                bool toBlobDepotTablet);

        struct THash {
            size_t operator ()(const TRequestInFlight& x) const { return std::hash<ui64>()(x.Id); }
        };

        friend bool operator ==(const TRequestInFlight& x, const TRequestInFlight& y) {
            return x.Id == y.Id;
        }
    };

    class TRequestSender {
        TIntrusiveList<TRequestInFlight> RequestsInFlight;

    protected:
        TBlobDepotAgent& Agent;

        friend class TBlobDepotAgent;
        std::set<std::weak_ptr<TEvBlobStorage::TExecutionRelay>, std::owner_less<std::weak_ptr<TEvBlobStorage::TExecutionRelay>>> SubrequestRelays;

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
        void ClearRequestsInFlight();
        void OnRequestComplete(TRequestInFlight& requestInFlight, TResponse response,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay);

    protected:
        virtual void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) = 0;

    private:
        friend struct TRequestInFlight;
        void RegisterRequestInFlight(TRequestInFlight *requestInFlight);
    };

    struct TReadOutcome {
        struct TOk {
            TRope Data;
        };
        struct TNodata {
        };
        struct TError {
            NKikimrProto::EReplyStatus Status;
            TString ErrorReason;
        };
        std::variant<TOk, TNodata, TError> Value;

        TString ToString() const {
            return std::visit(TOverloaded{
                [](const TOk& value) {
                    return TStringBuilder() << "{Ok Data.size# " << value.Data.size() << "}";
                },
                [](const TNodata& /*value*/) {
                    return TStringBuilder() << "{Nodata}";
                },
                [](const TError& value) {
                    return TStringBuilder() << "{Error Status# " << NKikimrProto::EReplyStatus_Name(value.Status)
                        << " ErrorReason# " << value.ErrorReason.Quote() << "}";
                }
            }, Value);
        }
    };

    class TBlobDepotAgent
        : public TActorBootstrapped<TBlobDepotAgent>
        , public TRequestSender
    {
        const ui32 VirtualGroupId;
        TActorId ProxyId;
        const ui64 AgentInstanceId;
        ui64 TabletId = Max<ui64>();
        TActorId PipeId;
        TActorId PipeServerId;
        bool IsConnected = false;

    private:
        struct TEvPrivate {
            enum {
                EvQueryWatchdog = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvProcessPendingEvent,
                EvPendingEventQueueWatchdog,
                EvPushMetrics,
            };
        };

    public:
        TString LogId;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_AGENT_ACTOR;
        }

        TBlobDepotAgent(ui32 virtualGroupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId);
        ~TBlobDepotAgent();

        void Bootstrap();

#define FORWARD_STORAGE_PROXY(TYPE) fFunc(TEvBlobStorage::TYPE, HandleStorageProxy);
        void StateFunc(STFUNC_SIG) {
            STRICT_STFUNC_BODY(
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
                fFunc(TEvBlobStorage::EvAssimilate, HandleAssimilate);
                hFunc(TEvBlobStorage::TEvBunchOfEvents, Handle);
                cFunc(TEvPrivate::EvProcessPendingEvent, HandleProcessPendingEvent);
                cFunc(TEvPrivate::EvPendingEventQueueWatchdog, HandlePendingEventQueueWatchdog);

                cFunc(TEvPrivate::EvQueryWatchdog, HandleQueryWatchdog);

                cFunc(TEvPrivate::EvPushMetrics, HandlePushMetrics);
            )

            DeletePendingQueries.Clear();
        }
#undef FORWARD_STORAGE_PROXY

        void PassAway() override {
            ClearPendingEventQueue("BlobDepot agent destroyed");
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeId);
            TActor::PassAway();
        }

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
            if (const auto& info = ev->Get()->Info) {
                Y_ABORT_UNLESS(info->BlobDepotId);
                if (TabletId != *info->BlobDepotId) {
                    TabletId = *info->BlobDepotId;
                    LogId = TStringBuilder() << '{' << TabletId << '@' << VirtualGroupId << '}';
                    if (TabletId && TabletId != Max<ui64>()) {
                        ConnectToBlobDepot();
                    }
                }
                if (!info->GetTotalVDisksNum()) {
                    // proxy finishes serving user requests
                    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, ProxyId, {}, nullptr, 0));
                    ProxyId = {};
                }
            }
            if (ProxyId) {
                TActivationContext::Send(ev->Forward(ProxyId));
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Request/response delivery logic

        using TRequestsInFlight = std::unordered_set<TRequestInFlight, TRequestInFlight::THash>;

        ui64 NextTabletRequestId = 1;
        TRequestsInFlight TabletRequestInFlight;
        ui64 NextOtherRequestId = 1;
        TRequestsInFlight OtherRequestInFlight;

        void RegisterRequest(ui64 id, TRequestSender *sender, TRequestContext::TPtr context,
            TRequestInFlight::TCancelCallback cancelCallback, bool toBlobDepotTablet,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay = nullptr);

        template<typename TEvent>
        void HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        template<typename TEvent>
        void HandleOtherResponse(TAutoPtr<TEventHandle<TEvent>> ev);

        void OnRequestComplete(ui64 id, TRequestSender::TResponse response, TRequestsInFlight& map,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay = nullptr);
        void DropTabletRequest(ui64 id);

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
        void OnConnect();
        void OnDisconnect();

        void ProcessResponse(ui64 id, TRequestContext::TPtr context, TResponse response) override;
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvRegisterAgentResult& msg);
        void Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvAllocateIdsResult& msg);

        template<typename T, typename = typename TEvBlobDepot::TEventFor<T>::Type>
        ui64 Issue(T msg, TRequestSender *sender, TRequestContext::TPtr context);

        ui64 Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestContext::TPtr context);

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
            mutable TString QueryIdString;
            const TMonotonic StartTime;
            std::multimap<TMonotonic, TQuery*>::iterator QueryWatchdogMapIter;
            NLog::EPriority WatchdogPriority = NLog::PRI_WARN;
            bool Destroyed = false;
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay;

            static constexpr TDuration WatchdogDuration = TDuration::Seconds(10);

        public:
            TQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event);
            virtual ~TQuery();

            void CheckQueryExecutionTime(TMonotonic now);

            void EndWithError(NKikimrProto::EReplyStatus status, const TString& errorReason);
            void EndWithSuccess(std::unique_ptr<IEventBase> response);
            TString GetName() const;
            TString GetQueryId() const;
            virtual ui64 GetTabletId() const { return 0; }
            virtual void Initiate() = 0;

            virtual void OnUpdateBlock() {}
            virtual void OnRead(ui64 /*tag*/, TReadOutcome&& /*outcome*/) {}
            virtual void OnIdAllocated(bool /*success*/) {}
            virtual void OnDestroy(bool /*success*/) {}

        protected: // reading logic
            struct TReadContext;
            struct TReadArg {
                TResolvedValue Value;
                NKikimrBlobStorage::EGetHandleClass GetHandleClass;
                bool MustRestoreFirst = false;
                ui64 Offset = 0;
                ui64 Size = 0;
                ui64 Tag = 0;
                std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> ReaderTabletData;
                TString Key; // the key we are reading -- this is used for retries when we are getting NODATA
            };

            bool IssueRead(TReadArg&& arg, TString& error);
            void HandleGetResult(const TRequestContext::TPtr& context, TEvBlobStorage::TEvGetResult& msg);
            void HandleResolveResult(const TRequestContext::TPtr& context, TEvBlobDepot::TEvResolveResult& msg);

        public:
            struct TDeleter {
                static void Destroy(TQuery *query) { delete query; }
            };

        private:
            void DoDestroy();
        };

        template<typename TEvent>
        class TBlobStorageQuery : public TQuery {
        public:
            TBlobStorageQuery(TBlobDepotAgent& agent, std::unique_ptr<IEventHandle> event)
                : TQuery(agent, std::move(event))
                , Request(*Event->Get<TEvent>())
            {
                ExecutionRelay = std::move(Request.ExecutionRelay);
            }

        protected:
            TEvent& Request;
        };

        struct TPendingEvent {
            std::unique_ptr<IEventHandle> Event;
            size_t Size;
            TMonotonic ExpirationTimestamp;
        };

        std::deque<TPendingEvent> PendingEventQ;
        size_t PendingEventBytes = 0;
        static constexpr size_t MaxPendingEventBytes = 32'000'000; // ~32 MB
        static constexpr TDuration EventExpirationTime = TDuration::Seconds(60);
        std::multimap<TMonotonic, TQuery*> QueryWatchdogMap;
        TIntrusiveListWithAutoDelete<TQuery, TQuery::TDeleter, TExecutingQueries> ExecutingQueries;
        TIntrusiveListWithAutoDelete<TQuery, TQuery::TDeleter, TExecutingQueries> DeletePendingQueries;
        bool ProcessPendingEventInFlight = false;

        template<ui32 EventType> TQuery *CreateQuery(std::unique_ptr<IEventHandle> ev);
        void HandleStorageProxy(TAutoPtr<IEventHandle> ev);
        void HandleAssimilate(TAutoPtr<IEventHandle> ev);
        void HandlePendingEvent();
        void HandleProcessPendingEvent();
        void ClearPendingEventQueue(const TString& reason);
        void ProcessStorageEvent(std::unique_ptr<IEventHandle> ev);
        void HandlePendingEventQueueWatchdog();
        void Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev);
        void HandleQueryWatchdog();

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
            void ProcessQueriesWaitingForId(bool success);
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
        // Blob mapping cache

        class TBlobMappingCache;
        std::shared_ptr<TBlobMappingCache> BlobMappingCachePtr;
        TBlobMappingCache& BlobMappingCache;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Status flags

        TStorageStatusFlags GetStorageStatusFlags() const;
        float GetApproximateFreeSpaceShare() const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Logging

        TString PrettyKey(const TString& key) const {
            if (VirtualGroupId) {
                return TLogoBlobID::FromBinary(key).ToString();
            } else {
                return EscapeC(key);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Metrics

        ui64 BytesRead = 0;
        ui64 BytesWritten = 0;
        ui64 LastBytesRead = 0;
        ui64 LastBytesWritten = 0;

        void HandlePushMetrics();
    };

#define BDEV_QUERY(MARKER, TEXT, ...) BDEV(MARKER, TEXT, (VG, Agent.VirtualGroupId), (BDT, Agent.TabletId), \
                                      (G, Agent.BlobDepotGeneration), (Q, QueryId), __VA_ARGS__)

} // NKikimr::NBlobDepot
