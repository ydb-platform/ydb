#include "kikimr_lookup_actor.h"
#include "kikimr_lookup_session_pool_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/kqp_lookup_source.pb.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <yql/essentials/utils/yql_panic.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE

using namespace NKikimr;

namespace {
template <typename T>
T ExtractFromConstFuture(const NThreading::TFuture<T>& f) {
    // We want to avoid making a copy of data stored in a future.
    // But there is no direct way to extract data from a const future
    // So, we make a copy of the future, that is cheap. Then, extract the value from this copy.
    // It destructs the value in the original future, but this trick is legal and documented here:
    // https://docs.yandex-team.ru/arcadia-cpp/cookbook/concurrency
    return NThreading::TFuture<T>(f).ExtractValueSync();
}
template <class TProto>
NYql::TIssues IssuesFromProtoMessage(const TProto& message) {
    NYql::TIssues issues;
    IssuesFromMessage(message.issues(), issues);
    return issues;
}

void Backtick(IOutputStream& os, const std::string_view s) {
    os << '`';
    for (auto c: s) {
        switch(c) {
            case '`': case '\\':
                os << '\\';
                [[fallthrough]];
            default:
                os << c;
        }
    }
    os << '`';
}
} // namespace {

namespace NYql::NDq {

namespace {
    // TODO consider moving to lookup parameters (...but likely not)
    constexpr ui32 RetriesLimit = 22;
    constexpr TDuration MinRetryDelay = TDuration::MilliSeconds(10);
    constexpr TDuration MaxRetryDelay = TDuration::Seconds(30);
    // = retry for at most 6 minutes
    constexpr ui64 ChannelBufferSize = 1_MB;
    constexpr ui64 SessionPoolLimit = 5; // arbitrary

    const NKikimr::NMiniKQL::TStructType* MergeStructTypes(const NKikimr::NMiniKQL::TTypeEnvironment& env, const NKikimr::NMiniKQL::TStructType* t1, const NKikimr::NMiniKQL::TStructType* t2) {
        Y_ABORT_UNLESS(t1);
        Y_ABORT_UNLESS(t2);
        NKikimr::NMiniKQL::TStructTypeBuilder resultTypeBuilder{env};
        for (ui32 i = 0; i != t1->GetMembersCount(); ++i) {
            resultTypeBuilder.Add(t1->GetMemberName(i), t1->GetMemberType(i));
        }
        for (ui32 i = 0; i != t2->GetMembersCount(); ++i) {
            resultTypeBuilder.Add(t2->GetMemberName(i), t2->GetMemberType(i));
        }
        return resultTypeBuilder.Build();
    }

    struct TSessionInfoDeleter;
    struct TSessionInfo {
        using TPtr = std::unique_ptr<TSessionInfo, TSessionInfoDeleter>;
        TString SessionId;
        bool Invalidate = false;
    };
    
    struct TSessionInfoDeleter {
        explicit TSessionInfoDeleter(NActors::TActorSystem* actorSystem = TActivationContext::ActorSystem())
            : ActorSystem(actorSystem)
        {
        }

        NActors::TActorSystem* ActorSystem;
        void operator()(TSessionInfo* sessionInfo);
    };

    namespace {
        // Event ids
        enum EEventIds: ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            // TQuerySessionPoolServiceActor
            // public
            EvAcquireSession = EvBegin,
            EvReleaseSession,
            // internal/private
            EvQueryCreateSessionResponse, 
            EvQuerySessionState,

            // TDqSourceKikimrLookupActor
            // public
            EvSessionAcquired,
            EvSessionError,
            // internal/private
            EvQueryExecuteQueryResponsePart,
            EvError,
            EvRetry,
            EvEnd
        };

        static_assert(EEventIds::EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        template <typename TResponse, typename TEvState, enum EEventIds EvId>
        struct TEvStreamResponse: NActors::TEventLocal<TEvStreamResponse<TResponse, TEvState, EvId>, EvId> {
            explicit TEvStreamResponse(TResponse response, TEvState state)
                : State(std::move(state))
                , Response(std::move(response))
            {
            }

            TEvState State;
            TResponse Response;
        };
    }

    class TQuerySessionPoolServiceActor
        : public NActors::TActorBootstrapped<TQuerySessionPoolServiceActor> {
        using TBase = NActors::TActorBootstrapped<TQuerySessionPoolServiceActor>;

        public:
        struct TSessionState {
            using TPtr = std::shared_ptr<TSessionState>;
            explicit TSessionState(NActors::TActorId sender)
                : Sender(sender)
            {}
            NActors::TActorId Sender;
            TString SessionId;
            NRpcService::TStreamReadProcessorPtr<Ydb::Query::SessionState> StreamProcessor;
        };

        struct TEvReleaseSession : NActors::TEventLocal<TEvReleaseSession, EvReleaseSession> {
            explicit TEvReleaseSession(TSessionInfo&& sessionInfo)
                : SessionInfo(std::move(sessionInfo))
            {
            }

            TSessionInfo SessionInfo;
        };

        struct TEvAcquireSession : NActors::TEventLocal<TEvAcquireSession, EvAcquireSession> {
        };

        struct TEvSessionAcquired : NActors::TEventLocal<TEvSessionAcquired, EvSessionAcquired> {
            explicit TEvSessionAcquired(TSessionInfo::TPtr sessionInfo)
                : SessionInfo(std::move(sessionInfo))
            {}
            TSessionInfo::TPtr SessionInfo;
        };

        struct TEvSessionError : NActors::TEventLocal<TEvSessionError, EvSessionError> {
            explicit TEvSessionError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
                : Status(status)
                , Issues(std::move(issues))
            {}
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };

        void Bootstrap() {
            Become(&TQuerySessionPoolServiceActor::StateFunc);
        }

        private:

        // Beware: destroys future value
        template <typename TResponse, enum EEventIds EvId, typename TStatePtr>
        struct TEvQueryResponse: NActors::TEventLocal<TEvQueryResponse<TResponse, EvId, TStatePtr>, EvId> {
            explicit TEvQueryResponse(const NThreading::TFuture<TResponse>& responseFuture, TStatePtr state)
                : State(std::move(state))
            {
                try {
                    Response = ExtractFromConstFuture(responseFuture);
                } catch(std::exception& ex) {
                    Response.set_status(Ydb::StatusIds::INTERNAL_ERROR);
                    auto& issue = *Response.add_issues();
                    issue.set_message(TStringBuilder() << "Got unexpected exception: " << ex.what());
                    // severity is FATAL by default
                }
            }

            TStatePtr State;
            TResponse Response;
        };

        using TEvQueryCreateSessionResponse = TEvQueryResponse<Ydb::Query::CreateSessionResponse, EvQueryCreateSessionResponse, TSessionState::TPtr>;
        using TEvQuerySessionState = TEvStreamResponse<Ydb::Query::SessionState, TSessionState::TPtr, EvQuerySessionState>;
        STRICT_STFUNC_EXC(StateFunc,
            hFunc(TEvAcquireSession, Handle)
            hFunc(TEvReleaseSession, Handle)
            hFunc(TEvQueryCreateSessionResponse, Handle)
            hFunc(TEvQuerySessionState, Handle)
            sFunc(NActors::TEvents::TEvPoison, PassAway)
            , ExceptionFunc(std::exception, HandleException)
        )

        // TODO consider periodic check / forcibly terminate stuck sessions
        // (then again, there are dev ui handle to terminate sessions)

        void HandleException(const std::exception& ex) {
            YDB_LOG_ERROR("Got unexpected exception",
                    {"exception", ex.what()});
            // TODO what can we do here? Except Y_ABORT?
        }

        void SendCreateSession(TSessionState::TPtr state) {
            ++InflightCreateSessions;

            using TRequest = Ydb::Query::CreateSessionRequest;
            using TResponse = Ydb::Query::CreateSessionResponse;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;

            TRequest request;
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), /*database=*/AppData(actorSystem)->TenantName, /*token=*/Nothing(), actorSystem);
            result.Subscribe([actorSystem, selfId, state = std::move(state)](const NThreading::TFuture<TResponse>& future) mutable {
                actorSystem->Send(selfId, new TEvQueryCreateSessionResponse(future, std::move(state)));
            });
        }

        void Handle(TEvAcquireSession::TPtr ev) {
            auto& sender = ev->Sender;
            WaitingQueue.push_back(sender);
            TryEnqueueWaiting();
        }

        void TryEnqueueWaiting() {
            // we have some ready session, serve from them
            while (!ReadySessions.empty() && !WaitingQueue.empty()) {
                auto session = std::move(ReadySessions.back());
                ReadySessions.pop_back();
                auto& sessionId = session->SessionId;
                if (sessionId.empty()) { // drop session from Ready
                    continue;
                }
                auto sender = WaitingQueue.front();
                Cerr << TInstant::Now() << " Take " << sender << Endl;
                WaitingQueue.pop_front();
                SendSession(sender, sessionId, std::move(session));
            }

            while (!WaitingQueue.empty()) {
                // too many sessions: wait until some session released
                if (BusySessions.size() + InflightCreateSessions >= SessionPoolLimit) {
                    Cerr << TInstant::Now() << " Hit pool limit " << BusySessions.size() << '+' << InflightCreateSessions << " " << WaitingQueue.front() << Endl;
                    return;
                }

                // create new session
                auto sender = WaitingQueue.front();
                WaitingQueue.pop_front();
                SendCreateSession(std::make_shared<TSessionState>(sender));
            }
        }

        void SendSession(const NActors::TActorId& sender, const TString& sessionId, TSessionState::TPtr session) {
            auto [_, inserted] = BusySessions.emplace(sessionId, std::move(session));
            Y_VALIDATE(inserted, "BusySession already contains session " << sessionId);
            TSessionInfo::TPtr sessionInfo(new TSessionInfo {
                    .SessionId = sessionId,
                    });
            Send(sender, new TEvSessionAcquired(std::move(sessionInfo)));
        }

        void Handle(TEvReleaseSession::TPtr ev) {
            auto& sessionInfo = ev->Get()->SessionInfo;
            auto it = BusySessions.find(sessionInfo.SessionId);
            Y_VALIDATE(it != BusySessions.end(), "Releasing unexisting session");
            auto& session = it->second;
            if (!WaitingQueue.empty()) {
                // serve and keep as BusySession
                auto sender = std::move(WaitingQueue.front());
                WaitingQueue.pop_front();
                if (sessionInfo.Invalidate) {
                    SendCreateSession(std::make_shared<TSessionState>(sender));
                } else {
                    TSessionInfo::TPtr sessionInfo(new TSessionInfo {
                        .SessionId = session->SessionId,
                    });
                    Cerr << TInstant::Now() << " Pass to " << sender << " " << session->SessionId << Endl;
                    Send(sender, new TEvSessionAcquired(std::move(sessionInfo)));
                    return;
                }
            }
            if (sessionInfo.Invalidate) {
                if (session->SessionId) {
                    SendDeleteSession(std::move(sessionInfo.SessionId));
                }
            } else {
                Cerr << TInstant::Now() << " Ready " << session->SessionId << Endl;
                ReadySessions.push_back(std::move(session));
            }
            BusySessions.erase(it);
        }

        void Handle(TEvQueryCreateSessionResponse::TPtr ev) {
            auto session = std::move(ev->Get()->State);
            auto& response = ev->Get()->Response;
            YDB_LOG_DEBUG("TEvQueryCreateSessionResponse",
                    {"response", response.DebugString()});
            if (auto status = response.status(); status != Ydb::StatusIds::SUCCESS) {
                if (auto sender = session->Sender) {
                    session->Sender = {};
                    Send(sender, new TEvSessionError(status, IssuesFromProtoMessage(response)));
                }
                Y_DEBUG_ABORT_UNLESS(InflightCreateSessions > 0);
                --InflightCreateSessions;
                TryEnqueueWaiting();
                return;
            }
            session->SessionId = std::move(*response.mutable_session_id());
            Cerr << TInstant::Now() << " Created "<< session->SessionId << Endl;
            SendAttachSession(std::move(session));
        }

        void SendDeleteSession(TString sessionId) {
            using TRequest = Ydb::Query::DeleteSessionRequest;
            using TResponse = Ydb::Query::DeleteSessionResponse;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;

            TRequest request;
            request.set_session_id(std::move(sessionId));
            auto actorSystem = TActivationContext::ActorSystem();
            [[maybe_unused]]
            auto selfId = SelfId();
            [[maybe_unused]]
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), /*database=*/AppData(actorSystem)->TenantName, /*token=*/Nothing(), actorSystem);
            // don't wait for results
        }

        void SendAttachSession(TSessionState::TPtr session) {
            using TRequest = Ydb::Query::AttachSessionRequest;
            using TResponse = Ydb::Query::SessionState;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;
            TRequest request;
            request.set_session_id(session->SessionId);
            session->StreamProcessor = NRpcService::DoLocalRpcStreamSameMailbox<TRpcRequest>(std::move(request), /*database*/AppData()->TenantName, /*token*/Nothing(), ActorContext(), false, ChannelBufferSize);
            ReadNextSessionState(std::move(session));
        }

        void ReadNextSessionState(TSessionState::TPtr session) {
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            Y_ABORT_UNLESS(session->StreamProcessor && session->StreamProcessor->HasData());
            session->StreamProcessor->Read([actorSystem, selfId, session = std::move(session)](Ydb::Query::SessionState&& response) mutable {
                actorSystem->Send(selfId, new TEvQuerySessionState(std::move(response), std::move(session)));
            });
        }

        void Handle(TEvQuerySessionState::TPtr ev) {
            auto session = std::move(ev->Get()->State);
            auto& response = ev->Get()->Response;
            YDB_LOG_TRACE("TEvQuerySessionState",
                    {"sessionId", session->SessionId},
                    {"response", response.DebugString()});
            auto status = response.status();
            if (response.has_session_shutdown()) {
                status = Ydb::StatusIds::SESSION_EXPIRED;
            }
            if (response.has_node_shutdown()) {
                status = Ydb::StatusIds::SESSION_EXPIRED; // XXX
            }
            switch(status) {
                case Ydb::StatusIds::SUCCESS:
                    break;

                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::BAD_SESSION:
                    session->SessionId.clear();
                    [[fallthrough]];
                default:
                    CleanupStreamProcessor(session);
                    if (auto sender = session->Sender) {
                        session->Sender = {};
                        Send(sender, new TEvSessionError(status, IssuesFromProtoMessage(response)));
                        Y_DEBUG_ABORT_UNLESS(InflightCreateSessions > 0);
                        --InflightCreateSessions;
                        TryEnqueueWaiting();
                    }
                    return;
            }
            if (auto sender = session->Sender) {
                session->Sender = {};

                Y_DEBUG_ABORT_UNLESS(InflightCreateSessions > 0);
                --InflightCreateSessions;
                Cerr << TInstant::Now() << " Attached "<< session->SessionId << Endl;
                SendSession(sender, session->SessionId, session);
            }
            if (session->StreamProcessor->HasData()) {
                ReadNextSessionState(std::move(session));
            } else {
                FinalizeSession(std::move(session));
            }
        }

        void FinalizeSession(TSessionState::TPtr session) {
            if (auto sender = session->Sender) {
                session->Sender = {};
                Y_DEBUG_ABORT_UNLESS(InflightCreateSessions > 0);
                --InflightCreateSessions;
                // Retries are handled inside lookup actor
                TIssues issues;
                issues.AddIssue(TIssue("Session attach terminated with unknown status"));
                Send(sender, new TEvSessionError(Ydb::StatusIds::UNDETERMINED, std::move(issues)));
                TryEnqueueWaiting();
            }
            YDB_LOG_DEBUG("FinalizeSession",
                    {"sessionId", session->SessionId});
            session->SessionId.clear();
        }

        void CleanupStreamProcessor(TSessionState::TPtr& session) {
            if (auto& streamProcessor = session->StreamProcessor) {
                if (!streamProcessor->IsFinished()) {
                    streamProcessor->Cancel();
                }
                streamProcessor.Reset();
            }
        }
        
        void PassAway() override {
            for (auto sender: WaitingQueue) {
                TIssues issues;
                issues.AddIssue(TIssue("QuerySessionPool actor was terminated"));
                Send(sender, new TEvSessionError(Ydb::StatusIds::CANCELLED, issues));
            }
            for (auto& session: ReadySessions) {
                if (!session->SessionId.empty()) {
                    SendDeleteSession(session->SessionId);
                }
                CleanupStreamProcessor(session);
            }
            for (auto& [_, session]: BusySessions) {
                if (!session->SessionId.empty()) {
                    SendDeleteSession(session->SessionId);
                }
                CleanupStreamProcessor(session);
            }
            TBase::PassAway();
        }

        private:
        std::deque<NActors::TActorId> WaitingQueue;
        TVector<TSessionState::TPtr> ReadySessions;
        // when entry removed from BusySessions or InflightCreateSessions decremented, we should call TryEnqueueWaiting() or directly create/reuse session
        std::unordered_map<TString, TSessionState::TPtr> BusySessions;
        ui64 InflightCreateSessions = 0;
    };

    void TSessionInfoDeleter::operator()(TSessionInfo* sessionInfo) {
        ActorSystem->Send(QuerySessionPoolServiceActorId(), new TQuerySessionPoolServiceActor::TEvReleaseSession(std::move(*sessionInfo)));
        delete sessionInfo;
    }

    class TDqSourceKikimrLookupActor
        : public NYql::NDq::IDqAsyncLookupSource,
          public NActors::TActorBootstrapped<TDqSourceKikimrLookupActor> {
        using TBase = NActors::TActorBootstrapped<TDqSourceKikimrLookupActor>;

        struct TLookupState {
            using TPtr = std::shared_ptr<TLookupState>;
            std::weak_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap> Request;
            // ^^^ must not be lock()ed without bound mkql allocator
            // ^^^ (and allocator must not be bound outside actor context)
            TBackoff Backoff;
            TInstant SentTime;
            size_t FullscanLimit = 0;
            size_t ResultRows = 0;
            TSessionInfo::TPtr SessionInfo;
            std::shared_ptr<arrow::Schema> Schema;
            NRpcService::TStreamReadProcessorPtr<Ydb::Query::ExecuteQueryResponsePart> StreamProcessor;
        };
        using TEvQueryExecuteQueryResponsePart = TEvStreamResponse<Ydb::Query::ExecuteQueryResponsePart, TLookupState::TPtr, EvQueryExecuteQueryResponsePart>;

    private:
        struct TEvLookupRetry : NActors::TEventLocal<TEvLookupRetry, EvRetry> {
            explicit TEvLookupRetry(TLookupState::TPtr state)
                : State(std::move(state))
            {
            }

            TLookupState::TPtr State;
        };

        // SessionHolder is std::shared_ptr<TString, [actorSystem]() {
        //      actorSystem->Send(ServiceActorId(), TEvRelease);
        // }
        //
        // Lookup
        //              Pool
        // SendRequest->EvAcquireSession
        //              Handle(AcquireSession):
        //              1) has session in pool: move to BusySessions & <- EvSessionAcquired with TSessionInfo
        //              2) reached limit: return; Puts sender to WaitingQueue
        //              3) Rpc<Create> -> EvCreated(TState { empty, Sender })
        //
        //              Handle(EvCreate): fill State->Id, attach it
        //              Handle(EvSessionInfo): if State->Sender is not false, <- EvSessionAcquired, put to BusySession, clear State->Sender
        // Handle(EvSessionAcquired): keep SessionInfo in State, start request, etc;
        //  1) Finished: release SessionInfo (will call back Send(TEvRelease))
        //  2) Error: release SessionInfo, optionally set Invalidate on *SESSION* errors (will call back Send(TEvRelease))
        //  3) Something happens in-transmit: event destroyed, SessionInfo destroyed (will call back Send(TEvRelease))
        //               Handle(TEvRelease):
        //               1) if invalidate - destroy session
        //               2) if not invalidate - serve WaitingQueue or return session to pool if empty
    public:
        TDqSourceKikimrLookupActor(
            NActors::TActorId&& parentId,
            ::NMonitoring::TDynamicCounterPtr taskCounters,
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
            std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
            NKqpProto::TDqSourceKikimrLookupSource&& lookupSource,
            const NKikimr::NMiniKQL::TStructType* keyType,
            const NKikimr::NMiniKQL::TStructType* payloadType,
            const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
            const NKikimr::NMiniKQL::THolderFactory& holderFactory,
            const size_t maxKeysInRequest,
            bool isMultiMatches = false)
            : ParentId(std::move(parentId))
            , Alloc(alloc)
            , KeyTypeHelper(keyTypeHelper)
            , LookupSource(std::move(lookupSource))
            , KeyType(keyType)
            , PayloadType(payloadType)
            , SelectResultType(MergeStructTypes(typeEnv, keyType, payloadType))
            , HolderFactory(holderFactory)
            , ColumnDestinations(CreateColumnDestination())
            , MaxKeysInRequest(maxKeysInRequest)
            , IsMultiMatches(isMultiMatches)
            , SelectBody(MakeSelect())
            , SelectWithKeys(MakeSelectWithKeys())
        {
            if (auto token = LookupSource.GetToken(); !token.empty()) {
                Token.emplace(token);
            }
            InitMonCounters(taskCounters);
        }

        ~TDqSourceKikimrLookupActor() {
            Free();
        }

    private:
        void Free() {
            auto guard = Guard(*Alloc);
            KeyTypeHelper.reset();
        }
        void InitMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
            if (!taskCounters) {
                return;
            }
            auto component = taskCounters->GetSubgroup("component", "LookupSrc");
            Count = component->GetCounter("Reqs", true);
            Fullscans = component->GetCounter("Fullscans", true);
            Keys = component->GetCounter("Keys", true);
            ResultRows = component->GetCounter("Rows", true);
            ResultChunks = component->GetCounter("Chunks", true);
            ResultBytes = component->GetCounter("Bytes", true);
            AnswerTime = component->GetCounter("AnswerUs", true);
            CpuTime = component->GetCounter("CpuUs", true);
            InFlight = component->GetCounter("InFlight");
        }
    public:

        void Bootstrap() {
#define COMMON_LOG \
            { "actorId", SelfId() }, \
            { "path", LookupSource.GetPath() }
            YDB_LOG_INFO("New kikimr provider lookup actor",
                    COMMON_LOG,
                    {"database", LookupSource.GetDatabase()},
                    {"parentId", ParentId});
            Become(&TDqSourceKikimrLookupActor::StateFunc);
        }

        static constexpr char ActorName[] = "KIKIMR_PROVIDER_LOOKUP_ACTOR";

    private: // IDqAsyncLookupSource
        size_t GetMaxSupportedKeysInRequest() const override {
            return MaxKeysInRequest;
        }
        size_t GetMaxSupportedFullscanRequest() const override {
            return MaxSupportedFullscanRequest;
        }

        void AsyncLookup(std::weak_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request) override {
            auto guard = Guard(*Alloc);
            CreateRequest(request.lock(), 0);
        }

        void PassAway() override {
            YDB_LOG_DEBUG("PassAway",
                    COMMON_LOG,
                    {"requests", InflightRequests.size()},
            );
            if (InFlight) {
                // If request fails on (unrecoverable) error or cancelled, we may end up with non-zero InFlight
                InFlight->Sub(InflightRequests.size());
            }
            for (auto state: InflightRequests) {
                state->SessionInfo.reset();
                CleanupStreamProcessor(state);
            }
            InflightRequests.clear();
            Free();
            TBase::PassAway();
        }

    private: // events
        STRICT_STFUNC_EXC(StateFunc,
            hFunc(TEvLookupRequest, Handle)
            hFunc(TEvQueryExecuteQueryResponsePart, Handle)
            hFunc(TQuerySessionPoolServiceActor::TEvSessionAcquired, Handle)
            hFunc(TQuerySessionPoolServiceActor::TEvSessionError, Handle)
            hFunc(TEvLookupRetry, Handle)
            sFunc(NActors::TEvents::TEvPoison, PassAway)
            hFunc(NActors::TEvents::TEvUndelivered, Handle)
            , ExceptionFunc(std::exception, HandleException)
        )

        void Handle(TEvLookupRetry::TPtr ev) {
            if (InflightRequests.empty()) { // already passed away
                YDB_LOG_DEBUG("Retry after PassAway", COMMON_LOG);
                return;
            }
            auto guard = Guard(*Alloc);
            auto state = std::move(ev->Get()->State);
            if (state->FullscanLimit > 0) {
                if (auto request = state->Request.lock()) {
                    request->erase(request->begin(), request->end());
                } else {
                    YDB_LOG_DEBUG("Retry: parent MIA", COMMON_LOG);
                    return;
                }
            } else if (IsMultiMatches) {
                if (auto request = state->Request.lock()) {
                    for (auto& [_, value]: *request) {
                        value = NUdf::TUnboxedValue();
                    }
                } else {
                    YDB_LOG_DEBUG("Retry: parent MIA", COMMON_LOG);
                    return;
                }
            }
            state->ResultRows = 0;
            SendRequest(std::move(state));
        }

        void Handle(IDqAsyncLookupSource::TEvLookupRequest::TPtr ev) {
            auto guard = Guard(*Alloc);
            CreateRequest(ev->Get()->Request.lock(), ev->Get()->FullscanLimit);
        }

        static bool IsRetryableError(Ydb::StatusIds::StatusCode status) {
            switch(status) {
                case Ydb::StatusIds::ABORTED:
                case Ydb::StatusIds::UNAVAILABLE:
                case Ydb::StatusIds::OVERLOADED:
                case Ydb::StatusIds::TIMEOUT:
                case Ydb::StatusIds::BAD_SESSION:
                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::CANCELLED:
                case Ydb::StatusIds::UNDETERMINED:
                case Ydb::StatusIds::SESSION_BUSY:
                    return true;
                default:
                    return false;
            }
        }

        void SendRetryOrError(TLookupState::TPtr state, Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
            CleanupStreamProcessor(state);
            state->SessionInfo.reset();
            if (IsRetryableError(status) && state->Backoff.HasMore()) {
                auto delay = state->Backoff.Next();
                YDB_LOG_WARN("Retrievable error",
                    COMMON_LOG,
                    {"issues", issues.ToOneLineString()},
                    {"delay", delay});
                Schedule(delay, new TEvLookupRetry(std::move(state)));
                return;
            }
            auto removed = InflightRequests.erase(state);
            Y_DEBUG_ABORT_UNLESS(removed);
            if (InFlight) { // all counters tied
                InFlight->Sub(removed);
                AnswerTime->Add((TInstant::Now() - state->SentTime).MicroSeconds());
            }
            SendError(status, std::move(issues));
        }

        void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
            SendError(Ydb::StatusIds::INTERNAL_ERROR,
                    TStringBuilder() << "TDqSourceKikimrLookupSource: "
                    << "Undelivered Event " << ev->Get()->SourceType
                    << " from " << SelfId() << " (Self) to " << ev->Sender
                    << " Reason: " << ev->Get()->Reason << " Cookie: " << ev->Cookie
                    << " (service was not started or failed, check logs)");
        }

        void HandleException(const std::exception& ex) {
            SendError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << ex.what());
        }

        void SendError(Ydb::StatusIds::StatusCode status, const TString& issue) {
            NYql::TIssues issues;
            issues.AddIssue(TIssue(issue));
            SendError(status, std::move(issues));
        }

        void SendError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
            YDB_LOG_ERROR("Fatal error",
                COMMON_LOG,
                {"issues", issues.ToOneLineString()});
            Send(ParentId, new IDqComputeActorAsyncInput::TEvAsyncInputError(-1, std::move(issues), YdbStatusToDqStatus(status, EStatusCompatibilityLevel::WithUnauthorized)));
        }

    private:
        static TDuration GetCpuTimeDelta(ui64 startCycleCount) {
            return TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - startCycleCount));
        }

        void CreateRequest(std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request, size_t fullscanLimit) {
            if (!request) {
                YDB_LOG_DEBUG("CreateRequest: parent MIA", COMMON_LOG);
                return;
            }
            Y_DEBUG_ABORT_UNLESS(request->empty() == (fullscanLimit > 0));
            YDB_LOG_DEBUG("Got LookupRequest",
                    COMMON_LOG,
                    {"keysSize", request->size()});
            Y_ABORT_IF((request->empty() == (fullscanLimit == 0)) || request->size() > MaxKeysInRequest);
            if (InFlight) { // all counters tied
                Count->Inc();
                InFlight->Inc();
                Keys->Add(request->size());
                if (fullscanLimit > 0) {
                    Fullscans->Inc();
                }
            }

            auto state = std::make_shared<TLookupState>(TLookupState {
                .Request = request,
                .Backoff = TBackoff(RetriesLimit, MinRetryDelay, MaxRetryDelay),
                .SentTime = TInstant::Now(),
                .FullscanLimit = fullscanLimit
            });
            InflightRequests.insert(state);
            SendRequest(std::move(state));
        }

        // must be called in actor context
        void SendRequest(TLookupState::TPtr state) {
            Y_DEBUG_ABORT_UNLESS(!state->SessionInfo);
            Pending.push_back(std::move(state));
            Send(QuerySessionPoolServiceActorId(), new TQuerySessionPoolServiceActor::TEvAcquireSession(), NActors::IEventHandle::FlagTrackDelivery);
        }

        void Handle(TQuerySessionPoolServiceActor::TEvSessionError::TPtr& ev) {
            Y_VALIDATE(!Pending.empty(), "Error on unrequested session");
            auto state = std::move(Pending.front());
            Pending.pop_front();
            SendRetryOrError(std::move(state), ev->Get()->Status, ev->Get()->Issues);
        }

        void Handle(TQuerySessionPoolServiceActor::TEvSessionAcquired::TPtr& ev) {
            auto startCycleCount = GetCycleCountFast();
            Y_VALIDATE(!Pending.empty(), "Acquired unrequested session");
            auto state = std::move(Pending.front());
            Pending.pop_front();
            state->SessionInfo = std::move(ev->Get()->SessionInfo);

            using TRequest = Ydb::Query::ExecuteQueryRequest;
            using TResponse = Ydb::Query::ExecuteQueryResponsePart;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;
            state->StreamProcessor = NRpcService::DoLocalRpcStreamSameMailbox<TRpcRequest>(FillQuery(state), LookupSource.GetDatabase(), Token, ActorContext(), false, ChannelBufferSize);
            ReadNextResponsePart(state);
            auto cputime = GetCpuTimeDelta(startCycleCount).MicroSeconds();
            if (CpuTime) {
                CpuTime->Add(cputime);
            }
            YDB_LOG_TRACE("SendRequest finished",
                    COMMON_LOG,
                    {"cpuTime", cputime});
        }

        void ReadNextResponsePart(TLookupState::TPtr state) {
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            Y_ABORT_UNLESS(state->StreamProcessor && state->StreamProcessor->HasData());
            state->StreamProcessor->Read([actorSystem, selfId, state = std::move(state)](Ydb::Query::ExecuteQueryResponsePart&& response) mutable {
                actorSystem->Send(selfId, new TEvQueryExecuteQueryResponsePart(std::move(response), std::move(state)));
            });
        }

        void Handle(TEvQueryExecuteQueryResponsePart::TPtr ev) {
            if (InflightRequests.empty()) { // already passed away
                YDB_LOG_DEBUG("TEvQueryExecuteQueryResponsePart after PassAway", COMMON_LOG);
                return;
            }
            auto state = std::move(ev->Get()->State);
            auto& response = ev->Get()->Response;
            YDB_LOG_TRACE("TEvQueryExecuteQueryResponsePart",
                    COMMON_LOG,
                    {"response", response.DebugString()});
            switch(response.status()) {
                case Ydb::StatusIds::SUCCESS:
                    break;

                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::BAD_SESSION:
                    if (state->SessionInfo) {
                        state->SessionInfo->Invalidate = true;
                    }
                    [[fallthrough]];

                default:
                    SendRetryOrError(std::move(state), response.status(), IssuesFromProtoMessage(response));
                    return;
            }
            ProcessReceivedData(response, state);
            if (state->StreamProcessor->HasData()) {
                ReadNextResponsePart(std::move(state));
            } else {
                FinalizeRequest(std::move(state));
            }
        }

        void CleanupStreamProcessor(TLookupState::TPtr& state) {
            if (auto& streamProcessor = state->StreamProcessor) {
                if(!streamProcessor->IsFinished()) {
                    streamProcessor->Cancel();
                }
                streamProcessor.Reset();
            }
        }

        // must be called in actor context
        void ProcessReceivedData(Ydb::Query::ExecuteQueryResponsePart& result, TLookupState::TPtr state) {
            if (result.has_result_set()) {
                Y_ENSURE(result.result_set_index() == 0);
                ProcessReceivedData(result.result_set(), std::move(state));
            }
            if (result.has_tx_meta()) {
                YDB_LOG_TRACE("tx meta",
                        {"txMeta", result.tx_meta().DebugString()});
            }
            if (result.has_exec_stats()) {
                YDB_LOG_DEBUG("query stats",
                        COMMON_LOG,
                        {"queryStats", result.exec_stats().DebugString()});
            }
        }

        // must be called in actor context
        void ProcessReceivedData(const Ydb::ResultSet& resultSet, TLookupState::TPtr state) {
            auto startCycleCount = GetCycleCountFast();
            auto guard = Guard(*Alloc);
            auto request = state->Request.lock();
            if (!request) {
                YDB_LOG_DEBUG("ProcessReceivedData: parent MIA", COMMON_LOG);
                return;
            }
            Y_ENSURE(!resultSet.truncated(), (state->FullscanLimit > 0 ? TStringBuilder() << "Fullscan request for " << state->FullscanLimit << " keys" : TStringBuilder() << "Keyed request for " << request->size() << " keys") << ": truncated result, terminate to avoid data loss");
            if (resultSet.has_arrow_format_meta()) {
                const auto& schema = resultSet.arrow_format_meta().schema();
                if (!schema.empty()) {
                    state->Schema = NKikimr::NArrow::DeserializeSchema(schema);
                    if (ResultBytes) {
                        ResultBytes->Add(schema.size());
                    }
                }
            }
            if (ResultBytes) { // all counters tied
                ResultBytes->Add(resultSet.data().size());
                ResultChunks->Inc();
            }
            NKikimr::NArrow::NSerialization::TSerializerContainer deser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer(); // todo move to class' member
            Y_ENSURE(resultSet.format() == Ydb::ResultSet::FORMAT_ARROW);
            const auto& data = state->Schema ? deser->Deserialize(resultSet.data(), state->Schema) : deser->Deserialize(resultSet.data());
            Y_ENSURE(data.ok(), data.status().ToString());
            const auto& value = data.ValueOrDie();
            Y_ENSURE(static_cast<ui32>(value->num_columns()) == ColumnDestinations.size(), value->num_columns() << " == " << ColumnDestinations.size());
            std::vector<NKikimr::NMiniKQL::TUnboxedValueVector> columns(ColumnDestinations.size());
            for (size_t i = 0; i != columns.size(); ++i) {
                Y_ENSURE(value->column_name(i) == (ColumnDestinations[i].first == EColumnDestination::Key ? KeyType : PayloadType)->GetMemberName(ColumnDestinations[i].second));
                columns[i] = NArrow::ExtractUnboxedValues(value->column(i), SelectResultType->GetMemberType(i), HolderFactory);
            }

            auto height = columns[0].size();
            Y_DEBUG_ABORT_UNLESS(state->FullscanLimit == 0 || state->FullscanLimit >= state->ResultRows);
            if (state->FullscanLimit > 0 && height > state->FullscanLimit - state->ResultRows) {
                CleanupStreamProcessor(state);
                Y_VALIDATE(false, "Result count exceed requested limit " << state->FullscanLimit); // unlike generic lookup/connector, this is an internal bug
            }
            for (size_t i = 0; i != height; ++i) {
                NUdf::TUnboxedValue* keyItems;
                NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(KeyType->GetMembersCount(), keyItems);
                NUdf::TUnboxedValue* outputItems;
                NUdf::TUnboxedValue output = HolderFactory.CreateDirectArrayHolder(PayloadType->GetMembersCount(), outputItems);
                for (size_t j = 0; j != columns.size(); ++j) {
                    (ColumnDestinations[j].first == EColumnDestination::Key ? keyItems : outputItems)[ColumnDestinations[j].second] = columns[j][i];
                }

                NUdf::TUnboxedValue *v;
                if (state->FullscanLimit > 0) {
                    auto [it, _] = request->emplace(key, NUdf::TUnboxedValue{});
                    v = &(it->second);
                } else if (auto it = request->find(key); it != request->end()) {
                    v = &(it->second);
                } else {
                    CleanupStreamProcessor(state);
                    Y_VALIDATE(false, "SELECT returned unrequested keys, should not have happened"); // unlike generic lookup/connector, this is an internal bug
                }
                if (IsMultiMatches) {
                    *v = HolderFactory.CreateDirectListHolder((*v ? *NKikimr::NMiniKQL::GetDefaultListRepresentation(*v) : NKikimr::NMiniKQL::TDefaultListRepresentation{}).Append(std::move(output)));
                } else {
                    *v = std::move(output); // duplicates will be overwritten
                }
            }
            state->ResultRows += height;
            auto cputime = GetCpuTimeDelta(startCycleCount).MicroSeconds();
            if (CpuTime) { // all counters tied
                CpuTime->Add(cputime);
                ResultRows->Add(height);
            }
            YDB_LOG_TRACE("ProcessReceivedData finished",
                    COMMON_LOG,
                    {"rows", height},
                    {"cpuTime", cputime});
        }

        void FinalizeRequest(TLookupState::TPtr state) {
            CleanupStreamProcessor(state);
            auto removed = InflightRequests.erase(state);
            Y_DEBUG_ABORT_UNLESS(removed);
            auto guard = Guard(*Alloc);
            YDB_LOG_DEBUG("Sending lookup results",
                    COMMON_LOG,
                    {"rows", state->ResultRows});
            if (InFlight) { // all counters tied
                AnswerTime->Add((TInstant::Now() - state->SentTime).MicroSeconds());
                InFlight->Sub(removed);
            }
            YDB_LOG_TRACE("AnswerTime",
                    {"duration", (TInstant::Now() - state->SentTime)});
            auto* ev = new IDqAsyncLookupSource::TEvLookupResult(std::move(state->Request), state->ResultRows, state->FullscanLimit);
            state->SessionInfo.reset(); // return session to pool
            state.reset();
            TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(ParentId, SelfId(), ev));
        }

    private:
        enum class EColumnDestination {
            Key,
            Output
        };

        std::vector<std::pair<EColumnDestination, size_t>> CreateColumnDestination() {
            THashMap<TStringBuf, size_t> keyColumns;
            for (ui32 i = 0; i != KeyType->GetMembersCount(); ++i) {
                keyColumns[KeyType->GetMemberName(i)] = i;
            }
            THashMap<TStringBuf, size_t> outputColumns;
            for (ui32 i = 0; i != PayloadType->GetMembersCount(); ++i) {
                outputColumns[PayloadType->GetMemberName(i)] = i;
            }

            std::vector<std::pair<EColumnDestination, size_t>> result(SelectResultType->GetMembersCount());
            for (size_t i = 0; i != result.size(); ++i) {
                if (const auto* p = keyColumns.FindPtr(SelectResultType->GetMemberName(i))) {
                    result[i] = {EColumnDestination::Key, *p};
                } else if (const auto* p = outputColumns.FindPtr(SelectResultType->GetMemberName(i))) {
                    result[i] = {EColumnDestination::Output, *p};
                } else {
                    Y_ABORT();
                }
            }
            return result;
        }

        TString MakeSelect() {
            TStringBuilder out;
            out << "SELECT";
            char sep = ' ';
            for (ui32 i = 0; i != SelectResultType->GetMembersCount(); ++i) {
                out << sep;
                Backtick(out.Out, SelectResultType->GetMemberName(i));
                sep = ',';
            }
            out << "\n  FROM ";
            Backtick(out.Out, LookupSource.GetPath());
            return std::move(out);
        }

        TString MakeSelectWithKeys() {
            TStringBuilder out;
            Y_DEBUG_ABORT_UNLESS(!SelectBody.empty());

            auto columnsCount = KeyType->GetMembersCount();
            Y_ENSURE(columnsCount > 0);
            out << "PRAGMA AnsiInForEmptyOrNullableItemsCollections;\n";
            out << "DECLARE "<< KeyTupleListName << " AS List<";
            if (columnsCount != 1) {
                out << "Tuple<";
            }
            char sep = ' ';
            for (ui32 c = 0; c != columnsCount; ++c) {
                out << sep;
                NUdf::TTypePrinter p(*TypeInfoHelper, KeyType->GetMemberType(c));
                p.Out(out.Out);
                sep = ',';
            }
            if (columnsCount != 1) {
                out << '>';
            }
            out << ">;\n";
            out << SelectBody;
            out << "\n WHERE ";
            if (columnsCount != 1) {
                out << "AsTuple(";
            }
            sep = ' ';
            for (ui32 c = 0; c != columnsCount; ++c) {
                out << sep;
                Backtick(out.Out, KeyType->GetMemberName(c));
                sep = ',';
            }
            if (columnsCount != 1) {
                out << ')';
            }
            out << " IN " << KeyTupleListName;
            return std::move(out);
        }

        TString MakeSelectWithLimit(ui64 limit, ui64 offset = 0) {
            Y_DEBUG_ABORT_UNLESS(!SelectBody.empty());
            TStringBuilder out;

            out << SelectBody;
            out << " LIMIT " << limit;
            if (offset) {
                out << " OFFSET " << offset;
            }
            return std::move(out);
        }

        // must be called only in actor context
        void FillKeyTupleList(Ydb::TypedValue& keyTupleList, TLookupState::TPtr& state) {
            auto guard = Guard(*Alloc);

            auto keyColumnsCount = KeyType->GetMembersCount();
            if (keyColumnsCount != 1) {
                auto& keyTupleTypes = *keyTupleList.mutable_type()->mutable_list_type()->mutable_item()->mutable_tuple_type();
                for (ui32 c = 0; c != keyColumnsCount; ++c) {
                    ExportTypeToProto(KeyType->GetMemberType(c), *keyTupleTypes.add_elements());
                }
            } else {
                auto& keyListType = *keyTupleList.mutable_type()->mutable_list_type()->mutable_item();
                ExportTypeToProto(KeyType->GetMemberType(0), keyListType);
            }
            auto& list = *keyTupleList.mutable_value();
            auto locked = state->Request.lock();
            if (!locked) {
                throw yexception() << "Actor died";
            }
            for (const auto& [keys, _]: *locked) {
                auto& row = *list.add_items();
                for (ui32 c = 0; c != keyColumnsCount; ++c) {
                    auto& value = keyColumnsCount != 1 ? *row.add_items() : row;
                    ExportValueToProto(KeyType->GetMemberType(c), keys.GetElement(c), value);
                }
            }
        }

        // must be called only in actor context
        Ydb::Query::ExecuteQueryRequest FillQuery(TLookupState::TPtr state) {
            Ydb::Query::ExecuteQueryRequest request;
            if (state->FullscanLimit > 0) {
                request.mutable_query_content()->set_text(MakeSelectWithLimit(state->FullscanLimit));
            } else {
                auto& keyTupleList = (*request.mutable_parameters())[KeyTupleListName];
                FillKeyTupleList(keyTupleList, state);
                request.mutable_query_content()->set_text(SelectWithKeys);
            }
            Y_ENSURE(state->SessionInfo);
            Y_ENSURE(state->SessionInfo->SessionId);
            request.set_session_id(state->SessionInfo->SessionId);
            request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
            request.set_result_set_format(Ydb::ResultSet::FORMAT_ARROW);
            request.mutable_arrow_format_settings()->mutable_compression_codec()->set_type(Ydb::Formats::ArrowFormatSettings::CompressionCodec::TYPE_NONE); // local RPC, avoid compression
            request.set_response_part_limit_bytes(ChannelBufferSize);
            // request.set_pool_id(...); // TODO: pass workload manager pool from caller
            request.set_schema_inclusion_mode(Ydb::Query::SCHEMA_INCLUSION_MODE_FIRST_ONLY);
            {
                auto& tx_control = *request.mutable_tx_control();
                tx_control.mutable_begin_tx()->mutable_snapshot_read_only();
                tx_control.set_commit_tx(true);
            }
            YDB_LOG_DEBUG("QueryStatsMode",
                    COMMON_LOG,
                    {"mode", (request.set_stats_mode(Ydb::Query::STATS_MODE_BASIC), "BASIC")}); // intentional side effects, order important
            YDB_LOG_TRACE("QueryStatsMode",
                    {"mode", (request.set_stats_mode(Ydb::Query::STATS_MODE_FULL), "FULL")}); // intentional side effects, order important
            YDB_LOG_TRACE("Query",
                    COMMON_LOG,
                    {"query", request.DebugString()});

            return request;
        }

    private:
        const NActors::TActorId ParentId;
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        std::shared_ptr<TKeyTypeHelper> KeyTypeHelper;
        NKqpProto::TDqSourceKikimrLookupSource LookupSource;
        const NKikimr::NMiniKQL::TStructType* const KeyType;
        const NKikimr::NMiniKQL::TStructType* const PayloadType;
        const NKikimr::NMiniKQL::TStructType* const SelectResultType; // columns from KeyType + PayloadType
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        const std::vector<std::pair<EColumnDestination, size_t>> ColumnDestinations;
        const size_t MaxKeysInRequest;
        const bool IsMultiMatches;
        TMaybe<TString> Token;
        static inline constexpr std::string_view KeyTupleListName = "$keyTupleList"sv;
        NYql::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper = new NKikimr::NMiniKQL::TTypeInfoHelper();
        const TString SelectBody;
        const TString SelectWithKeys;
        TSet<TLookupState::TPtr> InflightRequests; // all active (unanswered) requests
        std::deque<TLookupState::TPtr> Pending; // requests pending receiving SessionInfo

        ::NMonitoring::TDynamicCounters::TCounterPtr Count;
        ::NMonitoring::TDynamicCounters::TCounterPtr Fullscans;
        ::NMonitoring::TDynamicCounters::TCounterPtr Keys;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultRows;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultChunks;
        ::NMonitoring::TDynamicCounters::TCounterPtr AnswerTime;
        ::NMonitoring::TDynamicCounters::TCounterPtr CpuTime;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlight;
        static constexpr size_t MaxSupportedFullscanRequest = 20000;
        friend class TQuerySessionPoolServiceActor;
    };

    } // namespace

    std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateDqSourceKikimrLookupActor(
        NActors::TActorId parentId,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
        NKqpProto::TDqSourceKikimrLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest,
        const bool isMultiMatches
    )
    {
        auto guard = Guard(*alloc);
        const auto actor = new TDqSourceKikimrLookupActor(
            std::move(parentId),
            taskCounters,
            alloc,
            keyTypeHelper,
            std::move(lookupSource),
            keyType,
            payloadType,
            typeEnv,
            holderFactory,
            maxKeysInRequest,
            isMultiMatches);
        return {actor, actor};
    }

    NActors::IActor* CreateQuerySessionPoolActor() {
        return new TQuerySessionPoolServiceActor();
    }
    NActors::TActorId QuerySessionPoolServiceActorId() {
        return NActors::TActorId(0, "kqp_fq_qspsa");
    }

} // namespace NYql::NDq
