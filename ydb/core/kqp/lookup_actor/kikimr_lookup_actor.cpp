#include "kikimr_lookup_actor.h"

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
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <yql/essentials/utils/yql_panic.h>

#define LOG_T_AS(ctx, s) LOG_TRACE_S(ctx, NKikimrServices::KQP_COMPUTE, s)
#define LOG_T(s) LOG_T_AS(*NActors::TlsActivationContext, this->LogPrefix << s)
#define LOG_D_AS(ctx, s) LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPUTE, s)
#define LOG_D(s) LOG_D_AS(*NActors::TlsActivationContext, this->LogPrefix << s)
#define LOG_I_AS(ctx, s) LOG_INFO_S(ctx, NKikimrServices::KQP_COMPUTE, s)
#define LOG_I(s) LOG_I_AS(*NActors::TlsActivationContext, this->LogPrefix << s)
#define LOG_W_AS(ctx, s) LOG_WARN_S(ctx, NKikimrServices::KQP_COMPUTE, s)
#define LOG_W(s) LOG_W_AS(*NActors::TlsActivationContext, this->LogPrefix << s)
#define LOG_E_AS(ctx, s) LOG_ERROR_S(ctx, NKikimrServices::KQP_COMPUTE, s)
#define LOG_E(s) LOG_E_AS(*NActors::TlsActivationContext, this->LogPrefix << s)

using namespace NKikimr;

namespace {
    constexpr ui64 channelBufferSize = 1_MB; // XXX FIXME
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

    using namespace NActors;

    namespace {
        constexpr ui32 RetriesLimit = 15; // TODO lookup parameters or PRAGMA?
        constexpr TDuration MinRetryDelay = TDuration::MilliSeconds(10);
        constexpr TDuration MaxRetryDelay = TDuration::Seconds(30); // TODO lookup parameters or PRAGMA?
                                                                    // = at most 6 minutes
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

    } // namespace

    class TKikimrLookupActor
        : public NYql::NDq::IDqAsyncLookupSource,
          public NActors::TActorBootstrapped<TKikimrLookupActor> {
        using TBase = NActors::TActorBootstrapped<TKikimrLookupActor>;

        struct TSessionState;

        struct TLookupState {
            using TPtr = std::shared_ptr<TLookupState>;
            std::weak_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap> Request;
            // ^^^ must not be lock()ed without bound mkql allocator (e.g. in future handlers)
            TBackoff Backoff;
            TInstant SentTime;
            size_t FullscanLimit = 0;
            size_t ResultRows = 0;
            // Query
            std::shared_ptr<TSessionState> SessionState; // avoid circular ownership
            std::shared_ptr<arrow::Schema> Schema;
            NRpcService::TStreamReadProcessorPtr<Ydb::Query::ExecuteQueryResponsePart> StreamProcessor;
        };

        struct TSessionState {
            using TPtr = std::shared_ptr<TSessionState>;
            TBackoff Backoff;
            TString SessionId;
            NRpcService::TStreamReadProcessorPtr<Ydb::Query::SessionState> StreamProcessor;
            TLookupState::TPtr PendingLookup; // avoid circular ownership, either PendingLookup or PendingLookup->SessionState must be nullptr
        };

        // Event ids
        enum EEventIds: ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvYdbExecuteDataQueryResponse = EvBegin,
            EvYdbCreateSessionResponse,
            EvQueryCreateSessionResponse,
            EvQuerySessionState,
            EvQueryExecuteQueryResponsePart,
            EvError,
            EvRetry,
            EvEnd
        };

        static_assert(EEventIds::EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        // Beware: destroys future value
        template <typename TResponse, enum EEventIds EvId>
        struct TEvQueryResponse: NActors::TEventLocal<TEvQueryResponse<TResponse, EvId>, EvId> {
            explicit TEvQueryResponse(const NThreading::TFuture<TResponse>& responseFuture, TLookupState::TPtr state)
                : State(std::move(state))
            {
                try {
                    Response = ExtractFromConstFuture(responseFuture);
                } catch(std::exception& ex) {
                    Response.set_status(Ydb::StatusIds::INTERNAL_ERROR);
                    auto& issue = *Response.add_issues();
                    issue.set_message(TStringBuilder() << "Got unexpected exception: " << ex.what());
                }
            }

            TLookupState::TPtr State;
            TResponse Response;
        };

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
        using TEvQueryCreateSessionResponse = TEvQueryResponse<Ydb::Query::CreateSessionResponse, EvQueryCreateSessionResponse>;
        using TEvQuerySessionState = TEvStreamResponse<Ydb::Query::SessionState, TSessionState::TPtr, EvQuerySessionState>;
        using TEvQueryExecuteQueryResponsePart = TEvStreamResponse<Ydb::Query::ExecuteQueryResponsePart, TLookupState::TPtr, EvQueryExecuteQueryResponsePart>;

    private:
        TString LogPrefix;

        struct TEvLookupRetry : NActors::TEventLocal<TEvLookupRetry, EvRetry> {
            explicit TEvLookupRetry(TLookupState::TPtr state)
                : State(std::move(state))
            {
            }

            TLookupState::TPtr State;
        };

    public:
        TKikimrLookupActor(
            NActors::TActorId&& parentId,
            ::NMonitoring::TDynamicCounterPtr taskCounters,
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
            std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
            NKqpProto::TKikimrLookupSource&& lookupSource,
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
        {
            InitMonCounters(taskCounters);
            {
                TStringBuilder out;
                MakeSelectWithKeys(out);
                SelectWithKeys = std::move(out);
            }
        }

        ~TKikimrLookupActor() {
            Free();
        }

    private:
        void Free() {
            auto guard = Guard(*Alloc);
            if (InFlight) {
                // If request fails on (unrecoverable) error or cancelled, we may end up with non-zero InFlight
                InFlight->Sub(LocalInFlight);
            }
            LocalInFlight = 0;
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
            ActiveSessions = component->GetCounter("Sessions");
        }
    public:

        void Bootstrap() {
            auto path = LookupSource.GetPath();
            LogPrefix += TStringBuilder() << "ActorId=" << SelfId() << " Path=" << path << " ";
            LOG_I("New kikimr provider lookup actor, ParentId=" << ParentId);
            Become(&TKikimrLookupActor::StateFunc);
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
            for (auto&& session: Sessions) {
                CleanupStreamProcessor(session);
                SendDeleteSession(std::move(session->SessionId));
            }
            Sessions.clear();
            Free();
            TBase::PassAway();
        }

    private: // events
        STRICT_STFUNC_EXC(StateFunc,
            hFunc(TEvLookupRequest, Handle)
            hFunc(TEvQueryExecuteQueryResponsePart, Handle)
            hFunc(TEvQueryCreateSessionResponse, Handle)
            hFunc(TEvQuerySessionState, Handle)
            hFunc(TEvLookupRetry, Handle)
            hFunc(NActors::TEvents::TEvPoison, Handle)
            , ExceptionFunc(std::exception, HandleException)
        )

        void Handle(TEvLookupRetry::TPtr ev) {
            if (LocalInFlight == 0) { // already passed away
                LOG_D("Retry after PassAway");
                return;
            }
            auto guard = Guard(*Alloc);
            auto state = std::move(ev->Get()->State);
            if (state->FullscanLimit > 0) {
                if (auto request = state->Request.lock()) {
                    request->erase(request->begin(), request->end());
                } else {
                    LOG_D("Retry: parent MIA");
                    return;
                }
            } else if (IsMultiMatches) {
                if (auto request = state->Request.lock()) {
                    for (auto& [_, value]: *request) {
                        value = NUdf::TUnboxedValue();
                    }
                } else {
                    LOG_D("Retry: parent MIA");
                    return;
                }
            }
            state->ResultRows = 0;
            SendRequest(std::move(state));
        }

        void Handle(NActors::TEvents::TEvPoison::TPtr) {
            PassAway();
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
            if (IsRetryableError(status) && state->Backoff.HasMore()) {
                auto delay = state->Backoff.Next();
                LOG_W("Retrievable error " << issues.ToOneLineString() << ", schedule retry in " << delay);
                Schedule(delay, new TEvLookupRetry(std::move(state)));
                return;
            }
            CleanupStreamProcessor(state);
            if (auto& session = state->SessionState) {
                CleanupStreamProcessor(session);
                session.reset();
            }
            SendError(status, std::move(issues));
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
            LOG_E("Fatal error " << issues.ToOneLineString());
            Send(ParentId, new IDqComputeActorAsyncInput::TEvAsyncInputError(-1, std::move(issues), YdbStatusToDqStatus(status, EStatusCompatibilityLevel::WithUnauthorized)));
        }

    private:
        static TDuration GetCpuTimeDelta(ui64 startCycleCount) {
            return TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - startCycleCount));
        }

        void CreateRequest(std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request, size_t fullscanLimit) {
            if (!request) {
                LOG_D("CreateRequest: parent MIA");
                return;
            }
            Y_DEBUG_ABORT_UNLESS(request->empty() == (fullscanLimit > 0));
            LOG_D("Got LookupRequest for " << request->size() << " keys");
            Y_ABORT_IF((request->empty() == (fullscanLimit == 0)) || request->size() > MaxKeysInRequest);
            if (Count) {
                Count->Inc();
                InFlight->Inc();
                Keys->Add(request->size());
                if (fullscanLimit > 0) {
                    Fullscans->Inc();
                }
            }
            ++LocalInFlight;

            auto state = std::make_shared<TLookupState>(TLookupState {
                .Request = request,
                .Backoff = TBackoff(RetriesLimit, MinRetryDelay, MaxRetryDelay),
                .SentTime = TInstant::Now(),
                .FullscanLimit = fullscanLimit
            });
            SendRequest(std::move(state));
        }

        // must be called with bound Alloc
        void SendRequest(TLookupState::TPtr state) {
            auto startCycleCount = GetCycleCountFast();

            if (!state->SessionState) { // reuse or create session
                if (Sessions.empty()) {
                    SendCreateSession(std::move(state));
                    return;
                }
                state->SessionState = std::move(Sessions.back());
                Sessions.pop_back();
            }

            using TRequest = Ydb::Query::ExecuteQueryRequest;
            using TResponse = Ydb::Query::ExecuteQueryResponsePart;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;
            state->StreamProcessor = NRpcService::DoLocalRpcStreamSameMailbox<TRpcRequest>(FillQuery(state), /*database*/AppData()->TenantName, /*token=*/Nothing(), ActorContext(), false, channelBufferSize);
            ReadNextResponsePart(state);
            auto cputime = GetCpuTimeDelta(startCycleCount).MicroSeconds();
            if (CpuTime) {
                CpuTime->Add(cputime);
            }
        }

        void ReadNextResponsePart(TLookupState::TPtr state) {
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            Y_ABORT_UNLESS(state->StreamProcessor && state->StreamProcessor->HasData());
            state->StreamProcessor->Read([actorSystem, selfId, state = std::move(state)](Ydb::Query::ExecuteQueryResponsePart&& response) {
                actorSystem->Send(selfId, new TEvQueryExecuteQueryResponsePart(std::move(response), std::move(state)));
            });
        }

        void Handle(TEvQueryExecuteQueryResponsePart::TPtr ev) {
            auto state = std::move(ev->Get()->State);
            auto& response = ev->Get()->Response;
            LOG_T("TEvQueryExecuteQueryResponsePart: " << response.DebugString());
            switch(response.status()) {
                case Ydb::StatusIds::SUCCESS:
                    break;

                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::BAD_SESSION:
                    if (auto& sessionState = state->SessionState) {
                        CleanupStreamProcessor(sessionState);
                        sessionState.reset();
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

        void SendAttachSession(TSessionState::TPtr session) {
            using TRequest = Ydb::Query::AttachSessionRequest;
            using TResponse = Ydb::Query::SessionState;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;
            TRequest request;
            request.set_session_id(session->SessionId);
            session->StreamProcessor = NRpcService::DoLocalRpcStreamSameMailbox<TRpcRequest>(std::move(request), /*database*/AppData()->TenantName, /*token=*/Nothing(), ActorContext(), false, channelBufferSize);
            if (ActiveSessions) {
                ActiveSessions->Inc();
            }
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
            LOG_D("TEvQuerySessionState: " << response.DebugString());
            auto status = response.status();
            if (response.has_session_shutdown()) {
                status = Ydb::StatusIds::SESSION_EXPIRED;
            }
            if (response.has_node_shutdown()) {
                status = Ydb::StatusIds::SESSION_EXPIRED; // XXX
            }
            switch(status) {
                case Ydb::StatusIds::SUCCESS:
                    if (auto& lookup = session->PendingLookup) {
                        // send request (once) upon successful attach
                        lookup->SessionState = session;
                        SendRequest(std::exchange(lookup, {}));
                    }
                    break;

                case Ydb::StatusIds::SESSION_EXPIRED:
                case Ydb::StatusIds::BAD_SESSION:
                    session->SessionId.clear();
                    [[fallthrough]];
                default:
                    CleanupStreamProcessor(session);
                    if (auto& lookup = session->PendingLookup) {
                        SendRetryOrError(std::exchange(lookup, {}), response.status(), IssuesFromProtoMessage(response));
                    }
                    return;
            }
            if (session->StreamProcessor->HasData()) {
                ReadNextSessionState(std::move(session));
            } else {
                FinalizeSession(std::move(session));
            }
        }

        void FinalizeSession(TSessionState::TPtr state) {
            state->SessionId.clear();
        }

        void CleanupStreamProcessor(TSessionState::TPtr& session) {
            if (auto& streamProcessor = session->StreamProcessor) {
                if (!streamProcessor->IsFinished()) {
                    streamProcessor->Cancel();
                }
                streamProcessor.Reset();
                if (ActiveSessions) {
                    ActiveSessions->Dec();
                }
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

        void SendCreateSession(TLookupState::TPtr state) {
            using TRequest = Ydb::Query::CreateSessionRequest;
            using TResponse = Ydb::Query::CreateSessionResponse;
            using TRpcRequest = NGRpcService::TGrpcRequestNoOperationCall<TRequest, TResponse>;

            TRequest request;
            auto actorSystem = TActivationContext::ActorSystem();
            auto selfId = SelfId();
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), /*database=*/AppData()->TenantName, /*token=*/Nothing(), actorSystem);
            result.Subscribe([actorSystem, selfId, state = std::move(state)] (const NThreading::TFuture<TResponse>& future) mutable {
                actorSystem->Send(selfId, new TEvQueryCreateSessionResponse(future, std::move(state)));
            });
        }

        void Handle(TEvQueryCreateSessionResponse::TPtr ev) {
            auto state = std::move(ev->Get()->State);
            Y_ENSURE(!state->SessionState);
            auto& response = ev->Get()->Response;
            LOG_D("TEvQueryCreateSessionResponse: " << response.DebugString());
            if (response.status() != Ydb::StatusIds::SUCCESS) {
                SendRetryOrError(std::move(state), response.status(), IssuesFromProtoMessage(response));
                return;
            }
            auto sessionState = std::make_shared<TSessionState>();
            sessionState->SessionId = std::move(*response.mutable_session_id());
            sessionState->PendingLookup = std::move(state);
            SendAttachSession(std::move(sessionState));
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
            auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), /*database=*/AppData()->TenantName, /*token=*/Nothing(), actorSystem);
            // don't wait for results
        }

        void ProcessReceivedData(Ydb::Query::ExecuteQueryResponsePart& result, TLookupState::TPtr state) {
            Y_ENSURE(result.result_set_index() == 0);
            ProcessReceivedData(result.result_set(), std::move(state));
            LOG_T("tx meta: " << result.tx_meta().DebugString());
            LOG_D("query stats: " << result.exec_stats().DebugString());
        }

        void ProcessReceivedData(const Ydb::ResultSet& resultSet, TLookupState::TPtr state) {
            auto startCycleCount = GetCycleCountFast();
            auto guard = Guard(*Alloc);
            auto request = state->Request.lock();
            if (!request) {
                LOG_D("ProcessReceivedData: parent MIA");
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
            if (ResultBytes) {
                ResultBytes->Add(resultSet.data().size());
                ResultChunks->Inc();
            }
            NKikimr::NArrow::NSerialization::TSerializerContainer deser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer(); // todo move to class' member
            Y_ENSURE(resultSet.format() == Ydb::ResultSet::FORMAT_ARROW);
            const auto& data = deser->Deserialize(resultSet.data(), state->Schema);
            Y_ENSURE(data.ok(), data.status().ToString());
            const auto& value = data.ValueOrDie();
            Y_ENSURE(static_cast<ui32>(value->num_columns()) == ColumnDestinations.size(), value->num_columns() << " == " << ColumnDestinations.size());
            std::vector<NKikimr::NMiniKQL::TUnboxedValueVector> columns(ColumnDestinations.size());
            for (size_t i = 0; i != columns.size(); ++i) {
                Y_ENSURE(value->column_name(i) == (ColumnDestinations[i].first == EColumnDestination::Key ? KeyType : PayloadType)->GetMemberName(ColumnDestinations[i].second));
                columns[i] = NArrow::ExtractUnboxedValues(value->column(i), SelectResultType->GetMemberType(i), HolderFactory);
            }

            auto height = columns[0].size();
            Y_DEBUG_ABORT_UNLESS(state->FullscanLimit == 0 || state->FullscanLimit > state->ResultRows);
            if (state->FullscanLimit > 0 && height > state->FullscanLimit - state->ResultRows) {
                Y_VALIDATE(false, "Result count exceed requested limit " << state->FullscanLimit); // unlike generic lookup/connector, this is an internal bug
                height = state->FullscanLimit - state->ResultRows;
                if (!state->StreamProcessor->IsFinished()) {
                    state->StreamProcessor->Cancel();
                }
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
                    continue;
                }
                if (IsMultiMatches) {
                    *v = HolderFactory.CreateDirectListHolder((*v ? *NKikimr::NMiniKQL::GetDefaultListRepresentation(*v) : NKikimr::NMiniKQL::TDefaultListRepresentation{}).Append(std::move(output)));
                } else {
                    *v = std::move(output); // duplicates will be overwritten
                }
            }
            state->ResultRows += height;
            auto cputime = GetCpuTimeDelta(startCycleCount).MicroSeconds();
            if (CpuTime) {
                CpuTime->Add(cputime);
                ResultRows->Add(height);
            }
            LOG_T("ProcessReceivedData cputime " << cputime << " for " << height << " rows");
        }

        void FinalizeRequest(TLookupState::TPtr state) {
            CleanupStreamProcessor(state);
            if (LocalInFlight == 0) { // PassAway was called
                return;
            }
            --LocalInFlight;
            auto guard = Guard(*Alloc);
            LOG_D("Sending lookup results for " << state->ResultRows << " rows");
            if (AnswerTime) {
                AnswerTime->Add((TInstant::Now() - state->SentTime).MicroSeconds());
                InFlight->Dec();
            }
            LOG_T("AnswerTime " << (TInstant::Now() - state->SentTime));
            auto* ev = new IDqAsyncLookupSource::TEvLookupResult(std::move(state->Request), state->ResultRows, state->FullscanLimit);
            if (auto& session = state->SessionState) {
                if (session->SessionId) { // return Session to the pool
                    Sessions.push_back(std::move(state->SessionState));
                }
            }
            state->SessionState.reset();
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

        void MakeSelect(TStringBuilder& out) {
            out << "SELECT";
            char sep = ' ';
            for (ui32 i = 0; i != SelectResultType->GetMembersCount(); ++i) {
                out << sep;
                Backtick(out.Out, SelectResultType->GetMemberName(i));
                sep = ',';
            }
            out << "\n  FROM ";
            Backtick(out.Out, LookupSource.GetPath());
        }

        void MakeSelectWithKeys(TStringBuilder& out) {
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
            MakeSelect(out);
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
        }

        void MakeSelectWithLimit(TStringBuilder& out, ui64 limit, ui64 offset = 0) {
            MakeSelect(out);
            out << " LIMIT " << limit;
            if (offset) {
                out << " OFFSET " << offset;
            }
        }

        // must be called with bound Alloc
        void FillKeyTupleList(Ydb::TypedValue& keyTupleList, TLookupState::TPtr& state) {
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

        // must be called with bound Alloc
        Ydb::Query::ExecuteQueryRequest FillQuery(TLookupState::TPtr state) {
            Ydb::Query::ExecuteQueryRequest request;
            if (state->FullscanLimit > 0) {
                TStringBuilder out;
                MakeSelectWithLimit(out, state->FullscanLimit);
                request.mutable_query_content()->set_text(std::move(out));
            } else {
                auto& keyTupleList = (*request.mutable_parameters())[KeyTupleListName];
                FillKeyTupleList(keyTupleList, state);
                request.mutable_query_content()->set_text(SelectWithKeys);
            }
            Y_ENSURE(state->SessionState);
            request.set_session_id(state->SessionState->SessionId);
            request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
            request.set_result_set_format(Ydb::ResultSet::FORMAT_ARROW);
            request.mutable_arrow_format_settings()->mutable_compression_codec()->set_type(Ydb::Formats::ArrowFormatSettings::CompressionCodec::TYPE_NONE); // as local rpc
            request.set_schema_inclusion_mode(Ydb::Query::SCHEMA_INCLUSION_MODE_FIRST_ONLY);
            {
                auto& tx_control = *request.mutable_tx_control();
                tx_control.mutable_begin_tx()->mutable_snapshot_read_only();
                tx_control.set_commit_tx(true);
            }
            LOG_D("QueryStatsMode : " << (request.set_stats_mode(Ydb::Query::STATS_MODE_BASIC), "BASIC")); // intentional side effects
            LOG_T("QueryStatsMode : " << (request.set_stats_mode(Ydb::Query::STATS_MODE_FULL), "FULL")); // intentional side effects
            LOG_T("Query: " << request.DebugString());

            return request;
        }

    private:
        const NActors::TActorId ParentId;
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        std::shared_ptr<TKeyTypeHelper> KeyTypeHelper;
        NKqpProto::TKikimrLookupSource LookupSource;
        const NKikimr::NMiniKQL::TStructType* const KeyType;
        const NKikimr::NMiniKQL::TStructType* const PayloadType;
        const NKikimr::NMiniKQL::TStructType* const SelectResultType; // columns from KeyType + PayloadType
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        const std::vector<std::pair<EColumnDestination, size_t>> ColumnDestinations;
        const size_t MaxKeysInRequest;
        const bool IsMultiMatches;
        ui32 LocalInFlight = 0;
        static inline constexpr std::string_view KeyTupleListName = "$keyTupleList"sv;
        NYql::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper = new NKikimr::NMiniKQL::TTypeInfoHelper();
        TString SelectWithKeys;
        TVector<TSessionState::TPtr> Sessions;

        ::NMonitoring::TDynamicCounters::TCounterPtr Count;
        ::NMonitoring::TDynamicCounters::TCounterPtr Fullscans;
        ::NMonitoring::TDynamicCounters::TCounterPtr Keys;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultRows;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultChunks;
        ::NMonitoring::TDynamicCounters::TCounterPtr AnswerTime;
        ::NMonitoring::TDynamicCounters::TCounterPtr CpuTime;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlight;
        ::NMonitoring::TDynamicCounters::TCounterPtr ActiveSessions;
        static constexpr size_t MaxSupportedFullscanRequest = 50000;
    };

    std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateKikimrLookupActor(
        NActors::TActorId parentId,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
        NKqpProto::TKikimrLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest,
        const bool isMultiMatches
    )
    {
        auto guard = Guard(*alloc);
        const auto actor = new TKikimrLookupActor(
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

} // namespace NYql::NDq
