#pragma once

#include "kqp_event_ids.h"
#include "kqp_prepared_query.h"

#include <library/cpp/lwtrace/shuttle.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/cancelation/cancelation.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/guid.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NKqp {

void ConvertKqpQueryResultToDbResult(const NKikimrMiniKQL::TResult& from, Ydb::ResultSet* to);

template<typename TFrom, typename TTo>
inline void ConvertKqpQueryResultsToDbResult(const TFrom& from, TTo* to) {
    const auto& results = from.GetResults();
    for (const auto& result : results) {
        ConvertKqpQueryResultToDbResult(result, to->add_result_sets());
    }
}

enum class ETableReadType {
    Other = 0,
    Scan = 1,
    FullScan = 2,
};

const TStringBuf DefaultKikimrPublicClusterName = "db";

inline NActors::TActorId MakeKqpProxyID(ui32 nodeId) {
    const char name[12] = "kqp_proxy";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpCompileServiceID(ui32 nodeId) {
    const char name[12] = "kqp_compile";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpResourceManagerServiceID(ui32 nodeId) {
    const char name[12] = "kqp_resman";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpRmServiceID(ui32 nodeId) {
    const char name[12] = "kqp_rm";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpNodeServiceID(ui32 nodeId) {
    const char name[12] = "kqp_node";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpLocalFileSpillingServiceID(ui32 nodeId) {
    const char name[12] = "kqp_lfspill";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

class TKqpShutdownController;

class TKqpShutdownState : public TThrRefBase {
    friend class TKqpShutdownController;

public:
    void Update(ui32 pendingSessions) {
        AtomicSet(PendingSessions_, pendingSessions);

        if (!Initialized()) {
            AtomicSet(Initialized_, 1);
        }

        if (!pendingSessions) {
            SetCompleted();
        }
    }
private:
    bool ShutdownComplete() const {
        return AtomicGet(ShutdownComplete_) == 1;
    }

    ui32 GetPendingSessions() const {
        return AtomicGet(PendingSessions_);
    }

    bool Initialized() const {
        return AtomicGet(Initialized_) == 1;
    }

    void SetCompleted() {
        AtomicSet(ShutdownComplete_, 1);
    }

    TAtomic PendingSessions_ = 0;
    TAtomic Initialized_ = 0;
    TAtomic ShutdownComplete_ = 0;
};


class TKqpShutdownController {
public:
    TKqpShutdownController(NActors::TActorId kqpProxyActorId, const NKikimrConfig::TTableServiceConfig& tableServiceConfig, bool gracefulEnabled);
    ~TKqpShutdownController() = default;

    void Initialize(NActors::TActorSystem* actorSystem);
    void Stop();

private:
    NActors::TActorId KqpProxyActorId_;
    NActors::TActorSystem* ActorSystem_;
    bool EnableGraceful;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    TIntrusivePtr<TKqpShutdownState> ShutdownState_;
};

struct TKqpQuerySettings {
    bool DocumentApiRestricted = true;

    bool operator==(const TKqpQuerySettings& other) const {
        return
            DocumentApiRestricted == other.DocumentApiRestricted;
    }

    bool operator!=(const TKqpQuerySettings& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQuerySettings&) = delete;
    bool operator>(const TKqpQuerySettings&) = delete;
    bool operator<=(const TKqpQuerySettings&) = delete;
    bool operator>=(const TKqpQuerySettings&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(DocumentApiRestricted);
        return THash<decltype(tuple)>()(tuple);
    }
};

bool IsSqlQuery(const NKikimrKqp::EQueryType& queryType);

struct TKqpQueryId {
    TString Cluster;
    TString Database;
    TString UserSid;
    TString Text;
    TKqpQuerySettings Settings;
    NKikimrKqp::EQueryType QueryType;

public:
    TKqpQueryId(const TString& cluster, const TString& database, const TString& text, NKikimrKqp::EQueryType type)
        : Cluster(cluster)
        , Database(database)
        , Text(text)
        , QueryType(type)
    {
        switch (QueryType) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_DML:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_QUERY:
                break;

            default:
                Y_ENSURE(false, "Unsupported request type");
        }

    }

    bool IsSql() const {
        return IsSqlQuery(QueryType);
    }

    bool operator==(const TKqpQueryId& other) const {
        return
            Cluster == other.Cluster &&
            Database == other.Database &&
            UserSid == other.UserSid &&
            Text == other.Text &&
            Settings == other.Settings &&
            QueryType == other.QueryType;
    }

    bool operator!=(const TKqpQueryId& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQueryId&) = delete;
    bool operator>(const TKqpQueryId&) = delete;
    bool operator<=(const TKqpQueryId&) = delete;
    bool operator>=(const TKqpQueryId&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(Cluster, Database, UserSid, Text, Settings, QueryType);
        return THash<decltype(tuple)>()(tuple);
    }
};

struct TKqpCompileResult {
    using TConstPtr = std::shared_ptr<const TKqpCompileResult>;

    TKqpCompileResult(const TString& uid, TKqpQueryId&& query, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType)
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid)
        , MaxReadType(maxReadType) {}

    TKqpCompileResult(const TString& uid, const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues,
        ETableReadType maxReadType)
        : Status(status)
        , Issues(issues)
        , Uid(uid)
        , MaxReadType(maxReadType) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, TKqpQueryId&& query,
        const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues, ETableReadType maxReadType)
    {
        return std::make_shared<TKqpCompileResult>(uid, std::move(query), status, issues, maxReadType);
    }

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues, ETableReadType maxReadType)
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues, maxReadType);
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    ETableReadType MaxReadType;

    TPreparedQueryHolder::TConstPtr PreparedQuery;
};

struct TEvKqp {
    struct TEvQueryRequestRemote : public TEventPB<TEvQueryRequestRemote, NKikimrKqp::TEvQueryRequest,
        TKqpEvents::EvQueryRequest> {};

    struct TEvQueryRequest : public NActors::TEventLocal<TEvQueryRequest, TKqpEvents::EvQueryRequest> {
    public:
        TEvQueryRequest(
            const std::shared_ptr<NGRpcService::IRequestCtxMtSafe>& ctx,
            const TString& sessionId,
            TActorId actorId,
            TString&& yqlText,
            TString&& queryId,
            NKikimrKqp::EQueryAction queryAction,
            NKikimrKqp::EQueryType queryType,
            const ::Ydb::Table::TransactionControl* txControl,
            const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>* ydbParameters,
            const ::Ydb::Table::QueryStatsCollection::Mode collectStats,
            const ::Ydb::Table::QueryCachePolicy* queryCachePolicy,
            const ::Ydb::Operations::OperationParams* operationParams,
            bool keepSession = false);

        TEvQueryRequest() = default;

        bool IsSerializable() const override {
            return true;
        }

        // Same as TEventPBBase but without Rope
        bool IsExtendedFormat() const override {
            return false;
        }

        const TString& GetDatabase() const {
            return RequestCtx ? Database : Record.GetRequest().GetDatabase();
        }

        bool HasYdbStatus() const {
            return RequestCtx ? false : Record.HasYdbStatus();
        }

        const ::NKikimrKqp::TTopicOperations& GetTopicOperations() const {
            return Record.GetRequest().GetTopicOperations();
        }

        bool HasTopicOperations() const {
            return Record.GetRequest().HasTopicOperations();
        }

        bool GetKeepSession() const {
            return RequestCtx ? KeepSession : Record.GetRequest().GetKeepSession();
        }

        TDuration GetCancelAfter() const {
            return RequestCtx ? CancelAfter : TDuration::MilliSeconds(Record.GetRequest().GetCancelAfterMs());
        }

        TDuration GetOperationTimeout() const {
            return RequestCtx ? OperationTimeout : TDuration::MilliSeconds(Record.GetRequest().GetTimeoutMs());
        }

        bool HasAction() const {
            return RequestCtx ? true : Record.GetRequest().HasAction();
        }

        void SetSessionId(const TString& sessionId) {
            if (RequestCtx) {
                SessionId = sessionId;
            } else {
                Record.MutableRequest()->SetSessionId(sessionId);
            }
        }

        const TString& GetSessionId() const {
            return RequestCtx ? SessionId : Record.GetRequest().GetSessionId();
        }

        NKikimrKqp::EQueryAction GetAction() const {
            return RequestCtx ? QueryAction : Record.GetRequest().GetAction();
        }

        NKikimrKqp::EQueryType GetType() const {
            return RequestCtx ? QueryType : Record.GetRequest().GetType();
        }

        bool HasPreparedQuery() const {
            return RequestCtx ? QueryId.size() > 0 : Record.GetRequest().HasPreparedQuery();
        }

        const TString& GetPreparedQuery() const {
            return RequestCtx ? QueryId : Record.GetRequest().GetPreparedQuery();
        }

        const TString& GetQuery() const {
            return RequestCtx ? YqlText : Record.GetRequest().GetQuery();
        }

        const ::NKikimrMiniKQL::TParams& GetParameters() const {
            return Record.GetRequest().GetParameters();
        }

        const ::Ydb::Table::TransactionControl& GetTxControl() const {
            return RequestCtx ? *TxControl : Record.GetRequest().GetTxControl();
        }

        bool GetUsePublicResponseDataFormat() const {
            return RequestCtx ? true : Record.GetRequest().GetUsePublicResponseDataFormat();
        }

        bool GetQueryKeepInCache() const {
            if (RequestCtx) {
                if (QueryCachePolicy != nullptr) {
                    return QueryCachePolicy->keep_in_cache();
                }
                return false;
            }
            return Record.GetRequest().GetQueryCachePolicy().keep_in_cache();
        }

        bool HasTxControl() const {
            return RequestCtx ? TxControl != nullptr : Record.GetRequest().HasTxControl();
        }

        bool HasCollectStats() const {
            return RequestCtx ? true : Record.GetRequest().HasCollectStats();
        }

        TActorId GetRequestActorId() const {
            return ActorIdFromProto(Record.GetRequestActorId());
        }

        const TString& GetTraceId() const {
            if (RequestCtx) {
                if (!TraceId) {
                    TraceId = RequestCtx->GetTraceId().GetOrElse("");
                }
                return TraceId;
            }

            return Record.GetTraceId();
        }

        const TString& GetRequestType() const {
            if (RequestCtx) {
                if (!RequestType) {
                    RequestType = RequestCtx->GetRequestType().GetOrElse("");
                }
                return RequestType;
            }

            return Record.GetRequestType();
        }

        const TIntrusiveConstPtr<NACLib::TUserToken>& GetUserToken() const {
            if (RequestCtx && RequestCtx->GetInternalToken()) {
                return RequestCtx->GetInternalToken();
            }

            if (Token_) {
                return Token_;
            }

            Token_ = new NACLib::TUserToken(Record.GetUserToken());
            return Token_;
        }

        const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& GetYdbParameters() const {
            if (YdbParameters) {
                return *YdbParameters;
            }

            return Record.GetRequest().GetYdbParameters();
        }

        Ydb::StatusIds::StatusCode GetYdbStatus() const {
            return Record.GetYdbStatus();
        }

        ::Ydb::Table::QueryStatsCollection::Mode GetCollectStats() const {
            if (RequestCtx) {
                return CollectStats;
            }

            return Record.GetRequest().GetCollectStats();
        }

        const ::google::protobuf::RepeatedPtrField<::Ydb::Issue::IssueMessage>& GetQueryIssues() const {
            return Record.GetQueryIssues();
        }

        ui64 GetRequestSize() const {
            return Record.GetRequest().ByteSizeLong();
        }

        ui64 GetQuerySize() const {
            return RequestCtx ? YqlText.size() : Record.GetRequest().GetQuery().size();
        }

        ui64 GetParametersSize() const {
            if (ParametersSize > 0) {
                return ParametersSize;
            }

            ParametersSize += Record.GetRequest().GetParameters().ByteSizeLong();
            for(const auto& [name, param]: GetYdbParameters()) {
                ParametersSize += name.size();
                ParametersSize += param.ByteSizeLong();
            }

            return ParametersSize;
        }

        ui32 CalculateSerializedSize() const override {
            PrepareRemote();
            return Record.ByteSize();
        }

        bool SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
            PrepareRemote();
            return Record.SerializeToZeroCopyStream(chunker);
        }

        static NActors::IEventBase* Load(TEventSerializedData* data) {
            auto pbEv = THolder<TEvQueryRequestRemote>(static_cast<TEvQueryRequestRemote*>(TEvQueryRequestRemote::Load(data)));
            auto req = new TEvQueryRequest();
            req->Record.Swap(&pbEv->Record);
            return req;
        }

        void SetClientLostAction(TActorId actorId, NActors::TActorSystem* as) {
            if (RequestCtx) {
                RequestCtx->SetClientLostAction([actorId, as]() {
                    as->Send(actorId, new NGRpcService::TEvClientLost());
                });
            } else if (Record.HasCancelationActor()) {
                auto cancelationActor = ActorIdFromProto(Record.GetCancelationActor());
                NGRpcService::SubscribeRemoteCancel(cancelationActor, actorId, as);
            }
        }

        void PrepareRemote() const;

        mutable NKikimrKqp::TEvQueryRequest Record;

    private:
        mutable ui64 ParametersSize = 0;
        mutable std::shared_ptr<NGRpcService::IRequestCtxMtSafe> RequestCtx;
        mutable TString TraceId;
        mutable TString RequestType;
        mutable TIntrusiveConstPtr<NACLib::TUserToken> Token_;
        TString Database;
        TString SessionId;
        TString YqlText;
        TString QueryId;
        NKikimrKqp::EQueryAction QueryAction;
        NKikimrKqp::EQueryType QueryType;
        const ::Ydb::Table::TransactionControl* TxControl = nullptr;
        const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>* YdbParameters = nullptr;
        const ::Ydb::Table::QueryStatsCollection::Mode CollectStats = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        const ::Ydb::Table::QueryCachePolicy* QueryCachePolicy = nullptr;
        const ::Ydb::Operations::OperationParams* OperationParams = nullptr;
        bool KeepSession = false;
        TDuration OperationTimeout;
        TDuration CancelAfter;
    };

    struct TEvCloseSessionRequest : public TEventPB<TEvCloseSessionRequest,
        NKikimrKqp::TEvCloseSessionRequest, TKqpEvents::EvCloseSessionRequest> {};

    struct TEvCreateSessionRequest : public TEventPB<TEvCreateSessionRequest,
        NKikimrKqp::TEvCreateSessionRequest, TKqpEvents::EvCreateSessionRequest> {};

    struct TEvPingSessionRequest : public TEventPB<TEvPingSessionRequest,
        NKikimrKqp::TEvPingSessionRequest, TKqpEvents::EvPingSessionRequest> {};

    struct TEvInitiateSessionShutdown : public TEventLocal<TEvInitiateSessionShutdown, TKqpEvents::EvInitiateSessionShutdown> {
        ui32 SoftTimeoutMs;
        ui32 HardTimeoutMs;

        TEvInitiateSessionShutdown(ui32 softTimeoutMs, ui32 hardTimeoutMs)
            : SoftTimeoutMs(softTimeoutMs)
            , HardTimeoutMs(hardTimeoutMs)
        {}
    };

    struct TEvContinueShutdown : public TEventLocal<TEvContinueShutdown, TKqpEvents::EvContinueShutdown> {};

    struct TEvProcessResponse : public TEventPB<TEvProcessResponse, NKikimrKqp::TEvProcessResponse,
        TKqpEvents::EvProcessResponse>
    {
        static THolder<TEvProcessResponse> Error(Ydb::StatusIds::StatusCode ydbStatus, const TString& error) {
            auto ev = MakeHolder<TEvProcessResponse>();
            ev->Record.SetYdbStatus(ydbStatus);
            ev->Record.SetError(error);
            return ev;
        }

        static THolder<TEvProcessResponse> Success() {
            auto ev = MakeHolder<TEvProcessResponse>();
            ev->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            return ev;
        }
    };

    struct TEvDataQueryStreamPart : public TEventPB<TEvDataQueryStreamPart,
        NKikimrKqp::TEvDataQueryStreamPart, TKqpEvents::EvDataQueryStreamPart> {};

    struct TEvDataQueryStreamPartAck : public TEventLocal<TEvDataQueryStreamPartAck, TKqpEvents::EvDataQueryStreamPartAck> {};

    // Wrapper to use Arena allocated protobuf with ActorSystem (for serialization path).
    // Arena deserialization is not supported.
    // TODO: Add arena support to actor system TEventPB?
    template<typename TProto>
    class TProtoArenaHolder : public TNonCopyable {
    public:
        TProtoArenaHolder()
            : Protobuf_(google::protobuf::Arena::CreateMessage<TProto>(nullptr))
        {}

        ~TProtoArenaHolder() {
            // Deallocate message only if it was "normal" allocation
            // In case of protobuf arena memory will be freed during arena deallocation
            if (!Protobuf_->GetArena()) {
                delete Protobuf_;
            }
        }

        void Realloc(std::shared_ptr<google::protobuf::Arena> arena) {
            // Allow realloc only if previous allocation was made using "normal" allocator
            // and no data was writen. It prevents ineffective using of protobuf.
            Y_ASSERT(!Protobuf_->GetArena());
            Y_ASSERT(ByteSize() == 0);
            delete Protobuf_;
            Protobuf_ = google::protobuf::Arena::CreateMessage<TProto>(arena.get());
            // Make sure arena is alive
            Arena_ = arena;
        }

        bool ParseFromString(const TString& data) {
            return Protobuf_->ParseFromString(data);
        }

        bool ParseFromZeroCopyStream(google::protobuf::io::ZeroCopyInputStream* input) {
            return Protobuf_->ParseFromZeroCopyStream(input);
        }

        bool SerializeToZeroCopyStream(google::protobuf::io::ZeroCopyOutputStream* output) const {
            return Protobuf_->SerializeToZeroCopyStream(output);
        }

        bool SerializeToString(TString* output) const {
            return Protobuf_->SerializeToString(output);
        }

        int ByteSize() const {
            return Protobuf_->ByteSize();
        }

        TString DebugString() const {
            return Protobuf_->DebugString();
        }

        TString ShortDebugString() const {
            return Protobuf_->ShortDebugString();
        }

        TString GetTypeName() const {
            return Protobuf_->GetTypeName();
        }

        const TProto& GetRef() const {
            return *Protobuf_;
        }

        TProto& GetRef() {
            return *Protobuf_;
        }

    private:
        TProtoArenaHolder(TProtoArenaHolder&&) = default;
        TProtoArenaHolder& operator=(TProtoArenaHolder&&) = default;
        TProto* Protobuf_;
        std::shared_ptr<google::protobuf::Arena> Arena_;
    };

    struct TEvQueryResponse : public TEventPB<TEvQueryResponse, TProtoArenaHolder<NKikimrKqp::TEvQueryResponse>,
        TKqpEvents::EvQueryResponse> {};

    struct TEvCreateSessionResponse : public TEventPB<TEvCreateSessionResponse,
        NKikimrKqp::TEvCreateSessionResponse, TKqpEvents::EvCreateSessionResponse> {};

    struct TEvContinueProcess : public TEventLocal<TEvContinueProcess, TKqpEvents::EvContinueProcess> {
        TEvContinueProcess(ui32 queryId, bool finished)
            : QueryId(queryId)
            , Finished(finished) {}

        ui32 QueryId;
        bool Finished;
    };

    struct TEvQueryTimeout : public TEventLocal<TEvQueryTimeout, TKqpEvents::EvQueryTimeout> {
        TEvQueryTimeout(ui32 queryId)
            : QueryId(queryId) {}

        ui32 QueryId;
    };

    struct TEvIdleTimeout : public TEventLocal<TEvIdleTimeout, TKqpEvents::EvIdleTimeout> {
        TEvIdleTimeout(ui32 timerId)
            : TimerId(timerId) {}

        ui32 TimerId;
    };

    struct TEvCloseSessionResponse : public TEventPB<TEvCloseSessionResponse,
        NKikimrKqp::TEvCloseSessionResponse, TKqpEvents::EvCloseSessionResponse> {};

    struct TEvPingSessionResponse : public TEventPB<TEvPingSessionResponse,
        NKikimrKqp::TEvPingSessionResponse, TKqpEvents::EvPingSessionResponse> {};

    struct TEvCompileRequest : public TEventLocal<TEvCompileRequest, TKqpEvents::EvCompileRequest> {
        TEvCompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid, TMaybe<TKqpQueryId>&& query,
            bool keepInCache, TInstant deadline, TKqpDbCountersPtr dbCounters, NLWTrace::TOrbit orbit = {})
            : UserToken(userToken)
            , Uid(uid)
            , Query(std::move(query))
            , KeepInCache(keepInCache)
            , Deadline(deadline)
            , DbCounters(dbCounters)
            , Orbit(std::move(orbit))
        {
            Y_ENSURE(Uid.Defined() != Query.Defined());
        }

        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        TMaybe<TString> Uid;
        TMaybe<TKqpQueryId> Query;
        bool KeepInCache = false;
        // it is allowed for local event to use absolute time (TInstant) instead of time interval (TDuration)
        TInstant Deadline;
        TKqpDbCountersPtr DbCounters;
        TMaybe<bool> DocumentApiRestricted;

        NLWTrace::TOrbit Orbit;
    };

    struct TEvRecompileRequest : public TEventLocal<TEvRecompileRequest, TKqpEvents::EvRecompileRequest> {
        TEvRecompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& uid, const TMaybe<TKqpQueryId>& query,
            TInstant deadline, TKqpDbCountersPtr dbCounters, NLWTrace::TOrbit orbit = {})
            : UserToken(userToken)
            , Uid(uid)
            , Query(query)
            , Deadline(deadline)
            , DbCounters(dbCounters)
            , Orbit(std::move(orbit)) {}

        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        TString Uid;
        TMaybe<TKqpQueryId> Query;

        TInstant Deadline;
        TKqpDbCountersPtr DbCounters;

        NLWTrace::TOrbit Orbit;
    };

    struct TEvCompileResponse : public TEventLocal<TEvCompileResponse, TKqpEvents::EvCompileResponse> {
        TEvCompileResponse(const TKqpCompileResult::TConstPtr& compileResult, NLWTrace::TOrbit orbit = {})
            : CompileResult(compileResult)
            , Orbit(std::move(orbit)) {}

        TKqpCompileResult::TConstPtr CompileResult;
        NKqpProto::TKqpStatsCompile Stats;
        std::optional<TString> ReplayMessage;

        NLWTrace::TOrbit Orbit;
    };

    struct TEvCompileInvalidateRequest : public TEventLocal<TEvCompileInvalidateRequest,
        TKqpEvents::EvCompileInvalidateRequest>
    {
        TEvCompileInvalidateRequest(const TString& uid, TKqpDbCountersPtr dbCounters)
            : Uid(uid)
            , DbCounters(dbCounters) {}

        TString Uid;
        TKqpDbCountersPtr DbCounters;
    };

    struct TEvInitiateShutdownRequest : public TEventLocal<TEvInitiateShutdownRequest, TKqpEvents::EvInitiateShutdownRequest> {
        TIntrusivePtr<TKqpShutdownState> ShutdownState;

        TEvInitiateShutdownRequest(TIntrusivePtr<TKqpShutdownState> ShutdownState)
            : ShutdownState(ShutdownState)
        {}
    };

    using TEvAbortExecution = NYql::NDq::TEvDq::TEvAbortExecution;
};

class TKqpRequestInfo {
public:
    TKqpRequestInfo(const TString& traceId, const TString& sessionId)
        : TraceId(traceId)
        , SessionId(sessionId) {}

    TKqpRequestInfo(const TString& traceId)
        : TraceId(traceId)
        , SessionId() {}

    TKqpRequestInfo()
        : TraceId()
        , SessionId() {}

    const TString GetTraceId() const {
        return TraceId;
    }

    const TString GetSessionId() const {
        return SessionId;
    }

private:
    TString TraceId;
    TString SessionId;
};

class IQueryReplayBackend : public TNonCopyable {
public:

    /// Collect details about query:
    /// Accepts query text
    virtual void Collect(const TString& queryData) = 0;

    virtual ~IQueryReplayBackend() {};

    //// Updates configuration onn running backend, if applicable.
    virtual void UpdateConfig(const NKikimrConfig::TTableServiceConfig& serviceConfig) = 0;
};


class TNullQueryReplayBackend : public IQueryReplayBackend {
public:
    void Collect(const TString&) {
    }

    virtual void UpdateConfig(const NKikimrConfig::TTableServiceConfig&) {
    }

    ~TNullQueryReplayBackend() {
    }
};

class IQueryReplayBackendFactory {
public:
    virtual ~IQueryReplayBackendFactory() {}
    virtual IQueryReplayBackend *Create(
        const NKikimrConfig::TTableServiceConfig& serviceConfig,
        TIntrusivePtr<TKqpCounters> counters) = 0;
};

inline IQueryReplayBackend* CreateQueryReplayBackend(
        const NKikimrConfig::TTableServiceConfig& serviceConfig,
        TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<IQueryReplayBackendFactory> factory) {
    if (!factory) {
        return new TNullQueryReplayBackend();
    } else {
        return factory->Create(serviceConfig, std::move(counters));
    }
}

static inline IOutputStream& operator<<(IOutputStream& stream, const TKqpRequestInfo& requestInfo) {
    if (!requestInfo.GetTraceId().empty()) {
        stream << "TraceId: \"" << requestInfo.GetTraceId() << "\", ";
    }
    if (!requestInfo.GetSessionId().empty()) {
        stream << "SessionId: " << requestInfo.GetSessionId() << ", ";
    }

    return stream;
}

} // namespace NKqp
} // namespace NKikimr

template<>
struct THash<NKikimr::NKqp::TKqpQuerySettings> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQuerySettings& settings) const {
        return settings.GetHash();
    }
};

template<>
struct THash<NKikimr::NKqp::TKqpQueryId> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQueryId& query) const {
        return query.GetHash();
    }
};
