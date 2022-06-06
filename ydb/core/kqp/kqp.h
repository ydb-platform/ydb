#pragma once

#include "kqp_query_replay.h"
#include <ydb/core/kqp/common/kqp_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/provider/yql_kikimr_query_traits.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/generic/guid.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NKqp {

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

struct TKqpQueryId {
    TString Cluster;
    TString Database;
    TString UserSid;
    TString Text;
    TKqpQuerySettings Settings;

public:
    TKqpQueryId(const TString& cluster, const TString& database, const TString& text)
        : Cluster(cluster)
        , Database(database)
        , Text(text) {}

    bool operator==(const TKqpQueryId& other) const {
        return
            Cluster == other.Cluster &&
            Database == other.Database &&
            UserSid == other.UserSid &&
            Text == other.Text &&
            Settings == other.Settings;
    }

    bool operator!=(const TKqpQueryId& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQueryId&) = delete;
    bool operator>(const TKqpQueryId&) = delete;
    bool operator<=(const TKqpQueryId&) = delete;
    bool operator>=(const TKqpQueryId&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(Cluster, Database, UserSid, Text, Settings);
        return THash<decltype(tuple)>()(tuple);
    }
};

using TPreparedQueryConstPtr = std::shared_ptr<const NKikimrKqp::TPreparedQuery>;

struct TKqpCompileResult {
    using TConstPtr = std::shared_ptr<const TKqpCompileResult>;

    TKqpCompileResult(const TString& uid, TKqpQueryId&& query, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues)
        : Status(status)
        , Issues(issues)
        , Query(std::move(query))
        , Uid(uid) {}

    TKqpCompileResult(const TString& uid, const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues)
        : Status(status)
        , Issues(issues)
        , Uid(uid) {}

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, TKqpQueryId&& query,
        const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues)
    {
        return std::make_shared<TKqpCompileResult>(uid, std::move(query), status, issues);
    }

    static std::shared_ptr<TKqpCompileResult> Make(const TString& uid, const Ydb::StatusIds::StatusCode& status,
        const NYql::TIssues& issues)
    {
        return std::make_shared<TKqpCompileResult>(uid, status, issues);
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TMaybe<TKqpQueryId> Query;
    TString Uid;

    TPreparedQueryConstPtr PreparedQuery;
    TPreparedQueryConstPtr PreparedQueryNewEngine;
    std::optional<TQueryTraits> QueryTraits;
};

struct TEvKqp {
    struct TEvQueryRequest : public TEventPB<TEvQueryRequest, NKikimrKqp::TEvQueryRequest,
        TKqpEvents::EvQueryRequest> {};

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
        TEvCompileRequest(const TString& userToken, const TMaybe<TString>& uid, TMaybe<TKqpQueryId>&& query,
            bool keepInCache, TInstant deadline, TKqpDbCountersPtr dbCounters)
            : UserToken(userToken)
            , Uid(uid)
            , Query(std::move(query))
            , KeepInCache(keepInCache)
            , Deadline(deadline)
            , DbCounters(dbCounters)
        {
            Y_ENSURE(Uid.Defined() != Query.Defined());
        }

        TString UserToken;
        TMaybe<TString> Uid;
        TMaybe<TKqpQueryId> Query;
        bool KeepInCache = false;
        // it is allowed for local event to use absolute time (TInstant) instead of time interval (TDuration)
        TInstant Deadline;
        TKqpDbCountersPtr DbCounters;
        TMaybe<bool> DocumentApiRestricted;
    };

    struct TEvRecompileRequest : public TEventLocal<TEvRecompileRequest, TKqpEvents::EvRecompileRequest> {
        TEvRecompileRequest(const TString& userToken, const TString& uid, const TMaybe<TKqpQueryId>& query,
            TInstant deadline, TKqpDbCountersPtr dbCounters)
            : UserToken(userToken)
            , Uid(uid)
            , Query(query)
            , Deadline(deadline)
            , DbCounters(dbCounters) {}

        TString UserToken;
        TString Uid;
        TMaybe<TKqpQueryId> Query;

        TInstant Deadline;
        TKqpDbCountersPtr DbCounters;
    };

    struct TEvCompileResponse : public TEventLocal<TEvCompileResponse, TKqpEvents::EvCompileResponse> {
        TEvCompileResponse(const TKqpCompileResult::TConstPtr& compileResult)
            : CompileResult(compileResult) {}

        TKqpCompileResult::TConstPtr CompileResult;
        NKqpProto::TKqpStatsCompile Stats;
        std::optional<TString> ReplayMessage;

        ui32 ForceNewEnginePercent = 0;
        ui32 ForceNewEngineLevel = 0;
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


static inline IOutputStream& operator<<(IOutputStream& stream, const TKqpRequestInfo& requestInfo) {
    if (!requestInfo.GetTraceId().empty()) {
        stream << "TraceId: \"" << requestInfo.GetTraceId() << "\", ";
    }
    if (!requestInfo.GetSessionId().empty()) {
        stream << "SessionId: " << requestInfo.GetSessionId() << ", ";
    }

    return stream;
}

IActor* CreateKqpProxyService(const NKikimrConfig::TLogConfig& logConfig,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    TVector<NKikimrKqp::TKqpSetting>&& settings,
    std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory);

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
