#include "counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/util/concurrent_rw_hash.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

class TResponseStatusCounter {

    TIntrusivePtr<::NMonitoring::TDynamicCounters> TypeGroup;
    TString Status;
    ::NMonitoring::TDynamicCounters::TCounterPtr Counter = nullptr;
    TRWMutex RWLock;

public:

    TResponseStatusCounter(TIntrusivePtr<::NMonitoring::TDynamicCounters> typeGroup, TString&& status)
        : TypeGroup(std::move(typeGroup))
        , Status(std::move(status)) {
    }

    void Set(ui64 value) {
        {
            TReadGuard guard(RWLock);
            if (Counter == nullptr && value == 0) {
                return;
            }
            if (Counter != nullptr) {
                Counter->Set(value);
                return;
            }
        }
        TWriteGuard guard(RWLock);
        if (Counter == nullptr) {
            Counter = TypeGroup->GetSubgroup("status", Status)->GetExpiringNamedCounter(
                "name", "api.grpc.response.count", true
            );
        }
        Counter->Set(value);
    }

    void operator+=(ui64 value) {
        if (value == 0) {
            return;
        }
        {
            TReadGuard guard(RWLock);
            if (Counter != nullptr) {
                *Counter += value;
                return;
            }
        }
        TWriteGuard guard(RWLock);
        if (Counter == nullptr) {
            Counter = TypeGroup->GetSubgroup("status", Status)->GetExpiringNamedCounter(
                "name", "api.grpc.response.count", true
            );
        }
        *Counter += value;
    }

    ui64 Val() {
        TReadGuard guard(RWLock);
        if (Counter == nullptr) {
            return 0;
        }
        return Counter->Val();
    }
};

struct TYdbRpcCounters {

    TYdbRpcCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const char* serviceName,
        const char* requestName, bool forDatabase);

    ::NMonitoring::TDynamicCounters::TCounterPtr RequestCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestInflight;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestInflightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestRpcError;

    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseRpcError;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseRpcNotAuthenticated;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseRpcResourceExhausted;
    THashMap<ui32, std::shared_ptr<TResponseStatusCounter>> ResponseByStatus;
};

class TYdbCounterBlock : public NYdbGrpc::ICounterBlock {
protected:
    const bool Streaming = false;

    TYdbRpcCounters YdbCounters;
private:
    // "Internal" counters
    // TODO: Switch to public YDB counters
    ::NMonitoring::TDynamicCounters::TCounterPtr TotalCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr InflyCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr NotOkRequestCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr NotOkResponseCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr InflyRequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr NotAuthenticated;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResourceExhausted;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsWithoutDatabase;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsWithoutToken;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsWithoutTls;
    ::NMonitoring::THistogramPtr Histo;
    ::NMonitoring::THistogramPtr ClientTimeoutHisto;


    std::function<void()> InitFn;
    std::once_flag OnceFlag;

    void InitOnce() {
        std::call_once(OnceFlag, InitFn);
    }

    std::shared_ptr<TResponseStatusCounter> GetResponseCounterByStatus(ui32 status) {
        auto it = YdbCounters.ResponseByStatus.find(status);
        if (it == YdbCounters.ResponseByStatus.end()) {
            return YdbCounters.ResponseByStatus[0];
        }
        return it->second;
    }

public:
    TYdbCounterBlock(const ::NMonitoring::TDynamicCounterPtr& counters, const char* serviceName,
        const char* requestName, bool streaming,
        bool forDatabase = false, ::NMonitoring::TDynamicCounterPtr internalGroup = {});

    void CountNotOkRequest() override {
        InitOnce();
        NotOkRequestCounter->Inc();
        YdbCounters.RequestRpcError->Inc();
    }

    void CountNotOkResponse() override {
        InitOnce();
        NotOkResponseCounter->Inc();
        YdbCounters.ResponseRpcError->Inc();
    }

    void CountNotAuthenticated() override {
        InitOnce();
        NotAuthenticated->Inc();
        YdbCounters.ResponseRpcNotAuthenticated->Inc();
    }

    void CountResourceExhausted() override {
        InitOnce();
        ResourceExhausted->Inc();
        YdbCounters.ResponseRpcResourceExhausted->Inc();
    }

    void CountRequestsWithoutDatabase() override {
        InitOnce();
        RequestsWithoutDatabase->Inc();
    }

    void CountRequestsWithoutToken() override {
        InitOnce();
        RequestsWithoutToken->Inc();
    }

    void CountRequestWithoutTls() override {
        InitOnce();
        RequestsWithoutTls->Inc();
    }

    void CountRequestBytes(ui32 requestSize) override {
        InitOnce();
        *RequestBytes += requestSize;
        *YdbCounters.RequestBytes += requestSize;
    }

    void CountResponseBytes(ui32 responseSize) override {
        InitOnce();
        *ResponseBytes += responseSize;
        *YdbCounters.ResponseBytes += responseSize;
    }

    void StartProcessing(ui32 requestSize, TInstant deadline) override {
        InitOnce();
        TotalCounter->Inc();
        InflyCounter->Inc();
        *RequestBytes += requestSize;
        *InflyRequestBytes += requestSize;

        YdbCounters.RequestCount->Inc();
        YdbCounters.RequestInflight->Inc();
        *YdbCounters.RequestBytes += requestSize;
        *YdbCounters.RequestInflightBytes += requestSize;
        if (deadline != TInstant::Zero() && deadline != TInstant::Max()) {
            auto now = TInstant::Now();
            if (deadline > now) {
                auto timeout = deadline - now;
                ClientTimeoutHisto->Collect(timeout.MilliSeconds());
            } else {
                ClientTimeoutHisto->Collect(0);
            }
        }
    }

    void FinishProcessing(ui32 requestSize, ui32 responseSize, bool ok, ui32 status,
        TDuration requestDuration) override
    {
        InitOnce();
        InflyCounter->Dec();
        *InflyRequestBytes -= requestSize;
        *ResponseBytes += responseSize;

        YdbCounters.RequestInflight->Dec();
        *YdbCounters.RequestInflightBytes -= requestSize;
        *YdbCounters.ResponseBytes += responseSize;

        if (!ok) {
            NotOkResponseCounter->Inc();
            YdbCounters.ResponseRpcError->Inc();
        } else if (!Streaming) {
            *GetResponseCounterByStatus(status) += 1;
        }

        Histo->Collect(requestDuration.MillisecondsFloat());
    }

    NYdbGrpc::ICounterBlockPtr Clone() override {
        return this;
    }
};

TYdbRpcCounters::TYdbRpcCounters(const ::NMonitoring::TDynamicCounterPtr& counters,
    const char* serviceName, const char* requestName, bool forDatabase)
{
    ::NMonitoring::TDynamicCounterPtr ydbGroup;
    if (forDatabase) {
        ydbGroup = counters;
    } else {
        ydbGroup = GetServiceCounters(counters, "ydb");
    }

    auto serviceGroup = ydbGroup->GetSubgroup("api_service", serviceName);
    auto typeGroup = serviceGroup->GetSubgroup("method", requestName);

    RequestCount = typeGroup->GetNamedCounter("name", "api.grpc.request.count", true);
    RequestInflight = typeGroup->GetNamedCounter("name", "api.grpc.request.inflight_count", false);
    RequestBytes = typeGroup->GetNamedCounter("name", "api.grpc.request.bytes", true);
    RequestInflightBytes = typeGroup->GetNamedCounter("name", "api.grpc.request.inflight_bytes", false);
    RequestRpcError = typeGroup->GetNamedCounter("name", "api.grpc.request.dropped_count", true);

    ResponseBytes = typeGroup->GetNamedCounter("name", "api.grpc.response.bytes", true);
    ResponseRpcError = typeGroup->GetNamedCounter("name", "api.grpc.response.dropped_count", true);

    auto countName = "api.grpc.response.count";

    ResponseRpcNotAuthenticated =
        typeGroup->GetSubgroup("status", "UNAUTHENTICATED")->GetNamedCounter("name", countName, true);
    ResponseRpcResourceExhausted =
        typeGroup->GetSubgroup("status", "RESOURCE_EXHAUSTED")->GetNamedCounter("name", countName, true);

    ResponseByStatus[Ydb::StatusIds::STATUS_CODE_UNSPECIFIED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "UNSPECIFIED");
    ResponseByStatus[Ydb::StatusIds::SUCCESS] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "SUCCESS");
    ResponseByStatus[Ydb::StatusIds::BAD_REQUEST] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "BAD_REQUEST");
    ResponseByStatus[Ydb::StatusIds::UNAUTHORIZED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "UNAUTHORIZED");
    ResponseByStatus[Ydb::StatusIds::INTERNAL_ERROR] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "INTERNAL_ERROR");
    ResponseByStatus[Ydb::StatusIds::ABORTED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "ABORTED");
    ResponseByStatus[Ydb::StatusIds::UNAVAILABLE] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "UNAVAILABLE");
    ResponseByStatus[Ydb::StatusIds::OVERLOADED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "OVERLOADED");
    ResponseByStatus[Ydb::StatusIds::SCHEME_ERROR] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "SCHEME_ERROR");
    ResponseByStatus[Ydb::StatusIds::GENERIC_ERROR] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "GENERIC_ERROR");
    ResponseByStatus[Ydb::StatusIds::TIMEOUT] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "TIMEOUT");
    ResponseByStatus[Ydb::StatusIds::BAD_SESSION] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "BAD_SESSION");
    ResponseByStatus[Ydb::StatusIds::PRECONDITION_FAILED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "PRECONDITION_FAILED");
    ResponseByStatus[Ydb::StatusIds::ALREADY_EXISTS] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "ALREADY_EXISTS");
    ResponseByStatus[Ydb::StatusIds::NOT_FOUND] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "NOT_FOUND");
    ResponseByStatus[Ydb::StatusIds::SESSION_EXPIRED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "SESSION_EXPIRED");
    ResponseByStatus[Ydb::StatusIds::CANCELLED] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "CANCELLED");
    ResponseByStatus[Ydb::StatusIds::SESSION_BUSY] =
        std::make_shared<TResponseStatusCounter>(typeGroup, "SESSION_BUSY");
}

TYdbCounterBlock::TYdbCounterBlock(const ::NMonitoring::TDynamicCounterPtr& counters, const char* serviceName,
    const char* requestName, bool streaming,
    bool forDatabase, ::NMonitoring::TDynamicCounterPtr internalGroup)
    : Streaming(streaming)
    , YdbCounters(counters, serviceName, requestName, forDatabase)
{
    // group for all counters
    ::NMonitoring::TDynamicCounterPtr group;
    if (forDatabase) {
        group = internalGroup;
    } else {
        group = GetServiceCounters(counters, "grpc")->GetSubgroup("subsystem", "serverStats");
    }

    // subgroup for request-specific counters
    auto subgroup = group->GetSubgroup(streaming ? "stream" : "request", requestName);

    InitFn = [this, group, subgroup] () {
        // aggregated (non-request-specific counters)
        NotOkRequestCounter = group->GetCounter("notOkRequest", true);
        NotOkResponseCounter = group->GetCounter("notOkResponse", true);
        RequestBytes = group->GetCounter("requestBytes", true);
        InflyRequestBytes = group->GetCounter("inflyRequestBytes", false);
        ResponseBytes = group->GetCounter("responseBytes", true);
        NotAuthenticated = group->GetCounter("notAuthenticated", true);
        ResourceExhausted = group->GetCounter("resourceExhausted", true);
        RequestsWithoutDatabase = group->GetCounter("requestsWithoutDatabase", true);
        RequestsWithoutToken = group->GetCounter("requestsWithoutToken", true);
        RequestsWithoutTls = group->GetCounter("requestsWithoutTls", true);

        TotalCounter = subgroup->GetCounter("total", true);
        InflyCounter = subgroup->GetCounter("infly", false);

        {
            auto h = NMonitoring::ExplicitHistogram(
                NMonitoring::TBucketBounds{5, 10, 50, 100, 500, 1000, 5000, 10000, 20000, 60000});
            Histo = subgroup->GetHistogram("LatencyMs", std::move(h));
        }

        {
            auto h = NMonitoring::ExplicitHistogram(
                NMonitoring::TBucketBounds{
                    0, 5, 10, 50, 100, 250, 500,
                    1000, 5000, 10000, 20000, 60000});
            ClientTimeoutHisto = subgroup->GetHistogram("TimeoutMs", std::move(h));
        }
    };
}

using TYdbCounterBlockPtr = TIntrusivePtr<TYdbCounterBlock>;


#define DB_GRPC_SIMPLE_COUNTERS_MAP(XX) \
    XX(DB_GRPC_REQ_INFLIGHT_COUNT, YdbCounters.RequestInflight) \
    XX(DB_GRPC_REQ_INFLIGHT_BYTES, YdbCounters.RequestInflightBytes)

#define DB_GRPC_CUMULATIVE_COUNTERS_MAP(XX) \
    XX(DB_GRPC_REQ_COUNT, YdbCounters.RequestCount) \
    XX(DB_GRPC_REQ_BYTES, YdbCounters.RequestBytes) \
    XX(DB_GRPC_REQ_RPC_ERROR, YdbCounters.RequestRpcError) \
    XX(DB_GRPC_RSP_BYTES, YdbCounters.ResponseBytes) \
    XX(DB_GRPC_RSP_RPC_ERROR, YdbCounters.ResponseRpcError) \
    XX(DB_GRPC_RSP_RPC_NOT_AUTH, YdbCounters.ResponseRpcNotAuthenticated) \
    XX(DB_GRPC_RSP_RPC_RESOURCE_EXHAUSTED, YdbCounters.ResponseRpcResourceExhausted) \
    XX(DB_GRPC_RSP_UNSPECIFIED, YdbCounters.ResponseByStatus[Ydb::StatusIds::STATUS_CODE_UNSPECIFIED]) \
    XX(DB_GRPC_RSP_SUCCESS, YdbCounters.ResponseByStatus[Ydb::StatusIds::SUCCESS]) \
    XX(DB_GRPC_RSP_BAD_REQUEST, YdbCounters.ResponseByStatus[Ydb::StatusIds::BAD_REQUEST]) \
    XX(DB_GRPC_RSP_UNAUTHORIZED, YdbCounters.ResponseByStatus[Ydb::StatusIds::UNAUTHORIZED]) \
    XX(DB_GRPC_RSP_INTERNAL_ERROR, YdbCounters.ResponseByStatus[Ydb::StatusIds::INTERNAL_ERROR]) \
    XX(DB_GRPC_RSP_ABORTED, YdbCounters.ResponseByStatus[Ydb::StatusIds::ABORTED]) \
    XX(DB_GRPC_RSP_UNAVAILABLE, YdbCounters.ResponseByStatus[Ydb::StatusIds::UNAVAILABLE]) \
    XX(DB_GRPC_RSP_OVERLOADED, YdbCounters.ResponseByStatus[Ydb::StatusIds::OVERLOADED]) \
    XX(DB_GRPC_RSP_SCHEME_ERROR, YdbCounters.ResponseByStatus[Ydb::StatusIds::SCHEME_ERROR]) \
    XX(DB_GRPC_RSP_GENERIC_ERROR, YdbCounters.ResponseByStatus[Ydb::StatusIds::GENERIC_ERROR]) \
    XX(DB_GRPC_RSP_TIMEOUT, YdbCounters.ResponseByStatus[Ydb::StatusIds::TIMEOUT]) \
    XX(DB_GRPC_RSP_BAD_SESSION, YdbCounters.ResponseByStatus[Ydb::StatusIds::BAD_SESSION]) \
    XX(DB_GRPC_RSP_PRECONDITION_FAILED, YdbCounters.ResponseByStatus[Ydb::StatusIds::PRECONDITION_FAILED]) \
    XX(DB_GRPC_RSP_ALREADY_EXISTS, YdbCounters.ResponseByStatus[Ydb::StatusIds::ALREADY_EXISTS]) \
    XX(DB_GRPC_RSP_NOT_FOUND, YdbCounters.ResponseByStatus[Ydb::StatusIds::NOT_FOUND]) \
    XX(DB_GRPC_RSP_SESSION_EXPIRED, YdbCounters.ResponseByStatus[Ydb::StatusIds::SESSION_EXPIRED]) \
    XX(DB_GRPC_RSP_CANCELLED, YdbCounters.ResponseByStatus[Ydb::StatusIds::CANCELLED]) \
    XX(DB_GRPC_RSP_SESSION_BUSY, YdbCounters.ResponseByStatus[Ydb::StatusIds::SESSION_BUSY])

class TYdbDbCounterBlock : public TYdbCounterBlock {
public:
    TYdbDbCounterBlock(const ::NMonitoring::TDynamicCounterPtr& counters, const char* serviceName,
        const char* requestName, bool streaming,
        ::NMonitoring::TDynamicCounterPtr internalGroup = {})
        : TYdbCounterBlock(counters, serviceName, requestName, streaming, true, internalGroup)
    {}

    void ToProto(NKikimrSysView::TDbGRpcCounters& counters) {
        auto* main = counters.MutableRequestCounters();
        auto* simple = main->MutableSimple();
        auto* cumulative = main->MutableCumulative();

        simple->Resize(DB_GRPC_SIMPLE_COUNTER_SIZE, 0);
        cumulative->Resize(DB_GRPC_CUMULATIVE_COUNTER_SIZE, 0);

#define SAVE_SIMPLE_COUNTER(INDEX, TARGET) { (*simple)[INDEX] = (TARGET)->Val(); }
#define SAVE_CUMULATIVE_COUNTER(INDEX, TARGET) { (*cumulative)[INDEX] = (TARGET)->Val(); }

        DB_GRPC_SIMPLE_COUNTERS_MAP(SAVE_SIMPLE_COUNTER)
        DB_GRPC_CUMULATIVE_COUNTERS_MAP(SAVE_CUMULATIVE_COUNTER)
    }

    void FromProto(NKikimrSysView::TDbGRpcCounters& counters) {
        auto* main = counters.MutableRequestCounters();
        auto* simple = main->MutableSimple();
        auto* cumulative = main->MutableCumulative();

        simple->Resize(DB_GRPC_SIMPLE_COUNTER_SIZE, 0);
        cumulative->Resize(DB_GRPC_CUMULATIVE_COUNTER_SIZE, 0);

#define LOAD_SIMPLE_COUNTER(INDEX, TARGET) { (TARGET)->Set((*simple)[INDEX]); }
#define LOAD_CUMULATIVE_COUNTER(INDEX, TARGET) { (TARGET)->Set((*cumulative)[INDEX]); }

        DB_GRPC_SIMPLE_COUNTERS_MAP(LOAD_SIMPLE_COUNTER)
        DB_GRPC_CUMULATIVE_COUNTERS_MAP(LOAD_CUMULATIVE_COUNTER)
    }

    void AggregateFrom(TYdbDbCounterBlock& other) {
#define COPY_SIMPLE_COUNTER(INDEX, TARGET) { *TARGET += other.TARGET->Val(); }
#define COPY_CUMULATIVE_COUNTER(INDEX, TARGET) { *TARGET += other.TARGET->Val(); }

        DB_GRPC_SIMPLE_COUNTERS_MAP(COPY_SIMPLE_COUNTER)
        DB_GRPC_CUMULATIVE_COUNTERS_MAP(COPY_CUMULATIVE_COUNTER)
    }

private:
    enum ESimpleCounter {
        DB_GRPC_SIMPLE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_GRPC_SIMPLE_COUNTER_SIZE
    };
    enum ECumulativeCounter {
        DB_GRPC_CUMULATIVE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_GRPC_CUMULATIVE_COUNTER_SIZE
    };
};

using TYdbDbCounterBlockPtr = TIntrusivePtr<TYdbDbCounterBlock>;


class TGRpcDbCounters : public NSysView::IDbCounters {
public:
    TGRpcDbCounters()
        : Counters(new ::NMonitoring::TDynamicCounters)
        , InternalGroup(new ::NMonitoring::TDynamicCounters)
    {}

    TGRpcDbCounters(::NMonitoring::TDynamicCounterPtr counters, ::NMonitoring::TDynamicCounterPtr internalGroup)
        : Counters(counters)
        , InternalGroup(internalGroup)
    {}

    void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
        for (auto& bucket : CounterBlocks.Buckets) {
            TReadGuard guard(bucket.GetLock());
            for (auto& [key, block] : bucket.GetMap()) {
                auto* proto = counters.FindOrAddGRpcCounters(key.first, key.second);
                block->ToProto(*proto);
            }
        }
    }

    void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
        for (auto& proto : *counters.Proto().MutableGRpcCounters()) {
            auto block = GetCounterBlock(proto.GetGRpcService(), proto.GetGRpcRequest());
            block->FromProto(proto);
        }
    }

    TYdbDbCounterBlockPtr GetCounterBlock(const TString& serviceName, const TString& requestName) {
        auto key = std::make_pair(serviceName, requestName);

        TYdbDbCounterBlockPtr dbCounters;
        if (CounterBlocks.Get(key, dbCounters)) {
            return dbCounters;
        }

        return CounterBlocks.InsertIfAbsentWithInit(key, [&] {
            return new TYdbDbCounterBlock(Counters, serviceName.c_str(), requestName.c_str(), false, InternalGroup);
        });
    }

private:
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr InternalGroup;

    using TKey = std::pair<TString, TString>;
    TConcurrentRWHashMap<TKey, TYdbDbCounterBlockPtr, 16> CounterBlocks;
};

using TGRpcDbCountersPtr = TIntrusivePtr<TGRpcDbCounters>;


class TGRpcDbCountersRegistry {
    class TGRpcDbWatcherCallback : public NKikimr::NSysView::TDbWatcherCallback {
    public:
        void OnDatabaseRemoved(const TString& database, TPathId) override {
            Singleton<TGRpcDbCountersRegistry>()->RemoveDbCounters(database);
        }
    };

public:
    void Initialize(TActorSystem* actorSystem) {
        with_lock(InitLock) {
            if (Y_LIKELY(ActorSystem)) {
                return;
            }
            ActorSystem = actorSystem;

            auto callback = MakeIntrusive<TGRpcDbWatcherCallback>();
            DbWatcherActorId = ActorSystem->Register(NSysView::CreateDbWatcherActor(callback));
        }
    }

    TYdbDbCounterBlockPtr GetCounterBlock(
        const TString& database, const TString& serviceName, const TString& requestName)
    {
        TGRpcDbCountersPtr dbCounters;
        if (DbCounters.Get(database, dbCounters)) {
            return dbCounters->GetCounterBlock(serviceName, requestName);
        }

        dbCounters = DbCounters.InsertIfAbsentWithInit(database, [&database, this] {
            auto counters = MakeIntrusive<TGRpcDbCounters>();

            if (ActorSystem) {
                auto evRegister = MakeHolder<NSysView::TEvSysView::TEvRegisterDbCounters>(
                    NKikimrSysView::GRPC, database, counters);

                ActorSystem->Send(NSysView::MakeSysViewServiceID(ActorSystem->NodeId), evRegister.Release());

                if (DbWatcherActorId) {
                    auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(database);
                    ActorSystem->Send(DbWatcherActorId, evWatch.Release());
                }
            }

            return counters;
        });

        return dbCounters->GetCounterBlock(serviceName, requestName);
    }

    void RemoveDbCounters(const TString& database) {
        DbCounters.Erase(database);
    }

private:
    TConcurrentRWHashMap<TString, TIntrusivePtr<TGRpcDbCounters>, 256> DbCounters;
    TActorSystem* ActorSystem = {};
    TActorId DbWatcherActorId;
    TMutex InitLock;
};


class TYdbCounterBlockWrapper : public NYdbGrpc::ICounterBlock {
    TYdbCounterBlockPtr Common;
    const TString ServiceName;
    const TString RequestName;
    const bool Streaming = false;

    ::NMonitoring::TDynamicCounterPtr Root;
    TYdbDbCounterBlockPtr Db;

public:
    TYdbCounterBlockWrapper(TYdbCounterBlockPtr common, const TString& serviceName, const TString& requestName,
        bool streaming)
        : Common(common)
        , ServiceName(serviceName)
        , RequestName(requestName)
        , Streaming(streaming)
        , Root(new ::NMonitoring::TDynamicCounters)
        , Db(new TYdbDbCounterBlock(Root, serviceName.c_str(), requestName.c_str(), streaming, Root))
    {}

    void CountNotOkRequest() override {
        Common->CountNotOkRequest();
        Db->CountNotOkRequest();
    }

    void CountNotOkResponse() override {
        Common->CountNotOkResponse();
        Db->CountNotOkResponse();
    }

    void CountNotAuthenticated() override {
        Common->CountNotAuthenticated();
        Db->CountNotAuthenticated();
    }

    void CountResourceExhausted() override {
        Common->CountResourceExhausted();
        Db->CountResourceExhausted();
    }

    void CountRequestsWithoutDatabase() override {
        Common->CountRequestsWithoutDatabase();
        Db->CountRequestsWithoutDatabase();
    }

    void CountRequestsWithoutToken() override {
        Common->CountRequestsWithoutToken();
        Db->CountRequestsWithoutToken();
    }

    void CountRequestWithoutTls() override {
        Common->CountRequestWithoutTls();
        Db->CountRequestWithoutTls();
    }

    void CountRequestBytes(ui32 requestSize) override {
        Common->CountRequestBytes(requestSize);
        Db->CountRequestBytes(requestSize);
    }

    void CountResponseBytes(ui32 responseSize) override {
        Common->CountResponseBytes(responseSize);
        Db->CountResponseBytes(responseSize);
    }

    void StartProcessing(ui32 requestSize, TInstant deadline) override {
        Common->StartProcessing(requestSize, deadline);
        Db->StartProcessing(requestSize, deadline);
    }

    void FinishProcessing(ui32 requestSize, ui32 responseSize, bool ok, ui32 status,
        TDuration requestDuration) override
    {
        Common->FinishProcessing(requestSize, responseSize, ok, status, requestDuration);
        Db->FinishProcessing(requestSize, responseSize, ok, status, requestDuration);
    }

    NYdbGrpc::ICounterBlockPtr Clone() override {
        return new TYdbCounterBlockWrapper(Common, ServiceName, RequestName, Streaming);
    }

    void UseDatabase(const TString& database) override {
        if (database.empty()) {
            return;
        }

        auto block = Singleton<TGRpcDbCountersRegistry>()->GetCounterBlock(
            database, ServiceName, RequestName);
        block->AggregateFrom(*Db);
        Db = block;
    }
};

TServiceCounterCB::TServiceCounterCB(::NMonitoring::TDynamicCounterPtr counters, TActorSystem *actorSystem)
    : Counters(std::move(counters))
    , ActorSystem(actorSystem)
{
    if (ActorSystem) {
        Singleton<TGRpcDbCountersRegistry>()->Initialize(ActorSystem);
    }
}

NYdbGrpc::ICounterBlockPtr TServiceCounterCB::operator()(const char* serviceName,
    const char* requestName, bool streaming) const
{
    auto block = MakeIntrusive<TYdbCounterBlock>(Counters, serviceName, requestName, streaming);

    NYdbGrpc::ICounterBlockPtr res(block);
    if (ActorSystem && AppData(ActorSystem)->FeatureFlags.GetEnableDbCounters()) {
        res = MakeIntrusive<TYdbCounterBlockWrapper>(block, serviceName, requestName, streaming);
    }

    return res;
}

TIntrusivePtr<NSysView::IDbCounters> CreateGRpcDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup,
    ::NMonitoring::TDynamicCounterPtr internalGroup)
{
    return new TGRpcDbCounters(externalGroup, internalGroup);
}

} // namespace NGRpcService
} // namespace NKikimr
