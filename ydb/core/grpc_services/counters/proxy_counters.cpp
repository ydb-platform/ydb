#include "proxy_counters.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/util/concurrent_rw_hash.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcProxyCounters : public IGRpcProxyCounters {
protected:
    ::NMonitoring::TDynamicCounterPtr Root_;

    ::NMonitoring::TDynamicCounters::TCounterPtr DatabaseAccessDenyCounter_;
    ::NMonitoring::TDynamicCounters::TCounterPtr DatabaseSchemeErrorCounter_;
    ::NMonitoring::TDynamicCounters::TCounterPtr DatabaseUnavailableCounter_;
    ::NMonitoring::TDynamicCounters::TCounterPtr DatabaseRateLimitedCounter_;
    ::NMonitoring::TDynamicCounters::TCounterPtr ConsumedRUCounter_;
    NMonitoring::THistogramPtr ThrottleDelayHistogram_;

public:
    using TPtr = TIntrusivePtr<TGRpcProxyCounters>;

    TGRpcProxyCounters(::NMonitoring::TDynamicCounterPtr root, bool forDatabase)
        : Root_(root)
    {
        ::NMonitoring::TDynamicCounterPtr group;
        if (forDatabase) {
            group = Root_;
        } else {
            group = GetServiceCounters(Root_, "grpc");
        }

        DatabaseAccessDenyCounter_ = group->GetCounter("databaseAccessDeny", true);
        DatabaseSchemeErrorCounter_ = group->GetCounter("databaseSchemeError", true);
        DatabaseUnavailableCounter_ = group->GetCounter("databaseUnavailable", true);

        DatabaseRateLimitedCounter_ = group->GetCounter("api.grpc.request.throughput_quota_exceeded_count", true);

        ConsumedRUCounter_ = group->GetCounter("api.grpc.response.consumed_request_units", true);

        ThrottleDelayHistogram_ = group->GetHistogram(
            "api.grpc.request.throttling_delay_milliseconds",
            NMonitoring::ExponentialHistogram(20, 2, 1));
    }

    void IncDatabaseAccessDenyCounter() override {
        DatabaseAccessDenyCounter_->Inc();
    }

    void IncDatabaseSchemeErrorCounter() override {
        DatabaseSchemeErrorCounter_->Inc();
    }

    void IncDatabaseUnavailableCounter() override {
        DatabaseUnavailableCounter_->Inc();
    }

    void IncDatabaseRateLimitedCounter() override {
        DatabaseRateLimitedCounter_->Inc();
    }

    void AddConsumedRequestUnits(ui64 requestUnits) override {
        ConsumedRUCounter_->Add(requestUnits);
    }

    void ReportThrottleDelay(const TDuration& duration) override {
        ThrottleDelayHistogram_->Collect(duration.MilliSeconds());
    }
};


#define DB_GRPC_PROXY_CUMULATIVE_COUNTERS_MAP(XX) \
    XX(DB_GRPC_PROXY_ACCESS_DENY, DatabaseAccessDenyCounter_) \
    XX(DB_GRPC_PROXY_SCHEME_ERROR, DatabaseSchemeErrorCounter_) \
    XX(DB_GRPC_PROXY_UNAVAILABLE, DatabaseUnavailableCounter_) \
    XX(DB_GRPC_PROXY_RATE_LIMITED, DatabaseRateLimitedCounter_) \
    XX(DB_GRPC_PROXY_CONSUMED_RU, ConsumedRUCounter_)

#define DB_GRPC_PROXY_HISTOGRAM_COUNTERS_MAP(XX) \
    XX(DB_GRPC_PROXY_THROTTLE_DELAY, ThrottleDelayHistogram_)

template <typename T>
void SaveHistogram(T& histogram, int index, const NMonitoring::THistogramPtr& hgram) {
    auto* buckets = histogram[index].MutableBuckets();
    auto snapshot = hgram->Snapshot();
    auto count = snapshot->Count();
    buckets->Resize(count, 0);
    for (size_t i = 0; i < count; ++i) {
        (*buckets)[i] = snapshot->Value(i);
    }
}

template <typename T>
void LoadHistogram(T& histogram, int index, NMonitoring::THistogramPtr& hgram) {
    auto* buckets = histogram[index].MutableBuckets();
    auto snapshot = hgram->Snapshot();
    auto count = snapshot->Count();
    buckets->Resize(count, 0);
    hgram->Reset();
    for (ui32 i = 0; i < count; ++i) {
        hgram->Collect(snapshot->UpperBound(i), (*buckets)[i]);
    }
}

void AggregateHistogram(NMonitoring::THistogramPtr& dst, const NMonitoring::THistogramPtr& src) {
    auto srcSnapshot = src->Snapshot();
    auto srcCount = srcSnapshot->Count();
    auto dstSnapshot = dst->Snapshot();
    auto dstCount = dstSnapshot->Count();

    for (ui32 b = 0; b < std::min(srcCount, dstCount); ++b) {
        dst->Collect(dstSnapshot->UpperBound(b), srcSnapshot->Value(b));
    }
}

class TGRpcProxyDbCounters : public TGRpcProxyCounters, public NSysView::IDbCounters {
    enum ECumulativeCounter {
        DB_GRPC_PROXY_CUMULATIVE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_GRPC_PROXY_CUMULATIVE_COUNTER_SIZE
    };
    enum EHistogramCounter {
        DB_GRPC_PROXY_HISTOGRAM_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_GRPC_PROXY_HISTOGRAM_COUNTER_SIZE
    };

public:
    using TPtr = TIntrusivePtr<TGRpcProxyDbCounters>;

    TGRpcProxyDbCounters()
        : TGRpcProxyCounters(new ::NMonitoring::TDynamicCounters, true)
    {}

    explicit TGRpcProxyDbCounters(::NMonitoring::TDynamicCounterPtr counters)
        : TGRpcProxyCounters(counters, true)
    {}

    void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
        auto* grpc = counters.Proto().MutableGRpcProxyCounters();
        auto* main = grpc->MutableRequestCounters();
        auto* cumulative = main->MutableCumulative();
        auto* histogram = main->MutableHistogram();

        cumulative->Resize(DB_GRPC_PROXY_CUMULATIVE_COUNTER_SIZE, 0);
        if (main->HistogramSize() < DB_GRPC_PROXY_HISTOGRAM_COUNTER_SIZE) {
            auto missing = DB_GRPC_PROXY_HISTOGRAM_COUNTER_SIZE - main->HistogramSize();
            for (; missing > 0; --missing) {
                main->AddHistogram();
            }
        }

#define SAVE_CUMULATIVE_COUNTER(INDEX, TARGET) { (*cumulative)[INDEX] = (TARGET)->Val(); }
#define SAVE_HISTOGRAM_COUNTER(INDEX, TARGET) { SaveHistogram(*histogram, INDEX, (TARGET)); }

        DB_GRPC_PROXY_CUMULATIVE_COUNTERS_MAP(SAVE_CUMULATIVE_COUNTER)
        DB_GRPC_PROXY_HISTOGRAM_COUNTERS_MAP(SAVE_HISTOGRAM_COUNTER)
    }

    void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
        auto* grpc = counters.Proto().MutableGRpcProxyCounters();
        auto* main = grpc->MutableRequestCounters();
        auto* cumulative = main->MutableCumulative();
        auto* histogram = main->MutableHistogram();

        cumulative->Resize(DB_GRPC_PROXY_CUMULATIVE_COUNTER_SIZE, 0);
        if (main->HistogramSize() < DB_GRPC_PROXY_HISTOGRAM_COUNTER_SIZE) {
            auto missing = DB_GRPC_PROXY_HISTOGRAM_COUNTER_SIZE - main->HistogramSize();
            for (; missing > 0; --missing) {
                main->AddHistogram();
            }
        }

#define LOAD_CUMULATIVE_COUNTER(INDEX, TARGET) { (TARGET)->Set((*cumulative)[INDEX]); }
#define LOAD_HISTOGRAM_COUNTER(INDEX, TARGET) { LoadHistogram(*histogram, INDEX, (TARGET)); }

        DB_GRPC_PROXY_CUMULATIVE_COUNTERS_MAP(LOAD_CUMULATIVE_COUNTER)
        DB_GRPC_PROXY_HISTOGRAM_COUNTERS_MAP(LOAD_HISTOGRAM_COUNTER)
    }

    void AggregateFrom(TGRpcProxyDbCounters& other) {
#define AGGR_CUMULATIVE_COUNTER(INDEX, TARGET) { *TARGET += other.TARGET->Val(); }
#define AGGR_HISTOGRAM_COUNTER(INDEX, TARGET) { AggregateHistogram(TARGET, other.TARGET); }

        DB_GRPC_PROXY_CUMULATIVE_COUNTERS_MAP(AGGR_CUMULATIVE_COUNTER);
        DB_GRPC_PROXY_HISTOGRAM_COUNTERS_MAP(AGGR_HISTOGRAM_COUNTER);
    }
};


class TGRpcProxyDbCountersRegistry {
    TConcurrentRWHashMap<TString, TGRpcProxyDbCounters::TPtr, 256> DbCounters;
    TActorSystem* ActorSystem = {};
    TActorId DbWatcherActorId;
    TMutex InitLock;

    class TGRpcProxyDbWatcherCallback : public NKikimr::NSysView::TDbWatcherCallback {
    public:
        void OnDatabaseRemoved(const TString& database, TPathId) override {
            Singleton<TGRpcProxyDbCountersRegistry>()->RemoveDbProxyCounters(database);
        }
    };

public:
    void Initialize(TActorSystem* actorSystem) {
        with_lock(InitLock) {
            if (Y_LIKELY(ActorSystem)) {
                return;
            }
            ActorSystem = actorSystem;

            auto callback = MakeIntrusive<TGRpcProxyDbWatcherCallback>();
            DbWatcherActorId = ActorSystem->Register(NSysView::CreateDbWatcherActor(callback));
        }
    }

    TGRpcProxyDbCounters::TPtr GetDbProxyCounters(const TString& database) {
        TGRpcProxyDbCounters::TPtr dbCounters;
        if (DbCounters.Get(database, dbCounters)) {
            return dbCounters;
        }

        dbCounters = DbCounters.InsertIfAbsentWithInit(database, [&database, this] {
            auto counters = MakeIntrusive<TGRpcProxyDbCounters>();

            if (ActorSystem) {
                auto evRegister = MakeHolder<NSysView::TEvSysView::TEvRegisterDbCounters>(
                    NKikimrSysView::GRPC_PROXY, database, counters);

                ActorSystem->Send(NSysView::MakeSysViewServiceID(ActorSystem->NodeId), evRegister.Release());

                if (DbWatcherActorId) {
                    auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(database);
                    ActorSystem->Send(DbWatcherActorId, evWatch.Release());
                }
            }

            return counters;
        });

        return dbCounters;
    }

    void RemoveDbProxyCounters(const TString& database) {
        DbCounters.Erase(database);
    }
};


class TGRpcProxyCountersWrapper : public IGRpcProxyCounters {
    IGRpcProxyCounters::TPtr Common;
    TGRpcProxyDbCounters::TPtr Db;

public:
    explicit TGRpcProxyCountersWrapper(IGRpcProxyCounters::TPtr common)
        : Common(common)
        , Db(new TGRpcProxyDbCounters())
    {}

    void IncDatabaseAccessDenyCounter() override {
        Common->IncDatabaseAccessDenyCounter();
        Db->IncDatabaseAccessDenyCounter();
    }

    void IncDatabaseSchemeErrorCounter() override {
        Common->IncDatabaseSchemeErrorCounter();
        Db->IncDatabaseSchemeErrorCounter();
    }

    void IncDatabaseUnavailableCounter() override {
        Common->IncDatabaseUnavailableCounter();
        Db->IncDatabaseUnavailableCounter();
    }

    void IncDatabaseRateLimitedCounter() override {
        Common->IncDatabaseRateLimitedCounter();
        Db->IncDatabaseRateLimitedCounter();
    }

    void AddConsumedRequestUnits(ui64 requestUnits) override {
        Common->AddConsumedRequestUnits(requestUnits);
        Db->AddConsumedRequestUnits(requestUnits);
    }

    void ReportThrottleDelay(const TDuration& duration) override {
        Common->ReportThrottleDelay(duration);
        Db->ReportThrottleDelay(duration);
    }

    void UseDatabase(const TString& database) override {
        if (database.empty()) {
            return;
        }

        auto counters = Singleton<TGRpcProxyDbCountersRegistry>()->GetDbProxyCounters(database);
        counters->AggregateFrom(*Db);
        Db = counters;
    }
};


IGRpcProxyCounters::TPtr WrapGRpcProxyDbCounters(IGRpcProxyCounters::TPtr common) {
    return MakeIntrusive<TGRpcProxyCountersWrapper>(common);
}

IGRpcProxyCounters::TPtr CreateGRpcProxyCounters(::NMonitoring::TDynamicCounterPtr appCounters) {
    return MakeIntrusive<TGRpcProxyCounters>(appCounters, false);
}

TIntrusivePtr<NSysView::IDbCounters> CreateGRpcProxyDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup,
    ::NMonitoring::TDynamicCounterPtr internalGroup)
{
    Y_UNUSED(externalGroup);
    return MakeIntrusive<TGRpcProxyDbCounters>(internalGroup);
}

void InitializeGRpcProxyDbCountersRegistry(TActorSystem* actorSystem) {
    Singleton<TGRpcProxyDbCountersRegistry>()->Initialize(actorSystem);
}

}
}
