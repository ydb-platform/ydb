#include "db_counters.h"
#include "query_history.h"
#include "query_interval.h"
#include "sysview_service.h"

#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/time_provider/time_provider.h>


using namespace NActors;

namespace NKikimr {
namespace NSysView {

static void CopyCounters(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current)
{
    auto simpleSize = current.SimpleSize();
    auto cumulativeSize = current.CumulativeSize();
    auto histogramSize = current.HistogramSize();

    diff->MutableSimple()->Reserve(simpleSize);
    diff->MutableCumulative()->Reserve(cumulativeSize);
    diff->MutableHistogram()->Reserve(histogramSize);

    for (size_t i = 0; i < simpleSize; ++i) {
        diff->AddSimple(current.GetSimple(i));
    }

    diff->SetCumulativeCount(cumulativeSize);
    for (size_t i = 0; i < cumulativeSize; ++i) {
        auto value = current.GetCumulative(i);
        if (!value) {
            continue;
        }
        diff->AddCumulative(i);
        diff->AddCumulative(value);
    }

    for (size_t i = 0; i < histogramSize; ++i) {
        const auto& currentH = current.GetHistogram(i);
        auto bucketCount = currentH.BucketsSize();

        auto* histogram = diff->AddHistogram();
        histogram->MutableBuckets()->Reserve(bucketCount);
        histogram->SetBucketsCount(bucketCount);
        for (size_t b = 0; b < bucketCount; ++b) {
            auto value = currentH.GetBuckets(b);
            if (!value) {
                continue;
            }
            histogram->AddBuckets(b);
            histogram->AddBuckets(value);
        }
    }
}

static void CalculateCountersDiff(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current,
    NKikimrSysView::TDbCounters& prev)
{
    auto simpleSize = current.SimpleSize();
    auto cumulativeSize = current.CumulativeSize();
    auto histogramSize = current.HistogramSize();

    if (prev.SimpleSize() != simpleSize) {
        SVLOG_CRIT("CalculateCountersDiff: simple count mismatch, prev "
            << prev.SimpleSize() << ", current " << simpleSize);
        prev.MutableSimple()->Resize(simpleSize, 0);
    }
    if (prev.CumulativeSize() != cumulativeSize) {
        SVLOG_CRIT("CalculateCountersDiff: cumulative count mismatch, prev "
            << prev.CumulativeSize() << ", current " << cumulativeSize);
        prev.MutableCumulative()->Resize(cumulativeSize, 0);
    }
    if (prev.HistogramSize() != histogramSize) {
        SVLOG_CRIT("CalculateCountersDiff: histogram count mismatch, prev "
            << prev.HistogramSize() << ", current " << histogramSize);
        if (prev.HistogramSize() < histogramSize) {
            auto missing = histogramSize - prev.HistogramSize();
            for (; missing > 0; --missing) {
                prev.AddHistogram();
            }
        }
    }

    diff->MutableSimple()->Reserve(simpleSize);
    diff->MutableCumulative()->Reserve(cumulativeSize);
    diff->MutableHistogram()->Reserve(histogramSize);

    for (size_t i = 0; i < simpleSize; ++i) {
        diff->AddSimple(current.GetSimple(i));
    }

    diff->SetCumulativeCount(cumulativeSize);
    for (size_t i = 0; i < cumulativeSize; ++i) {
        auto value = current.GetCumulative(i) - prev.GetCumulative(i);
        if (!value) {
            continue;
        }
        diff->AddCumulative(i);
        diff->AddCumulative(value);
    }

    for (size_t i = 0; i < histogramSize; ++i) {
        const auto& currentH = current.GetHistogram(i);
        auto& prevH = *prev.MutableHistogram(i);
        auto bucketCount = currentH.BucketsSize();
        if (prevH.BucketsSize() != bucketCount) {
            SVLOG_CRIT("CalculateCountersDiff: histogram buckets count mismatch, index " << i
                << ", prev " << prevH.BucketsSize() << ", current " << bucketCount);
            prevH.MutableBuckets()->Resize(bucketCount, 0);
        }
        auto* histogram = diff->AddHistogram();
        histogram->MutableBuckets()->Reserve(bucketCount);
        histogram->SetBucketsCount(bucketCount);
        for (size_t b = 0; b < bucketCount; ++b) {
            auto value = currentH.GetBuckets(b) - prevH.GetBuckets(b);
            if (!value) {
                continue;
            }
            histogram->AddBuckets(b);
            histogram->AddBuckets(value);
        }
    }
}

static void CalculateCountersDiff(NKikimrSysView::TDbServiceCounters* diff,
    const NKikimr::NSysView::TDbServiceCounters& current,
    NKikimr::NSysView::TDbServiceCounters& prev)
{
    if (current.Proto().HasMain()) {
        if (prev.Proto().HasMain()) {
            CalculateCountersDiff(diff->MutableMain(),
                current.Proto().GetMain(), *prev.Proto().MutableMain());
        } else {
            CopyCounters(diff->MutableMain(), current.Proto().GetMain());
        }
    }

    for (const auto& currentT : current.Proto().GetTabletCounters()) {
        const auto type = currentT.GetType();
        auto* diffT = diff->AddTabletCounters();
        diffT->SetType(type);

        auto* prevT = prev.FindTabletCounters(type);
        if (prevT) {
            CalculateCountersDiff(diffT->MutableExecutorCounters(),
                currentT.GetExecutorCounters(), *prevT->MutableExecutorCounters());
            CalculateCountersDiff(diffT->MutableAppCounters(),
                currentT.GetAppCounters(), *prevT->MutableAppCounters());
        } else {
            CopyCounters(diffT->MutableExecutorCounters(), currentT.GetExecutorCounters());
            CopyCounters(diffT->MutableAppCounters(), currentT.GetAppCounters());
        }
        CopyCounters(diffT->MutableMaxExecutorCounters(), currentT.GetMaxExecutorCounters());
        CopyCounters(diffT->MutableMaxAppCounters(), currentT.GetMaxAppCounters());
    }

    for (const auto& currentR : current.Proto().GetGRpcCounters()) {
        const auto grpcService = currentR.GetGRpcService();
        const auto grpcRequest = currentR.GetGRpcRequest();
        auto* diffR = diff->AddGRpcCounters();
        diffR->SetGRpcService(grpcService);
        diffR->SetGRpcRequest(grpcRequest);

        auto* prevR = prev.FindGRpcCounters(grpcService, grpcRequest);
        if (prevR) {
            CalculateCountersDiff(diffR->MutableRequestCounters(),
                currentR.GetRequestCounters(), *prevR->MutableRequestCounters());
        } else {
            CopyCounters(diffR->MutableRequestCounters(), currentR.GetRequestCounters());
        }
    }

    if (current.Proto().HasGRpcProxyCounters()) {
        if (prev.Proto().HasGRpcProxyCounters()) {
            CalculateCountersDiff(diff->MutableGRpcProxyCounters()->MutableRequestCounters(),
                current.Proto().GetGRpcProxyCounters().GetRequestCounters(),
                *prev.Proto().MutableGRpcProxyCounters()->MutableRequestCounters());
        } else {
            CopyCounters(diff->MutableGRpcProxyCounters()->MutableRequestCounters(),
                current.Proto().GetGRpcProxyCounters().GetRequestCounters());
        }
    }
}

class TSysViewService : public TActorBootstrapped<TSysViewService> {
public:
    using TBase = TActorBootstrapped<TSysViewService>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SYSTEM_VIEW_SERVICE;
    }

    explicit TSysViewService(
        TExtCountersConfig&& config,
        bool hasExternalCounters,
        EProcessorMode processorMode)
        : Config(std::move(config))
        , HasExternalCounters(hasExternalCounters)
        , TotalInterval(TDuration::Seconds(processorMode == EProcessorMode::FAST ? 6 : 60))
        , CollectInterval(TDuration::Seconds(processorMode == EProcessorMode::FAST ? 3 : 30))
        , SendInterval(TDuration::Seconds(processorMode == EProcessorMode::FAST ? 2 : 20))
        , ProcessLabeledCountersInterval(TDuration::Seconds(processorMode == EProcessorMode::FAST ? 5 : 60))
    {}

    void Bootstrap(const TActorContext &ctx) {
        auto now = AppData()->TimeProvider->Now();

        TopByDuration1Minute = MakeHolder<TServiceQueryHistory<TDurationGreater>>(
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE, now);
        TopByDuration1Hour = MakeHolder<TServiceQueryHistory<TDurationGreater>>(
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE, now);

        TopByReadBytes1Minute = MakeHolder<TServiceQueryHistory<TReadBytesGreater>>(
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE, now);
        TopByReadBytes1Hour = MakeHolder<TServiceQueryHistory<TReadBytesGreater>>(
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE, now);

        TopByCpuTime1Minute = MakeHolder<TServiceQueryHistory<TCpuTimeGreater>>(
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE, now);
        TopByCpuTime1Hour = MakeHolder<TServiceQueryHistory<TCpuTimeGreater>>(
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE, now);

        TopByRequestUnits1Minute = MakeHolder<TServiceQueryHistory<TRequestUnitsGreater>>(
            ONE_MINUTE_BUCKET_COUNT, ONE_MINUTE_BUCKET_SIZE, now);
        TopByRequestUnits1Hour = MakeHolder<TServiceQueryHistory<TRequestUnitsGreater>>(
            ONE_HOUR_BUCKET_COUNT, ONE_HOUR_BUCKET_SIZE, now);

        ScanLimiter = MakeIntrusive<TScanLimiter>(ConcurrentScansLimit);

        if (AppData()->FeatureFlags.GetEnablePersistentQueryStats()) {
            IntervalEnd = GetNextIntervalEnd();
            Schedule(IntervalEnd, new TEvPrivate::TEvProcessInterval(IntervalEnd));
        }

        if (AppData()->FeatureFlags.GetEnableDbCounters()) {
            {
                auto intervalSize = ProcessCountersInterval.MicroSeconds();
                auto deadline = (TInstant::Now().MicroSeconds() / intervalSize + 1) * intervalSize;
                deadline += RandomNumber<ui64>(intervalSize / 5);
                Schedule(TInstant::MicroSeconds(deadline), new TEvPrivate::TEvProcessCounters());
            }

            {
                auto intervalSize = ProcessLabeledCountersInterval.MicroSeconds();
                auto deadline = (TInstant::Now().MicroSeconds() / intervalSize + 1) * intervalSize;
                deadline += RandomNumber<ui64>(intervalSize / 5);
                Schedule(TInstant::MicroSeconds(deadline), new TEvPrivate::TEvProcessLabeledCounters());
            }

            auto callback = MakeIntrusive<TServiceDbWatcherCallback>(ctx.ActorSystem());
            DbWatcherActorId = ctx.Register(CreateDbWatcherActor(callback));
        }

        if (HasExternalCounters) {
            ctx.Register(CreateExtCountersUpdater(std::move(Config)));
        }

        Become(&TSysViewService::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSysView::TEvCollectQueryStats, Handle);
            hFunc(TEvSysView::TEvGetQueryStats, Handle);
            hFunc(TEvSysView::TEvGetScanLimiter, Handle);
            hFunc(TEvPrivate::TEvProcessInterval, Handle);
            hFunc(TEvPrivate::TEvSendSummary, Handle);
            hFunc(TEvPrivate::TEvProcessCounters, Handle);
            hFunc(TEvPrivate::TEvProcessLabeledCounters, Handle);
            hFunc(TEvPrivate::TEvRemoveDatabase, Handle);
            hFunc(TEvSysView::TEvRegisterDbCounters, Handle);
            hFunc(TEvSysView::TEvSendDbCountersResponse, Handle);
            hFunc(TEvSysView::TEvSendDbLabeledCountersResponse, Handle);
            hFunc(TEvSysView::TEvGetIntervalMetricsRequest, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                SVLOG_CRIT("NSysView::TSysViewService: unexpected event# "
                    << ev->GetTypeRewrite());
        }
    }

private:
    struct TEvPrivate {
        enum EEv {
            EvProcessInterval = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvSendSummary,
            EvProcessCounters,
            EvProcessLabeledCounters,
            EvRemoveDatabase,
            EvEnd
        };

        struct TEvProcessInterval : public TEventLocal<TEvProcessInterval, EvProcessInterval> {
            TInstant IntervalEnd;

            explicit TEvProcessInterval(TInstant intervalEnd)
                : IntervalEnd(intervalEnd)
            {}
        };

        struct TEvSendSummary : public TEventLocal<TEvSendSummary, EvSendSummary> {
            TInstant IntervalEnd;
            TString Database;

            TEvSendSummary(TInstant intervalEnd, const TString& database)
                : IntervalEnd(intervalEnd)
                , Database(database)
            {}
        };

        struct TEvProcessCounters : public TEventLocal<TEvProcessCounters, EvProcessCounters> {
        };

        struct TEvProcessLabeledCounters : public TEventLocal<TEvProcessLabeledCounters, EvProcessLabeledCounters> {
        };

        struct TEvRemoveDatabase : public TEventLocal<TEvRemoveDatabase, EvRemoveDatabase> {
            TString Database;
            TPathId PathId;

            TEvRemoveDatabase(const TString& database, TPathId pathId)
                : Database(database)
                , PathId(pathId)
            {}
        };
    };

    class TServiceDbWatcherCallback : public TDbWatcherCallback {
        TActorSystem* ActorSystem = {};

    public:
        explicit TServiceDbWatcherCallback(TActorSystem* actorSystem)
            : ActorSystem(actorSystem)
        {}

        void OnDatabaseRemoved(const TString& database, TPathId pathId) override {
            auto evRemove = MakeHolder<TEvPrivate::TEvRemoveDatabase>(database, pathId);
            auto service = MakeSysViewServiceID(ActorSystem->NodeId);
            ActorSystem->Send(service, evRemove.Release());
        }
    };

    TInstant GetNextIntervalEnd() {
        auto intervalSize = TotalInterval.MicroSeconds();
        auto rounded = (Now().MicroSeconds() / intervalSize + 1) * intervalSize;
        return TInstant::MicroSeconds(rounded);
    }

    void SendSummary(TInstant intervalEnd, const TString& database) {
        auto processorId = GetProcessorId(database);
        if (!processorId) {
            return;
        }

        auto summary = MakeHolder<TEvSysView::TEvIntervalQuerySummary>();
        auto& record = summary->Record;
        record.SetDatabase(database);
        record.SetIntervalEndUs(intervalEnd.MicroSeconds());
        record.SetNodeId(SelfId().NodeId());

        auto& log = QueryLogs[database];
        log.Metrics.Report.FillSummary(*record.MutableMetrics());
        log.TopByDuration.Report.FillSummary(*record.MutableTopByDuration());
        log.TopByReadBytes.Report.FillSummary(*record.MutableTopByReadBytes());
        log.TopByCpuTime.Report.FillSummary(*record.MutableTopByCpuTime());
        log.TopByRequestUnits.Report.FillSummary(*record.MutableTopByRequestUnits());

        SVLOG_D("Send interval summary: "
            << "service id# " << SelfId()
            << ", processor id# " << processorId
            << ", database# " << database
            << ", interval end# " << intervalEnd
            << ", query count# " << record.GetMetrics().HashesSize());

        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(summary.Release(), processorId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void Rotate() {
        auto summaryEnd = IntervalEnd;
        auto delta = Now() - summaryEnd;

        IntervalEnd = GetNextIntervalEnd();
        Schedule(IntervalEnd, new TEvPrivate::TEvProcessInterval(IntervalEnd));

        Attempts.clear();

        SVLOG_D("Rotate logs: service id# " << SelfId()
            << ", query logs count# " << QueryLogs.size()
            << ", processor ids count# " << ProcessorIds.size()
            << ", processor id to database count# " << ProcessorIdToDatabase.size());

        if (QueryLogs.empty()) {
            return;
        }

        if (delta > CollectInterval) {
            // too late to send summaries, skip
            QueryLogs.clear();
            return;
        }

        for (auto it = QueryLogs.begin(); it != QueryLogs.end(); ) {
            it->second.Rotate();
            if (it->second.Metrics.Report.Empty()) {
                it = QueryLogs.erase(it);
            } else {
                ++it;
            }
        }

        if (delta >= SendInterval) {
            // send immediately
            for (const auto& [database, _] : QueryLogs) {
                SendSummary(summaryEnd, database);
            }
            return;
        }

        // randomize send time
        auto windowUs = (SendInterval - delta).MicroSeconds() / 2;
        auto now = Now();
        for (const auto& [database, _] : QueryLogs) {
            TInstant deadline = now + TDuration::MicroSeconds(windowUs + RandomNumber<ui64>(windowUs));
            Schedule(deadline, new TEvPrivate::TEvSendSummary(summaryEnd, database));
            Attempts[database] = std::make_pair(summaryEnd, 1);
        }
    }

    ui64 GetProcessorId(const TString& database) {
        auto it = ProcessorIds.find(database);
        if (it == ProcessorIds.end() || !it->second) {
            return 0;
        }
        auto processorId = *it->second;
        if (!processorId) {
            RequestProcessorId(database);
            return 0;
        }
        return processorId;
    }

    void RequestProcessorId(const TString& database) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.push_back({});

        auto& entry = request->ResultSet.back();
        entry.Path = SplitPath(database);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    template <typename T>
        requires std::is_same_v<T, TEvSysView::TEvSendDbCountersRequest> ||
                 std::is_same_v<T, TEvSysView::TEvSendDbLabeledCountersRequest>
    void SendCounters(const TString& database) {
        auto processorId = GetProcessorId(database);
        if (!processorId) {
            return;
        }

        constexpr bool isLabeled = std::is_same<T, TEvSysView::TEvSendDbLabeledCountersRequest>::value;
        auto& dbCounters = isLabeled ? DatabaseLabeledCounters[database] : DatabaseCounters[database];
        auto sendEv = MakeHolder<T>();
        auto& record = sendEv->Record;

        if (dbCounters.IsConfirmed) {
            for (auto& [service, state] : dbCounters.States) {
                state.Counters->ToProto(state.Current);
            }
            ++dbCounters.Generation;
            dbCounters.IsConfirmed = false;
            dbCounters.IsRetrying = false;
        } else {
            dbCounters.IsRetrying = true;
        }

        record.SetGeneration(dbCounters.Generation);
        record.SetNodeId(SelfId().NodeId());

        for (auto& [service, state] : dbCounters.States) {
            auto* serviceCounters = record.AddServiceCounters();
            serviceCounters->SetService(service);
            auto* diff = serviceCounters->MutableCounters();

            if (isLabeled) {
                diff->CopyFrom(state.Current.Proto());
            } else {
                CalculateCountersDiff(diff, state.Current, state.Confirmed);
            }
        }

        SVLOG_D("Send counters: "
            << "service id# " << SelfId()
            << ", processor id# " << processorId
            << ", database# " << database
            << ", generation# " << record.GetGeneration()
            << ", node id# " << record.GetNodeId()
            << ", is retrying# " << dbCounters.IsRetrying
            << ", is labeled# " << isLabeled);

        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(sendEv.Release(), processorId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void RequestDatabaseName(TPathId pathId) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.push_back({});

        auto& entry = request->ResultSet.back();
        entry.TableId.PathId = pathId;
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void RegisterDbCounters(const TString& database, NKikimrSysView::EDbCountersService service,
        TIntrusivePtr<IDbCounters> counters)
    {
        auto [it, inserted] = DatabaseCounters.try_emplace(database, TDbCounters());
        if (inserted) {
            if (ProcessorIds.find(database) == ProcessorIds.end()) {
                RequestProcessorId(database);
            }

            if (DbWatcherActorId) {
                auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(database);
                Send(DbWatcherActorId, evWatch.Release());
            }
        }
        it->second.States[service].Counters = counters;
    }

    void RegisterDbLabeledCounters(const TString& database, NKikimrSysView::EDbCountersService service,
        TIntrusivePtr<IDbCounters> counters)
    {
        auto [it, inserted] = DatabaseLabeledCounters.try_emplace(database, TDbCounters());
        if (inserted) {
            if (ProcessorIds.find(database) == ProcessorIds.end()) {
                RequestProcessorId(database);
            }

            if (DbWatcherActorId) {
                auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(database);
                Send(DbWatcherActorId, evWatch.Release());
            }
        }
        it->second.States[service].Counters = counters;
    }

    void Handle(TEvPrivate::TEvSendSummary::TPtr& ev) {
        auto prevIntervalEnd = IntervalEnd - TotalInterval;
        auto intervalEnd = ev->Get()->IntervalEnd;
        if (intervalEnd != prevIntervalEnd) {
            return;
        }
        if (Now() > intervalEnd + CollectInterval) {
            return;
        }
        SendSummary(intervalEnd, ev->Get()->Database);
    }

    void Handle(TEvPrivate::TEvProcessInterval::TPtr& ev) {
        SVLOG_D("Handle TEvPrivate::TEvProcessInterval: "
            << "service id# " << SelfId()
            << ", interval end# " << IntervalEnd
            << ", event interval end# " << ev->Get()->IntervalEnd);

        if (IntervalEnd == ev->Get()->IntervalEnd) {
            Rotate();
        }
    }

    void Handle(TEvSysView::TEvGetIntervalMetricsRequest::TPtr& ev) {
        auto response = MakeHolder<TEvSysView::TEvGetIntervalMetricsResponse>();

        if (!AppData()->FeatureFlags.GetEnablePersistentQueryStats()) {
            Send(ev->Sender, std::move(response), 0, ev->Cookie);
            return;
        }

        const auto& record = ev->Get()->Record;
        response->Record.SetIntervalEndUs(record.GetIntervalEndUs());
        const auto& database = record.GetDatabase();

        auto prevIntervalEnd = IntervalEnd - TotalInterval;
        if (record.GetIntervalEndUs() != prevIntervalEnd.MicroSeconds()) {
            SVLOG_W("Handle TEvSysView::TEvGetIntervalMetricsRequest, time mismatch: "
                << "service id# " << SelfId()
                << ", database# " << database
                << ", prev interval end# " << prevIntervalEnd
                << ", event interval end# " << record.GetIntervalEndUs());

            Send(ev->Sender, std::move(response), 0, ev->Cookie);
            return;
        }

        auto it = QueryLogs.find(database);
        if (it == QueryLogs.end()) {
            SVLOG_W("Handle TEvSysView::TEvGetIntervalMetricsRequest, no database: "
                << "service id# " << SelfId()
                << ", database# " << database
                << ", prev interval end# " << prevIntervalEnd);

            Send(ev->Sender, std::move(response), 0, ev->Cookie);
            return;
        }

        auto& log = it->second;
        log.Metrics.Report.FillMetrics(record, response->Record);
        log.TopByDuration.Report.FillStats(
            record.GetTopByDuration(), *response->Record.MutableTopByDuration());
        log.TopByReadBytes.Report.FillStats(
            record.GetTopByReadBytes(), *response->Record.MutableTopByReadBytes());
        log.TopByCpuTime.Report.FillStats(
            record.GetTopByCpuTime(), *response->Record.MutableTopByCpuTime());
        log.TopByRequestUnits.Report.FillStats(
            record.GetTopByRequestUnits(), *response->Record.MutableTopByRequestUnits());

        SVLOG_D("Handle TEvSysView::TEvGetIntervalMetricsRequest: "
            << "service id# " << SelfId()
            << ", database# " << database
            << ", prev interval end# " << prevIntervalEnd
            << ", metrics count# " << response->Record.MetricsSize()
            << ", texts count# " << response->Record.QueryTextsSize());

        Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void Handle(TEvPrivate::TEvProcessCounters::TPtr&) {
        SVLOG_D("Handle TEvPrivate::TEvProcessCounters: "
            << "service id# " << SelfId());

        for (auto& [database, dbCounters] : DatabaseCounters) {
            SendCounters<TEvSysView::TEvSendDbCountersRequest>(database);
        }

        Schedule(ProcessCountersInterval, new TEvPrivate::TEvProcessCounters());
    }

    void Handle(TEvPrivate::TEvProcessLabeledCounters::TPtr&) {
        SVLOG_D("Handle TEvPrivate::TEvProcessLabeledCounters: "
            << "service id# " << SelfId());

        for (auto& [database, dbCounters] : DatabaseLabeledCounters) {
            SendCounters<TEvSysView::TEvSendDbLabeledCountersRequest>(database);
        }

        Schedule(ProcessLabeledCountersInterval, new TEvPrivate::TEvProcessLabeledCounters());
    }

    void Handle(TEvPrivate::TEvRemoveDatabase::TPtr& ev) {
        auto database = ev->Get()->Database;
        auto pathId = ev->Get()->PathId;

        SVLOG_D("Handle TEvPrivate::TEvRemoveDatabase: "
            << "database# " << database
            << ", pathId# " << pathId);

        QueryLogs.erase(database);
        if (auto it = ProcessorIds.find(database); it != ProcessorIds.end()) {
            if (it->second) {
                ProcessorIdToDatabase.erase(*it->second);
            }
        }
        ProcessorIds.erase(database);
        Attempts.erase(database);
        DatabaseCounters.erase(database);
        DatabaseLabeledCounters.erase(database);
        UnresolvedTabletCounters.erase(pathId);
    }

    void Handle(TEvSysView::TEvSendDbCountersResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& database = record.GetDatabase();
        const auto generation = record.GetGeneration();

        auto it = DatabaseCounters.find(database);
        if (it == DatabaseCounters.end()) {
            SVLOG_W("Handle TEvSysView::TEvSendDbCountersResponse: "
                << "service id# " << SelfId()
                << ", unknown database# " << database);
            return;
        }

        auto& dbCounters = it->second;
        if (generation != dbCounters.Generation) {
            SVLOG_W("Handle TEvSysView::TEvSendDbCountersResponse, wrong generation: "
                << "service id# " << SelfId()
                << ", database# " << database
                << ", generation# " << generation
                << ", service generation# " << dbCounters.Generation);
            return;
        }

        dbCounters.IsConfirmed = true;
        for (auto& [_, state] : dbCounters.States) {
            state.Confirmed.Swap(state.Current);
        }

        if (dbCounters.IsRetrying) {
            SendCounters<TEvSysView::TEvSendDbCountersRequest>(database);
        }

        SVLOG_D("Handle TEvSysView::TEvSendDbCountersResponse: "
            << "service id# " << SelfId()
            << ", database# " << database
            << ", generation# " << generation);
    }

    void Handle(TEvSysView::TEvSendDbLabeledCountersResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& database = record.GetDatabase();
        const auto generation = record.GetGeneration();

        auto it = DatabaseLabeledCounters.find(database);
        if (it == DatabaseLabeledCounters.end()) {
            return;
        }

        auto& dbCounters = it->second;
        if (generation != dbCounters.Generation) {
            return;
        }

        dbCounters.IsConfirmed = true;
        for (auto& [_, state] : dbCounters.States) {
            state.Confirmed.Swap(state.Current);
        }

        if (dbCounters.IsRetrying) {
            SendCounters<TEvSysView::TEvSendDbLabeledCountersRequest>(database);
        }

        SVLOG_D("Handle TEvSysView::TEvSendDbLabeledCountersResponse: "
            << "service id# " << SelfId()
            << ", database# " << database
            << ", generation# " << generation);
    }

    void Handle(TEvSysView::TEvRegisterDbCounters::TPtr& ev) {
        const auto service = ev->Get()->Service;

        if (service == NKikimrSysView::TABLETS) { // register by path id
            auto pathId = ev->Get()->PathId;
            UnresolvedTabletCounters[pathId] = ev->Get()->Counters;
            RequestDatabaseName(pathId);

            SVLOG_D("Handle TEvSysView::TEvRegisterDbCounters: "
                << "service id# " << SelfId()
                << ", path id# " << pathId
                << ", service# " << (int)service);

        } else if (service == NKikimrSysView::LABELED) {
            const auto& database = ev->Get()->Database;
            RegisterDbLabeledCounters(database, service, ev->Get()->Counters);

            SVLOG_D("Handle TEvSysView::TEvRegisterDbLabeledCounters: "
                << "service id# " << SelfId()
                << ", database# " << database
                << ", service# " << (int)service);

        } else { // register by database name
            const auto& database = ev->Get()->Database;
            RegisterDbCounters(database, service, ev->Get()->Counters);

            SVLOG_D("Handle TEvSysView::TEvRegisterDbCounters: "
                << "service id# " << SelfId()
                << ", database# " << database
                << ", service# " << (int)service);
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        ui64 processorId = ev->Get()->TabletId;
        TString database;
        auto it = ProcessorIdToDatabase.find(processorId);
        if (it != ProcessorIdToDatabase.end()) {
            database = it->second;
            RequestProcessorId(database);
        }

        SVLOG_W("Summary delivery problem: service id# " << SelfId()
            << ", processor id# " << processorId
            << ", database# " << database);

        if (!database) {
            return;
        }

        auto attemptIt = Attempts.find(database);
        if (attemptIt == Attempts.end()) {
            return;
        }

        auto& attempt = attemptIt->second;
        if (++attempt.second > SummaryRetryAttempts) {
            return;
        }

        auto summaryEnd = attempt.first;
        auto deadline = Now() + SummaryRetryInterval;
        Schedule(deadline, new TEvPrivate::TEvSendSummary(summaryEnd, database));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;

        THolder<TNavigate> request(ev->Get()->Request.Release());
        Y_ABORT_UNLESS(request->ResultSet.size() == 1);
        auto& entry = request->ResultSet.back();

        if (entry.RequestType == TNavigate::TEntry::ERequestType::ByTableId) {
            auto pathId = entry.TableId.PathId;

            if (entry.Status != TNavigate::EStatus::Ok) {
                SVLOG_W("Navigate by path id failed: service id# " << SelfId()
                    << ", path id# " << pathId
                    << ", status# " << entry.Status);
                return;
            }

            auto database = CanonizePath(entry.Path);
            auto it = UnresolvedTabletCounters.find(pathId);
            if (it == UnresolvedTabletCounters.end()) {
                return;
            }

            RegisterDbCounters(database, NKikimrSysView::TABLETS, it->second);
            UnresolvedTabletCounters.erase(it);

            SVLOG_I("Navigate by path id succeeded: service id# " << SelfId()
                << ", path id# " << pathId
                << ", database# " << database);

        } else {
            auto database = CanonizePath(entry.Path);

            if (entry.Status != TNavigate::EStatus::Ok) {
                SVLOG_W("Navigate by database failed: service id# " << SelfId()
                    << ", database# " << database
                    << ", status# " << entry.Status);
                ProcessorIds.erase(database);
                return;
            }

            if (entry.DomainInfo->Params.HasSysViewProcessor()) {
                auto processorId = entry.DomainInfo->Params.GetSysViewProcessor();
                ProcessorIds[database] = processorId;
                ProcessorIdToDatabase[processorId] = database;

                SVLOG_I("Navigate by database succeeded: service id# " << SelfId()
                    << ", database# " << database
                    << ", processor id# " << processorId);
            } else {
                ProcessorIds[database] = 0;

                SVLOG_I("Navigate by database succeeded: service id# " << SelfId()
                    << ", database# " << database
                    << ", no sysview processor");
            }
        }
    }

    void Handle(TEvSysView::TEvCollectQueryStats::TPtr& ev) {
        const auto& database = ev->Get()->Database;

        auto stats = std::make_shared<NKikimrSysView::TQueryStats>();
        stats->Swap(&ev->Get()->QueryStats);

        SVLOG_T("Collect query stats: service id# " << SelfId()
            << ", database# " << database
            << ", query hash# " << stats->GetQueryTextHash()
            << ", cpu time# " << stats->GetTotalCpuTimeUs());

        if (AppData()->FeatureFlags.GetEnablePersistentQueryStats() && !database.empty()) {
            auto queryEnd = TInstant::MilliSeconds(stats->GetEndTimeMs());
            if (queryEnd < IntervalEnd - TotalInterval) {
                return;
            }
            if (queryEnd >= IntervalEnd) {
                Rotate();
            }

            QueryLogs[database].Add(stats);

            if (ProcessorIds.find(database) == ProcessorIds.end()) {
                ProcessorIds[database];
                RequestProcessorId(database);

                if (DbWatcherActorId) {
                    auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(database);
                    Send(DbWatcherActorId, evWatch.Release());
                }
            }
        }

        // gather old style stats while migrating
        // TODO: remove later
        TopByDuration1Minute->Add(stats);
        TopByDuration1Hour->Add(stats);
        TopByReadBytes1Minute->Add(stats);
        TopByReadBytes1Hour->Add(stats);
        TopByCpuTime1Minute->Add(stats);
        TopByCpuTime1Hour->Add(stats);
        TopByRequestUnits1Minute->Add(stats);
        TopByRequestUnits1Hour->Add(stats);
    }

    void Handle(TEvSysView::TEvGetQueryStats::TPtr& ev) {
        auto& record = ev->Get()->Record;
        auto result = MakeHolder<TEvSysView::TEvGetQueryStatsResult>();

        ui64 startBucket = 0;
        if (record.HasStartBucket()) {
            startBucket = record.GetStartBucket();
        }

        if (!record.HasTenantName() ||
            record.GetTenantName() != AppData()->TenantName)
        {
            Send(ev->Sender, std::move(result), 0, ev->Cookie);
            return;
        }

        switch (record.GetStatsType()) {
            case NKikimrSysView::TOP_DURATION_ONE_MINUTE:
                TopByDuration1Minute->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_DURATION_ONE_HOUR:
                TopByDuration1Hour->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE:
                TopByReadBytes1Minute->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_READ_BYTES_ONE_HOUR:
                TopByReadBytes1Hour->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE:
                TopByCpuTime1Minute->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_CPU_TIME_ONE_HOUR:
                TopByCpuTime1Hour->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE:
                TopByRequestUnits1Minute->ToProto(startBucket, result->Record);
                break;
            case NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR:
                TopByRequestUnits1Hour->ToProto(startBucket, result->Record);
                break;
            default:
                SVLOG_CRIT("NSysView::TSysViewService: unexpected query stats type# "
                    << (size_t)record.GetStatsType());
                // send empty result
                break;
        }

        Send(ev->Sender, std::move(result), 0, ev->Cookie);
    }

    void Handle(TEvSysView::TEvGetScanLimiter::TPtr& ev) {
        auto result = MakeHolder<TEvSysView::TEvGetScanLimiterResult>();
        result->ScanLimiter = ScanLimiter;
        Send(ev->Sender, std::move(result));
    }

    void PassAway() override {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    TExtCountersConfig Config;
    const bool HasExternalCounters;
    const TDuration TotalInterval;
    const TDuration CollectInterval;
    const TDuration SendInterval;
    const TDuration ProcessLabeledCountersInterval;

    template <typename TInterval>
    struct TDbWindow {
        TInterval Collect;
        TInterval Report;

        void Clear() {
            Collect.Clear();
            Report.Clear();
        }

        void Rotate() {
            Report.Clear();
            Report.Swap(Collect);
        }
    };

    struct TDbQueryLog {
        TDbWindow<TQueryInterval> Metrics;
        TDbWindow<TQueryStatsDedupBucket<TDurationGreater>> TopByDuration;
        TDbWindow<TQueryStatsDedupBucket<TReadBytesGreater>> TopByReadBytes;
        TDbWindow<TQueryStatsDedupBucket<TCpuTimeGreater>> TopByCpuTime;
        TDbWindow<TQueryStatsDedupBucket<TRequestUnitsGreater>> TopByRequestUnits;

        void Clear() {
            Metrics.Clear();
            TopByDuration.Clear();
            TopByReadBytes.Clear();
            TopByCpuTime.Clear();
            TopByRequestUnits.Clear();
        }

        void Rotate() {
            Metrics.Rotate();
            TopByDuration.Rotate();
            TopByReadBytes.Rotate();
            TopByCpuTime.Rotate();
            TopByRequestUnits.Rotate();
        }

        void Add(TQueryStatsPtr stats) {
            Metrics.Collect.Add(stats);
            TopByDuration.Collect.Add(stats);
            TopByReadBytes.Collect.Add(stats);
            TopByCpuTime.Collect.Add(stats);
            TopByRequestUnits.Collect.Add(stats);
        }
    };
    std::unordered_map<TString, TDbQueryLog> QueryLogs;

    TInstant IntervalEnd;

    std::unordered_map<TString, TMaybe<ui64>> ProcessorIds;
    std::unordered_map<ui64, TString> ProcessorIdToDatabase;

    std::unordered_map<TString, std::pair<TInstant, size_t>> Attempts;

    THolder<TServiceQueryHistory<TDurationGreater>> TopByDuration1Minute;
    THolder<TServiceQueryHistory<TDurationGreater>> TopByDuration1Hour;

    THolder<TServiceQueryHistory<TReadBytesGreater>> TopByReadBytes1Minute;
    THolder<TServiceQueryHistory<TReadBytesGreater>> TopByReadBytes1Hour;

    THolder<TServiceQueryHistory<TCpuTimeGreater>> TopByCpuTime1Minute;
    THolder<TServiceQueryHistory<TCpuTimeGreater>> TopByCpuTime1Hour;

    THolder<TServiceQueryHistory<TRequestUnitsGreater>> TopByRequestUnits1Minute;
    THolder<TServiceQueryHistory<TRequestUnitsGreater>> TopByRequestUnits1Hour;

    struct TDbCountersState {
        TIntrusivePtr<IDbCounters> Counters;
        NKikimr::NSysView::TDbServiceCounters Current;
        NKikimr::NSysView::TDbServiceCounters Confirmed;
    };

    struct TDbCounters {
        std::unordered_map<NKikimrSysView::EDbCountersService, TDbCountersState> States;
        ui64 Generation;
        bool IsConfirmed = true;
        bool IsRetrying = false;

        TDbCounters()
            : Generation(RandomNumber<ui64>())
        {}
    };

    std::unordered_map<TString, TDbCounters> DatabaseCounters;
    std::unordered_map<TString, TDbCounters> DatabaseLabeledCounters;
    THashMap<TPathId, TIntrusivePtr<IDbCounters>> UnresolvedTabletCounters;
    TActorId DbWatcherActorId;

    static constexpr i64 ConcurrentScansLimit = 5;
    TIntrusivePtr<TScanLimiter> ScanLimiter;

    static constexpr TDuration SummaryRetryInterval = TDuration::Seconds(2);
    static constexpr size_t SummaryRetryAttempts = 5;

    static constexpr TDuration ProcessCountersInterval = TDuration::Seconds(5);
};

THolder<NActors::IActor> CreateSysViewService(
    TExtCountersConfig&& config, bool hasExternalCounters)
{
    return MakeHolder<TSysViewService>(
        std::move(config), hasExternalCounters, EProcessorMode::MINUTE);
}

THolder<NActors::IActor> CreateSysViewServiceForTests() {
    return MakeHolder<TSysViewService>(
        TExtCountersConfig(), true, EProcessorMode::FAST);
}

} // NSysView
} // NKikimr
