#include "rate_accounting.h"

#include "probes.h"

#include <ydb/core/metering/metering.h>
#include <ydb/core/util/token_bucket.h>
#include <ydb/core/metering/time_grid.h>
#include <ydb/core/metering/bill_record.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/string/builder.h>
#include <util/generic/deque.h>

LWTRACE_USING(KESUS_QUOTER_PROVIDER);

namespace NKikimr {
namespace NKesus {

namespace {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////
struct TEvPrivate {
    enum EEv {
        EvRunAccounting = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvConfigure,
        EvCounters,
        EvEnd
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
        "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvRunAccounting : public TEventLocal<TEvRunAccounting, EvRunAccounting> {
        TInstant Time;
        TConsumptionHistory History;

        TEvRunAccounting(TInstant time, TConsumptionHistory&& history)
            : Time(time)
            , History(std::move(history))
        {}
    };

    struct TEvConfigure : public TEventLocal<TEvConfigure, EvConfigure> {
        TInstant ApplyAt;
        NKikimrKesus::TStreamingQuoterResource Props;

        TEvConfigure(TInstant applyAt, const NKikimrKesus::TStreamingQuoterResource& props)
            : ApplyAt(applyAt)
            , Props(props)
        {}
    };

    struct TEvCounters : public TEventLocal<TEvCounters, EvCounters> {
        TRateAccountingCounters Counters;

        TEvCounters(const TRateAccountingCounters& counters)
            : Counters(counters)
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////
inline NJson::TJsonMap ToJsonMap(const google::protobuf::Map<TString, TString>& o) {
    NJson::TJsonMap m;
    for (auto& [k, v] : o) {
        m[k] = v;
    }
    return m;
}

////////////////////////////////////////////////////////////////////////////////
class TBillingMetric {
    double Quantity = 0; // total consumption
    TInstant LastBillTime; // start of last billing period
    TInstant Accumulated; // start of accumulation `Quantity`

    // Monitoring
    ::NMonitoring::TDynamicCounters::TCounterPtr Billed;

    // Configuration
    NKikimrKesus::TAccountingConfig::TMetric Cfg;
    TTimeGrid TimeGrid; // billing metric time grid
    TString QuoterPath;
    TString ResourcePath;
    TString Category;
    IBillSink::TPtr BillSink;

public:
    explicit TBillingMetric()
        : TimeGrid(TDuration::Minutes(1))
    {}

    static TTimeGrid TimeGridForPeriod(int seconds) {
        // Period must be in [2, 3600] seconds interval and be divisor of an hour
        seconds = std::clamp(seconds, 2, 3600);
        for (; 3600 % seconds != 0; seconds--) {}
        return TTimeGrid(TDuration::Seconds(seconds));
    }

    void SetResourceCounters(const ::NMonitoring::TDynamicCounters::TCounterPtr& billed) {
        Billed = billed;
    }

    void Configure(const NKikimrKesus::TAccountingConfig::TMetric& cfg,
        const TString& quoterPath,
        const TString& resourcePath,
        const TString& category,
        const IBillSink::TPtr& billSink)
    {
        if (cfg.GetBillingPeriodSec() != Cfg.GetBillingPeriodSec()) {
            TimeGrid = TimeGridForPeriod(cfg.GetBillingPeriodSec());
            ResetAccumulated(); // just reset data aligned with old grid (underbill is better than overbill)
        }
        Cfg = cfg;
        QuoterPath = quoterPath;
        ResourcePath = resourcePath;
        Category = category;
        BillSink = billSink;
    }

    void Add(double value, TInstant t, const NActors::TActorContext& ctx) {
        SendBill(t, ctx);
        if (!Accumulated) {
            Accumulated = t;
        }
        Quantity += value;
        LWPROBE(ResourceBillAdd,
            QuoterPath,
            ResourcePath,
            Category,
            Accumulated,
            t,
            t - Accumulated,
            Quantity,
            value);
    }

private:
    void SendBill(TInstant t, const NActors::TActorContext& ctx) {
        auto curr = TimeGrid.Get(t);
        auto bill = TimeGrid.Get(Accumulated);
        auto last = TimeGrid.Get(LastBillTime);
        if ((LastBillTime && bill.Start <= last.End) || curr.Start <= bill.End) {
            return; // too early to bill
        }

        LastBillTime = bill.Start;
        ui64 BillQuantity = std::ceil(Quantity);
        if (Billed) {
            *Billed += BillQuantity;
        }
        if (Cfg.GetEnabled() && BillQuantity > 0) {
            const TString id = TStringBuilder()
                << ResourcePath
                << "-" << Category
                << "-" << bill.Start.Seconds()
                << "-" << bill.End.Seconds();

            const TBillRecord billRecord = TBillRecord()
                .Id(id)
                .SourceWt(TActivationContext::Now())
                .Usage(TBillRecord::RequestUnits(BillQuantity, bill.Start, bill.End))
                .Version(Cfg.GetVersion())
                .Schema(Cfg.GetSchema())
                .CloudId(Cfg.GetCloudId())
                .FolderId(Cfg.GetFolderId())
                .ResourceId(Cfg.GetResourceId())
                .SourceId(Cfg.GetSourceId())
                .Tags(ToJsonMap(Cfg.GetTags()))
                .Labels(ToJsonMap(Cfg.GetLabels()));

            LWPROBE(ResourceBillSend,
                QuoterPath,
                ResourcePath,
                Category,
                BillQuantity,
                bill.Start,
                bill.End,
                billRecord.Version_,
                billRecord.Schema_,
                billRecord.CloudId_,
                billRecord.FolderId_,
                billRecord.ResourceId_,
                billRecord.SourceId_);
            BillSink->Send(ctx, billRecord);
        }

        ResetAccumulated();
    }

    void ResetAccumulated() {
        Quantity = 0;
        Accumulated = TInstant::Zero();
    }
};

////////////////////////////////////////////////////////////////////////////////
class TAccountingActor final: public TActor<TAccountingActor> {
private:
    // Accounting (aggregate history intervals and split consumption into billing categories)
    TInstant Time; // current history replay time for token buckets
    TTokenBucket ProvisionedBucket;
    TTokenBucket OnDemandBucket;
    TDeque<TAutoPtr<TEvPrivate::TEvConfigure>> ConfigQueue;

    // Billing (actually make bill records and send'em to metering actor)
    IBillSink::TPtr BillSink;
    TString QuoterPath;
    TString ResourcePath;
    TBillingMetric Provisioned;
    TBillingMetric OnDemand;
    TBillingMetric Overshoot;

    // Monitoring
    TRateAccountingCounters Counters;
public:
    explicit TAccountingActor(const IBillSink::TPtr& billSink, const NKikimrKesus::TStreamingQuoterResource& props, const TString& quoterPath)
        : TActor(&TThis::StateWork)
        , BillSink(billSink)
        , QuoterPath(quoterPath)
        , ResourcePath(props.GetResourcePath())
    {
        Configure(props);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KESUS_ACCOUNTING_ACTOR;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvConfigure, Handle);
            hFunc(TEvPrivate::TEvCounters, Handle);
            HFunc(TEvPrivate::TEvRunAccounting, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    void Handle(const TEvPrivate::TEvConfigure::TPtr& ev) {
        // Delay config apply since we work with history here
        ConfigQueue.emplace_back(ev.Get()->Release());
    }

    void Handle(const TEvPrivate::TEvCounters::TPtr& ev) {
        auto* e = ev->Get();
        Counters = e->Counters;
        Provisioned.SetResourceCounters(Counters.Provisioned);
        OnDemand.SetResourceCounters(Counters.OnDemand);
        Overshoot.SetResourceCounters(Counters.Overshoot);
    }

    void ProcessConfigQueue() {
        while (!ConfigQueue.empty() && ConfigQueue.front()->ApplyAt <= Time) {
            Configure(ConfigQueue.front()->Props);
            ConfigQueue.pop_front();
        }
    }

    void Configure(const NKikimrKesus::TStreamingQuoterResource& props) {
        const auto& accCfg = props.GetAccountingConfig();
        const auto& resCfg = props.GetHierarchicalDRRResourceConfig();
        ProvisionedBucket.SetRate(accCfg.GetProvisionedUnitsPerSecond());
        ProvisionedBucket.SetCapacity(accCfg.GetProvisionedUnitsPerSecond() * accCfg.GetProvisionedCoefficient());
        OnDemandBucket.SetRate(resCfg.GetMaxUnitsPerSecond());
        OnDemandBucket.SetCapacity(accCfg.GetOvershootCoefficient() * resCfg.GetMaxUnitsPerSecond() * resCfg.GetPrefetchCoefficient());
        Provisioned.Configure(accCfg.GetProvisioned(), QuoterPath, props.GetResourcePath(), "provisioned", BillSink);
        OnDemand.Configure(accCfg.GetOnDemand(), QuoterPath, props.GetResourcePath(), "ondemand", BillSink);
        Overshoot.Configure(accCfg.GetOvershoot(), QuoterPath, props.GetResourcePath(), "overshoot", BillSink);
        LWPROBE(ResourceAccountConfigure,
            QuoterPath,
            ResourcePath,
            ProvisionedBucket.GetRate(), ProvisionedBucket.GetCapacity(), ProvisionedBucket.Available(), ProvisionedBucket.GetLastFill(),
            OnDemandBucket.GetRate(), OnDemandBucket.GetCapacity(), OnDemandBucket.Available(), OnDemandBucket.GetLastFill());

    }

    void Handle(const TEvPrivate::TEvRunAccounting::TPtr& ev, const NActors::TActorContext& ctx) {
        auto* e = ev->Get();

        // Aggregate consumption over history
        for (TInstant t = e->History.Begin(); t != e->History.End(); t = e->History.Next(t)) {
            AddValue(t, e->History.Get(t), ctx);
        }

        // Propagate time to the last accounted instant
        // NOTE: required to report zero consumption not saved in history
        // NOTE: may coincide with the last point and therefore ignored
        AddValue(e->Time, 0, ctx);
    }

    void AddValue(TInstant t, double consumed, const NActors::TActorContext& ctx) {
        if (Y_UNLIKELY(t <= Time)) {
            return; // ignore data from past
        }
        Time = t;
        ProcessConfigQueue();

        // Classify consumption into 3 categories
        double provisionedAndOnDemand = OnDemandBucket.FillAndTryTake(t, consumed);
        double provisioned = ProvisionedBucket.FillAndTryTake(t, consumed);
        double onDemand = Max(0.0, provisionedAndOnDemand - provisioned);
        double overshoot = Max(0.0, consumed - provisioned - onDemand);

        LWPROBE(ResourceAccount,
            QuoterPath,
            ResourcePath,
            t, provisionedAndOnDemand, provisioned, onDemand, overshoot,
            ProvisionedBucket.GetRate(), ProvisionedBucket.GetCapacity(), ProvisionedBucket.Available(),
            OnDemandBucket.GetRate(), OnDemandBucket.GetCapacity(), OnDemandBucket.Available());

        Provisioned.Add(provisioned, t, ctx);
        OnDemand.Add(onDemand, t, ctx);
        Overshoot.Add(overshoot, t, ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRateAccounting::TRateAccounting(NActors::TActorId kesus, const IBillSink::TPtr& billSink, const NKikimrKesus::TStreamingQuoterResource& props, const TString& quoterPath)
    : Props(props)
    , QuoterPath(quoterPath)
    , History(HistorySize())
    , Kesus(kesus)
{
    AccountingActor = TActivationContext::Register(new TAccountingActor(billSink, Props, QuoterPath), Kesus);
    ConfigureImpl();
}

void TRateAccounting::Stop() {
    TActivationContext::Send(new IEventHandle(AccountingActor, Kesus,
        new TEvents::TEvPoisonPill()));
}

bool TRateAccounting::ValidateProps(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage) {
    // NOTE: ValidateProps() is called with NOT effective props, so some fields can be unset, see CalcParameters()
    const auto& cfg = props.GetAccountingConfig();
    if (cfg.GetCollectPeriodSec() > 3600) {
        errorMessage = "CollectPeriodSec must be less than an hour.";
        return false;
    }
    if (!std::isfinite(cfg.GetProvisionedUnitsPerSecond())) {
        errorMessage = "ProvisionedUnitsPerSecond must be finite.";
        return false;
    }
    if (!std::isfinite(cfg.GetProvisionedCoefficient())) {
        errorMessage = "ProvisionedCoefficient must be finite.";
        return false;
    }
    if (!std::isfinite(cfg.GetOvershootCoefficient())) {
        errorMessage = "OvershootCoefficient must be finite.";
        return false;
    }
    if (cfg.GetProvisionedUnitsPerSecond() < 0.0) {
        errorMessage = "ProvisionedUnitsPerSecond must be greater or equal to 0.";
        return false;
    }
    if (cfg.GetProvisionedCoefficient() < 0.0) {
        errorMessage = "ProvisionedCoefficient must be greater or equal to 0.";
        return false;
    }
    if (cfg.GetOvershootCoefficient() != 0.0 && cfg.GetOvershootCoefficient() < 1.0) {
        errorMessage = "OvershootCoefficient must be greater or equal to 1 or not set (zero).";
        return false;
    }
    return true;
}

void TRateAccounting::Configure(const NKikimrKesus::TStreamingQuoterResource& props) {
    // NOTE: Configure is called with effective props
    Props = props;
    ConfigureImpl();
    TActivationContext::Send(new NActors::IEventHandle(AccountingActor, Kesus,
        new TEvPrivate::TEvConfigure(TActivationContext::Now(), props)));
}

void TRateAccounting::ConfigureImpl() {
    size_t newHistorySize = HistorySize();
    if (History.Size() != newHistorySize) {
        TConsumptionHistory newHistory(newHistorySize);
        newHistory.Add(History, Accounted); // Note that data loss is possible on size decrease
        History = std::move(newHistory);
    }
    const auto& accCfg = Props.GetAccountingConfig();
    MaxBillingPeriod = TDuration::Seconds(Max(
        accCfg.GetProvisioned().GetBillingPeriodSec(),
        accCfg.GetOnDemand().GetBillingPeriodSec(),
        accCfg.GetOvershoot().GetBillingPeriodSec()));
}

TInstant TRateAccounting::Report(
    const NActors::TActorId& clientId,
    ui64 resourceId,
    TInstant start,
    TDuration interval,
    const double* values,
    size_t size)
{
    // Deduplicate
    TDedupId dedupId(clientId, resourceId);
    auto [iter, isNew] = ClientToReported.emplace(dedupId, TInstant::Zero());
    TInstant& reported = iter->second;
    if (start < reported) {
        ui64 skip = ((reported - start).MicroSeconds() + interval.MicroSeconds() - 1) / interval.MicroSeconds();
        LWPROBE(ResourceReportDedup, QuoterPath, Props.GetResourcePath(), skip, size, start);
        if (skip >= size) { // all values are deduplicated
            LWPROBE(ResourceReportDedupFull, QuoterPath, Props.GetResourcePath(), skip, size, start);
            return reported;
        }
        values += skip;
        size -= skip;
        start += interval * skip;
        Y_ABORT_UNLESS(start >= reported, "rate accounting report deduplication error");
    }

    // Accept values and add them into history
    // Note that data from distant future can be left unaccepted
    size_t added = History.Add(interval, start, values, size, Accounted);
    LWPROBE(ResourceReportAdd, QuoterPath, Props.GetResourcePath(), added, size, start, Accounted);

    // Propagate deduplication timestamp
    if (!isNew) {
        SortedClients.erase(std::make_pair(reported, dedupId));
    }
    reported = start + added * interval;
    SortedClients.emplace(reported, dedupId);
    TInstant result = reported;

    // Clean is delayed till this moment to avoid `iter` invalidation
    RemoveOldClients();
    return result;
}

void TRateAccounting::RemoveOldClients() {
    while (!SortedClients.empty() && SortedClients.begin()->first <= Accounted) {
        ClientToReported.erase(SortedClients.begin()->second);
        SortedClients.erase(SortedClients.begin());
        Y_DEBUG_ABORT_UNLESS(SortedClients.size() == ClientToReported.size());
    }
}

bool TRateAccounting::RunAccounting() {
    bool accountingRequired = RunAccountingNoClean();
    RemoveOldClients();
    bool cleaningRequried = !SortedClients.empty();
    return accountingRequired || cleaningRequried;
}

void TRateAccounting::SetResourceCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& resourceCounters) {
    Counters.SetResourceCounters(resourceCounters);
    TActivationContext::Send(new NActors::IEventHandle(AccountingActor, Kesus,
        new TEvPrivate::TEvCounters(Counters)));
}

bool TRateAccounting::RunAccountingNoClean() {
    // Check if we have enough values for at least one account period
    TInstant accountTill = History.Align(TActivationContext::Now() - CollectPeriod());
    if (accountTill - Accounted < AccountPeriod()) {
        LWPROBE(ResourceAccountingTooEarly, QuoterPath, Props.GetResourcePath(), accountTill, Accounted, AccountPeriod());
        return true;
    }

    // Check if we have data to send
    if (History.End() + MaxBillingPeriod <= Accounted) {
        LWPROBE(ResourceAccountingNoData, QuoterPath, Props.GetResourcePath(), History.End(), Accounted, MaxBillingPeriod);
        return false;
    }

    // Offload hard work into accounting actor
    TActivationContext::Send(new NActors::IEventHandle(AccountingActor, Kesus,
        new TEvPrivate::TEvRunAccounting(
            accountTill - History.Interval(),
            TConsumptionHistory(History, Accounted, accountTill))));
    LWPROBE(ResourceAccountingSend, QuoterPath, Props.GetResourcePath(), accountTill, Accounted);
    Accounted = accountTill;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TMeteringSink : public IBillSink {
public:
    void Send(const NActors::TActorContext& ctx, const TBillRecord& billRecord) override {
        ctx.Send(NMetering::MakeMeteringServiceID(),
            new NMetering::TEvMetering::TEvWriteMeteringJson(billRecord.ToString()));
    }
};

IBillSink::TPtr MakeMeteringSink() {
    return IBillSink::TPtr(new TMeteringSink());
}

}   // namespace NKesus
}   // namespace NKikimr
