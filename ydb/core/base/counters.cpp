#include "counters.h"

#include "monitoring_provider.h"

LWTRACE_DEFINE_PROVIDER(MONITORING_PROVIDER)

namespace NKikimr {

static const THashSet<TString> DATABASE_SERVICES
    = {{ TString("compile"),
         TString("coordinator"),
         TString("dsproxy"),
         TString("dsproxy_mon"),
         TString("dsproxynode"),
         TString("dsproxy_overview"),
         TString("dsproxy_percentile"),
         TString("dsproxy_queue"),
         TString("grpc"),
         TString("interconnect"),
         TString("kqp"),
         TString("processing"),
         TString("proxy"),
         TString("followers"),
         TString("sqs"),
         TString("storage_pool_stat"),
         TString("tablets"),
         TString("utils"),
         TString("auth"),
         TString("ydb"),
         TString("pqproxy|writeInfo"),
         TString("pqproxy|writeTimeLag"),
         TString("pqproxy|writeSession"),
         TString("pqproxy|writingTime"),
         TString("pqproxy|SLI"),
         TString("pqproxy|cache"),
         TString("pqproxy|partitionWriteQuotaWait"),
         TString("pqproxy|topicWriteQuotaWait"),
         TString("pqproxy|readTimeLag"),
         TString("pqproxy|readingTime"),
         TString("pqproxy|readSession"),
         TString("pqproxy|schemecache"),
         TString("pqproxy|mirrorWriteTimeLag"),
         TString("datastreams"),
    }};


static const THashSet<TString> DATABASE_ATTRIBUTE_SERVICES
    = {{ TString("ydb"), TString("datastreams") }};

static const THashSet<TString> DATABASE_ATTRIBUTE_LABELS
    = {{ TString("cloud_id"),
         TString("folder_id"),
         TString("database_id")
    }};

using ::NMonitoring::TDynamicCounters;

const THashSet<TString> &GetDatabaseSensorServices()
{
    return DATABASE_SERVICES;
}

void ReplaceSubgroup(TIntrusivePtr<TDynamicCounters> root, const TString &service)
{
    auto serviceGroup = GetServiceCounters(root, service);
    const auto &[svc, subSvc] = ExtractSubServiceName(service);
    auto rt = GetServiceCountersRoot(root, service);
    rt->ReplaceSubgroup(subSvc.empty() ? "counters" : "subsystem", subSvc.empty() ? svc : subSvc, serviceGroup);
}

const THashSet<TString> &GetDatabaseAttributeSensorServices()
{
    return DATABASE_ATTRIBUTE_SERVICES;
}

const THashSet<TString> &GetDatabaseAttributeLabels()
{
    return DATABASE_ATTRIBUTE_LABELS;
}

static TIntrusivePtr<TDynamicCounters> SkipLabels(TIntrusivePtr<TDynamicCounters> counters,
                                                  const THashSet<TString> &labels)
{
    TString name, value;
    do {
        name = "";
        counters->EnumerateSubgroups([&name, &value, &labels](const TString &n, const TString &v) {
                if (labels.contains(n)) {
                    name = n;
                    value = v;
                }
            });
        if (name)
            counters = counters->GetSubgroup(name, value);
    } while (name);

    return counters;
}

void OnCounterLookup(const char *methodName, const TString &name, const TString &value) {
    GLOBAL_LWPROBE(MONITORING_PROVIDER, MonitoringCounterLookup, methodName, name, value);
}


std::pair<TString, TString> ExtractSubServiceName(const TString &service)
{
    TStringBuf svc = TStringBuf(service);
    TStringBuf subSvc = svc.SplitOff('|');
    return {TString(svc), TString(subSvc)};
}

TIntrusivePtr<TDynamicCounters> GetServiceCountersRoot(TIntrusivePtr<TDynamicCounters> root,
                                                   const TString &service)
{
    auto pair = ExtractSubServiceName(service);
    if (pair.second.empty())
        return root;
    return root->GetSubgroup("counters", pair.first);
}

static THashSet<TString> MakeServiceCountersExtraLabels() {
    THashSet<TString> extraLabels;
    extraLabels.insert(DATABASE_LABEL);
    extraLabels.insert(SLOT_LABEL);
    extraLabels.insert(HOST_LABEL);
    extraLabels.insert(DATABASE_ATTRIBUTE_LABELS.begin(),
                       DATABASE_ATTRIBUTE_LABELS.end());
    return extraLabels;
}

static const THashSet<TString> SERVICE_COUNTERS_EXTRA_LABELS = MakeServiceCountersExtraLabels();

TIntrusivePtr<TDynamicCounters> GetServiceCounters(TIntrusivePtr<TDynamicCounters> root,
                                                   const TString &service, bool skipAddedLabels)
{
    const auto &[svc, subSvc]  = ExtractSubServiceName(service);
    auto res = root->GetSubgroup("counters", svc);
    if (!subSvc.empty()) {
        res = res->GetSubgroup("subsystem", subSvc);
    }
    if (!skipAddedLabels)
        return res;

    res = SkipLabels(res, SERVICE_COUNTERS_EXTRA_LABELS);

    auto utils = root->GetSubgroup("counters", "utils");
    utils = SkipLabels(utils, SERVICE_COUNTERS_EXTRA_LABELS);
    auto lookupCounter = utils->GetSubgroup("component", service)->GetCounter("CounterLookups", true);
    res->SetLookupCounter(lookupCounter);
    res->SetOnLookup(OnCounterLookup);

    return res;
}


} // namespace NKikimr
