#include "counters.h"

#include "monitoring_provider.h"

LWTRACE_DEFINE_PROVIDER(MONITORING_PROVIDER)

namespace NKikimr {

static const THashSet<TString> DATABASE_SERVICES
    = {{
         TString("blob_depot_agent"),
         TString("compile"),
         TString("config"),
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
         TString("utils|comp_broker"),
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
         TString("pqproxy|userAgents"),
         TString("datastreams"),
    }};


static const THashSet<TString> DATABASE_ATTRIBUTE_SERVICES
    = {{ TString("ydb"), TString("datastreams") }};

static const TVector<TString> DATABASE_ATTRIBUTE_LABELS
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

const TVector<TString> &GetDatabaseAttributeLabels()
{
    return DATABASE_ATTRIBUTE_LABELS;
}

static TIntrusivePtr<TDynamicCounters> SkipLabels(TIntrusivePtr<TDynamicCounters> counters,
                                                  const TVector<TString> &labels)
{
    for (const TString& label : labels) {
        auto next = counters->FindSubgroup(label);
        if (next) {
            counters = next;
        }
    }

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

static TVector<TString> MakeServiceCountersExtraLabels() {
    // NOTE: order of labels should match labels maintainer order for efficiency
    TVector<TString> extraLabels;
    extraLabels.push_back(DATABASE_LABEL);
    extraLabels.push_back(SLOT_LABEL);
    extraLabels.push_back(HOST_LABEL);
    extraLabels.insert(extraLabels.end(),
        DATABASE_ATTRIBUTE_LABELS.begin(),
        DATABASE_ATTRIBUTE_LABELS.end());
    return extraLabels;
}

static const TVector<TString> SERVICE_COUNTERS_EXTRA_LABELS = MakeServiceCountersExtraLabels();

static TIntrusivePtr<TDynamicCounters> SkipExtraLabels(TIntrusivePtr<TDynamicCounters> counters) {
    for (;;) {
        // Keep trying as long as there is something to skip
        auto next = SkipLabels(counters, SERVICE_COUNTERS_EXTRA_LABELS);
        if (next == counters) {
            break;
        }
        counters = next;
    }

    return counters;
}

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

    res = SkipExtraLabels(res);

    auto utils = root->GetSubgroup("counters", "utils");
    utils = SkipExtraLabels(utils);
    auto lookupCounter = utils->GetSubgroup("component", service)->GetCounter("CounterLookups", true);
    res->SetLookupCounter(lookupCounter);
    res->SetOnLookup(OnCounterLookup);

    return res;
}


} // namespace NKikimr
