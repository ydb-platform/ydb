#include "processor_impl.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tablet/labeled_db_counters.h>
#include <ydb/core/tablet/labeled_counters_merger.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

namespace NKikimr {
namespace NSysView {

template <bool IsMax>
struct TAggregateCumulative {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        auto cumulativeSize = src.GetCumulativeCount();
        auto histogramSize = src.HistogramSize();

        if (dst->CumulativeSize() < cumulativeSize) {
            dst->MutableCumulative()->Resize(cumulativeSize, 0);
        }
        if (dst->HistogramSize() < histogramSize) {
            auto missing = histogramSize - dst->HistogramSize();
            for (; missing > 0; --missing) {
                dst->AddHistogram();
            }
        }

        const auto& from = src.GetCumulative();
        auto* to = dst->MutableCumulative();
        auto doubleDiffSize = from.size() / 2 * 2;
        for (int i = 0; i < doubleDiffSize; ) {
            auto index = from[i++];
            auto value = from[i++];
            if (index >= cumulativeSize) {
                continue;
            }
            if constexpr (!IsMax) {
                (*to)[index] += value;
            } else {
                (*to)[index] = std::max(value, (*to)[index]);
            }
        }
        for (size_t i = 0; i < histogramSize; ++i) {
            const auto& histogram = src.GetHistogram(i);
            const auto& from = histogram.GetBuckets();
            auto* to = dst->MutableHistogram(i)->MutableBuckets();
            auto bucketCount = histogram.GetBucketsCount();
            if (to->size() < (int)bucketCount) {
                to->Resize(bucketCount, 0);
            }
            auto doubleDiffSize = from.size();
            for (int b = 0; b < doubleDiffSize; ) {
                auto index = from[b++];
                auto value = from[b++];
                if (index >= bucketCount) {
                    continue;
                }
                if constexpr (!IsMax) {
                    (*to)[index] += value;
                } else {
                    (*to)[index] = std::max(value, (*to)[index]);
                }
            }
        }
    }
};

template <bool IsMax>
struct TAggregateSimple {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        auto simpleSize = src.SimpleSize();
        if (dst->SimpleSize() < simpleSize) {
            dst->MutableSimple()->Resize(simpleSize, 0);
        }
        const auto& from = src.GetSimple();
        auto* to = dst->MutableSimple();
        for (size_t i = 0; i < simpleSize; ++i) {
            if constexpr (!IsMax) {
                (*to)[i] += from[i];
            } else {
                (*to)[i] = std::max(from[i], (*to)[i]);
            }
        }
    }
};

struct TAggregateIncrementalSum {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        TAggregateCumulative<false>::Apply(dst, src);
    }
};

struct TAggregateIncrementalMax {
    static void Apply(NKikimrSysView::TDbCounters* /*dst*/, const NKikimrSysView::TDbCounters& /*src*/) {
    }
};

struct TAggregateStatefulSum {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        TAggregateSimple<false>::Apply(dst, src);
    }
};

struct TAggregateStatefulMax {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        TAggregateSimple<true>::Apply(dst, src);
        TAggregateCumulative<true>::Apply(dst, src);
    }
};

static void SwapSimpleCounters(NKikimrSysView::TDbCounters* dst, NKikimrSysView::TDbCounters& src) {
    dst->MutableSimple()->Swap(src.MutableSimple());
};

static void SwapMaxCounters(NKikimrSysView::TDbCounters* dst, NKikimrSysView::TDbCounters& src) {
    SwapSimpleCounters(dst, src);
    dst->MutableCumulative()->Swap(src.MutableCumulative());
    dst->SetCumulativeCount(src.GetCumulativeCount());
};

static void SwapLabeledCounters(NKikimrLabeledCounters::TTabletLabeledCounters* dst, NKikimrLabeledCounters::TTabletLabeledCounters& src) {
    dst->MutableLabeledCounter()->Swap(src.MutableLabeledCounter());
};

static void ResetSimpleCounters(NKikimrSysView::TDbCounters* dst) {
    auto simpleSize = dst->SimpleSize();
    auto* to = dst->MutableSimple();
    for (size_t i = 0; i < simpleSize; ++i) {
        (*to)[i] = 0;
    }
}

static void ResetMaxCounters(NKikimrSysView::TDbCounters* dst) {
    ResetSimpleCounters(dst);
    auto cumulativeSize = dst->CumulativeSize();
    auto* to = dst->MutableCumulative();
    for (size_t i = 0; i < cumulativeSize; ++i) {
        (*to)[i] = 0;
    }
}

static void ResetLabeledCounters(NKikimrLabeledCounters::TTabletLabeledCounters* dst) {
    auto labeledSize = dst->LabeledCounterSize();
    auto* to = dst->MutableLabeledCounter();
    for (size_t i = 0; i < labeledSize; ++i) {
        auto& counter = (*to)[i];
        TLabeledCounterOptions::ECounterType type(counter.GetType());
        TLabeledCounterOptions::EAggregateFunc aggrFunc(counter.GetAggregateFunc());
        const bool switchResetValue = (type == TLabeledCounterOptions::CT_TIMELAG);
        switch (aggrFunc) {
            case TLabeledCounterOptions::EAF_MIN:
                if (switchResetValue) {
                    counter.SetValue(0);
                } else {
                    counter.SetValue(std::numeric_limits<ui64>::max());
                }
                break;
            case TLabeledCounterOptions::EAF_MAX:
                if (switchResetValue) {
                    counter.SetValue(std::numeric_limits<ui64>::max());
                } else {
                    counter.SetValue(0);
                }
                break;
            case TLabeledCounterOptions::EAF_SUM:
                counter.SetValue(0);
                break;
            default:
                Y_ABORT("bad aggrFunc value");
        }
    }
}

template <typename TAggrSum, typename TAggrMax>
static void AggregateCounters(NKikimr::NSysView::TDbServiceCounters* dst,
    const NKikimrSysView::TDbServiceCounters& src)
{
    if (src.HasMain()) {
        TAggrSum::Apply(dst->Proto().MutableMain(), src.GetMain());
    }

    for (const auto& srcTablet : src.GetTabletCounters()) {
        auto* dstTablet = dst->FindOrAddTabletCounters(srcTablet.GetType());
        TAggrSum::Apply(dstTablet->MutableExecutorCounters(), srcTablet.GetExecutorCounters());
        TAggrSum::Apply(dstTablet->MutableAppCounters(), srcTablet.GetAppCounters());
        TAggrMax::Apply(dstTablet->MutableMaxExecutorCounters(), srcTablet.GetMaxExecutorCounters());
        TAggrMax::Apply(dstTablet->MutableMaxAppCounters(), srcTablet.GetMaxAppCounters());
    }

    for (const auto& srcReq : src.GetGRpcCounters()) {
        auto* dstReq = dst->FindOrAddGRpcCounters(srcReq.GetGRpcService(), srcReq.GetGRpcRequest());
        TAggrSum::Apply(dstReq->MutableRequestCounters(), srcReq.GetRequestCounters());
    }

    if (src.HasGRpcProxyCounters()) {
        TAggrSum::Apply(dst->Proto().MutableGRpcProxyCounters()->MutableRequestCounters(),
            src.GetGRpcProxyCounters().GetRequestCounters());
    }

    for (const auto& srcReq : src.GetLabeledCounters()) {
        auto* dstReq = dst->FindOrAddLabeledCounters(srcReq.GetAggregatedPerTablets().GetGroup());
        if (dstReq->GetAggregatedPerTablets().GetLabeledCounter().size() <
            srcReq.GetAggregatedPerTablets().GetLabeledCounter().size()) {
            const ui32 n = srcReq.GetAggregatedPerTablets().GetLabeledCounter().size() -
                    dstReq->GetAggregatedPerTablets().GetLabeledCounter().size();
            for (ui32 i = 0; i < n; ++i) {
                dstReq->MutableAggregatedPerTablets()->AddLabeledCounter();
            }
        }

        for (int i = 0; i < srcReq.GetAggregatedPerTablets().GetLabeledCounter().size(); ++i) {
            const auto& srcCounter = srcReq.GetAggregatedPerTablets().GetLabeledCounter(i);
            auto* trgCounter = dstReq->MutableAggregatedPerTablets()->MutableLabeledCounter(i);
            NKikimr::TMerger::MergeOne(srcCounter, *trgCounter);
        }
    }
}

static void AggregateIncrementalCounters(NKikimr::NSysView::TDbServiceCounters* dst,
    const NKikimrSysView::TDbServiceCounters& src)
{
    AggregateCounters<TAggregateIncrementalSum, TAggregateIncrementalMax>(dst, src);
}

static void AggregateStatefulCounters(NKikimr::NSysView::TDbServiceCounters* dst,
    const NKikimrSysView::TDbServiceCounters& src)
{
    AggregateCounters<TAggregateStatefulSum, TAggregateStatefulMax>(dst, src);
}

static void SwapStatefulCounters(NKikimr::NSysView::TDbServiceCounters* dst,
    NKikimrSysView::TDbServiceCounters& src)
{
    if (src.HasMain()) {
        SwapSimpleCounters(dst->Proto().MutableMain(), *src.MutableMain());
    }

    for (auto& srcTablet : *src.MutableTabletCounters()) {
        auto* dstTablet = dst->FindOrAddTabletCounters(srcTablet.GetType());
        SwapSimpleCounters(dstTablet->MutableExecutorCounters(), *srcTablet.MutableExecutorCounters());
        SwapSimpleCounters(dstTablet->MutableAppCounters(), *srcTablet.MutableAppCounters());
        SwapMaxCounters(dstTablet->MutableMaxExecutorCounters(), *srcTablet.MutableMaxExecutorCounters());
        SwapMaxCounters(dstTablet->MutableMaxAppCounters(), *srcTablet.MutableMaxAppCounters());
    }

    for (auto& srcReq : *src.MutableGRpcCounters()) {
        auto* dstReq = dst->FindOrAddGRpcCounters(srcReq.GetGRpcService(), srcReq.GetGRpcRequest());
        SwapSimpleCounters(dstReq->MutableRequestCounters(), *srcReq.MutableRequestCounters());
    }

    for (auto& srcReq : *src.MutableLabeledCounters()) {
        auto* dstReq = dst->FindOrAddLabeledCounters(srcReq.GetAggregatedPerTablets().GetGroup());
        SwapLabeledCounters(dstReq->MutableAggregatedPerTablets(), *srcReq.MutableAggregatedPerTablets());
    }
}

static void ResetStatefulCounters(NKikimrSysView::TDbServiceCounters* dst) {
    if (dst->HasMain()) {
        ResetSimpleCounters(dst->MutableMain());
    }
    for (auto& dstTablet : *dst->MutableTabletCounters()) {
        ResetSimpleCounters(dstTablet.MutableExecutorCounters());
        ResetSimpleCounters(dstTablet.MutableAppCounters());
        ResetMaxCounters(dstTablet.MutableMaxExecutorCounters());
        ResetMaxCounters(dstTablet.MutableMaxAppCounters());
    }
    for (auto& dstReq : *dst->MutableGRpcCounters()) {
        ResetSimpleCounters(dstReq.MutableRequestCounters());
    }
    for (auto& dstReq : *dst->MutableLabeledCounters()) {
        ResetLabeledCounters(dstReq.MutableAggregatedPerTablets());
    }
}

void TSysViewProcessor::SendNavigate() {
    if (!Database) {
        ScheduleSendNavigate();
        return;
    }

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    auto request = MakeHolder<TNavigate>();
    request->ResultSet.push_back({});

    auto& entry = request->ResultSet.back();
    entry.Path = SplitPath(Database);
    entry.Operation = TNavigate::EOp::OpPath;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.RedirectRequired = false;

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

TIntrusivePtr<IDbCounters> TSysViewProcessor::CreateCountersForService(
    NKikimrSysView::EDbCountersService service)
{
    TIntrusivePtr<IDbCounters> result;
    switch (service) {
    case NKikimrSysView::KQP: {
        auto group = InternalGroups["kqp_serverless"];
        Y_ABORT_UNLESS(group);
        result = MakeIntrusive<NKqp::TKqpDbCounters>(ExternalGroup, group);
        break;
    }
    case NKikimrSysView::TABLETS: {
        auto group = InternalGroups["tablets_serverless"];
        Y_ABORT_UNLESS(group);
        THolder<TTabletCountersBase> executorCounters(new NTabletFlatExecutor::TExecutorCounters);
        result = CreateTabletDbCounters(ExternalGroup, group, std::move(executorCounters));
        break;
    }
    case NKikimrSysView::GRPC: {
        auto group = InternalGroups["grpc_serverless"];
        Y_ABORT_UNLESS(group);
        result = NGRpcService::CreateGRpcDbCounters(ExternalGroup, group);
        break;
    }
    case NKikimrSysView::GRPC_PROXY: {
        auto group = InternalGroups["grpc_serverless"];
        Y_ABORT_UNLESS(group);
        result = NGRpcService::CreateGRpcProxyDbCounters(ExternalGroup, group);
        break;
    }
    case NKikimrSysView::LABELED: {
        result = NKikimr::CreateLabeledDbCounters(LabeledGroup);
        break;
    }
    default:
        break;
    }

    if (result) {
        Counters[service] = result;
    }
    return result;
}

void TSysViewProcessor::AttachExternalCounters() {
    if (!Database) {
        return;
    }

    GetServiceCounters(AppData()->Counters, "ydb_serverless", false)
        ->GetSubgroup("database", Database)
        ->GetSubgroup("cloud_id", CloudId)
        ->GetSubgroup("folder_id", FolderId)
        ->GetSubgroup("database_id", DatabaseId)
        ->RegisterSubgroup("host", "", ExternalGroup);

    GetServiceCounters(AppData()->Counters, "labeled_serverless", false)
        ->GetSubgroup("database", Database)
        ->GetSubgroup("cloud_id", CloudId)
        ->GetSubgroup("folder_id", FolderId)
        ->GetSubgroup("database_id", DatabaseId)
        ->RegisterSubgroup("host", "", LabeledGroup);
}

void TSysViewProcessor::AttachInternalCounters() {
    if (!Database) {
        return;
    }

    for (const auto& [name, group] : InternalGroups) {
        GetServiceCounters(AppData()->Counters, name, false)
            ->GetSubgroup("database", Database)
            ->RegisterSubgroup("host", "", group);
    }
}

void TSysViewProcessor::DetachExternalCounters() {
    if (!Database) {
        return;
    }

    GetServiceCounters(AppData()->Counters, "ydb_serverless", false)
        ->RemoveSubgroup("database", Database);

    GetServiceCounters(AppData()->Counters, "labeled_serverless", false)
        ->RemoveSubgroup("database", Database);
}

void TSysViewProcessor::DetachInternalCounters() {
    if (!Database) {
        return;
    }

    for (const auto& [name, group] : InternalGroups) {
        GetServiceCounters(AppData()->Counters, name, false)
            ->RemoveSubgroup("database", Database);
    }
}

void TSysViewProcessor::Handle(TEvSysView::TEvSendDbCountersRequest::TPtr& ev) {
    if (!AppData()->FeatureFlags.GetEnableDbCounters()) {
        return;
    }

    auto& record = ev->Get()->Record;
    auto nodeId = record.GetNodeId();

    auto& state = NodeCountersStates[nodeId];
    state.FreshCount = 0;

    if (state.Generation == record.GetGeneration()) {
        SVLOG_D("[" << TabletID() << "] TEvSendDbCountersRequest, known generation: "
            << "node id# " << nodeId
            << ", generation# " << record.GetGeneration());

        auto response = MakeHolder<TEvSysView::TEvSendDbCountersResponse>();
        response->Record.SetDatabase(Database);
        response->Record.SetGeneration(state.Generation);
        Send(ev->Sender, std::move(response));
        return;
    }

    state.Generation = record.GetGeneration();

    std::unordered_set<NKikimrSysView::EDbCountersService> incomingServicesSet;

    for (auto& serviceCounters : *record.MutableServiceCounters()) {
        auto service = serviceCounters.GetService();
        incomingServicesSet.insert(service);

        auto& simpleState = state.Simple[service];
        simpleState.Clear();
        SwapStatefulCounters(&simpleState, *serviceCounters.MutableCounters());

        auto& aggrState = AggregatedCountersState[service];
        AggregateIncrementalCounters(&aggrState, serviceCounters.GetCounters());
    }

    for (auto it = state.Simple.begin(); it != state.Simple.end(); ) {
        if (incomingServicesSet.find(it->first) == incomingServicesSet.end()) {
            it = state.Simple.erase(it);
        } else {
            ++it;
        }
    }

    SVLOG_D("[" << TabletID() << "] TEvSendDbCountersRequest: "
        << "node id# " << nodeId
        << ", generation# " << state.Generation
        << ", services count# " << incomingServicesSet.size()
        << ", request size# " << record.ByteSize());

    auto response = MakeHolder<TEvSysView::TEvSendDbCountersResponse>();
    response->Record.SetDatabase(Database);
    response->Record.SetGeneration(state.Generation);
    Send(ev->Sender, std::move(response));
}

void TSysViewProcessor::Handle(TEvSysView::TEvSendDbLabeledCountersRequest::TPtr& ev) {
    if (!AppData()->FeatureFlags.GetEnableDbCounters()) {
        return;
    }

    auto& record = ev->Get()->Record;
    auto nodeId = record.GetNodeId();

    auto& state = NodeLabeledCountersStates[nodeId];
    state.FreshCount = 0;

    if (state.Generation == record.GetGeneration()) {
        SVLOG_D("[" << TabletID() << "] TEvSendDbLabeledCountersRequest, known generation: "
            << "node id# " << nodeId
            << ", generation# " << record.GetGeneration());

        auto response = MakeHolder<TEvSysView::TEvSendDbLabeledCountersResponse>();
        response->Record.SetDatabase(Database);
        response->Record.SetGeneration(state.Generation);
        Send(ev->Sender, std::move(response));
        return;
    }

    state.Generation = record.GetGeneration();

    std::unordered_set<NKikimrSysView::EDbCountersService> incomingServicesSet;

    for (auto& serviceCounters : *record.MutableServiceCounters()) {
        const auto service = serviceCounters.GetService();
        incomingServicesSet.insert(service);

        auto& simpleState = state.Simple[service];
        simpleState.Clear();
        SwapStatefulCounters(&simpleState, *serviceCounters.MutableCounters());
    }

    for (auto it = state.Simple.begin(); it != state.Simple.end(); ) {
        if (incomingServicesSet.find(it->first) == incomingServicesSet.end()) {
            it = state.Simple.erase(it);
        } else {
            ++it;
        }
    }

    SVLOG_D("[" << TabletID() << "] TEvSendDbLabeledCountersRequest: "
        << "node id# " << nodeId
        << ", generation# " << state.Generation
        << ", request size# " << record.ByteSize());

    auto response = MakeHolder<TEvSysView::TEvSendDbLabeledCountersResponse>();
    response->Record.SetDatabase(Database);
    response->Record.SetGeneration(state.Generation);
    Send(ev->Sender, std::move(response));
}

void TSysViewProcessor::Handle(TEvPrivate::TEvApplyCounters::TPtr&) {
    for (auto& [_, counters] : AggregatedCountersState) {
        ResetStatefulCounters(&counters.Proto());
    }

    for (auto it = NodeCountersStates.begin(); it != NodeCountersStates.end(); ) {
        auto& state = it->second;
        if (state.FreshCount > 1) {
            it = NodeCountersStates.erase(it);
            continue;
        }
        ++state.FreshCount;
        for (const auto& [service, counters] : state.Simple) {
            AggregateStatefulCounters(&AggregatedCountersState[service], counters.Proto());
        }
        ++it;
    }
    for (auto& [service, aggrCounters] : AggregatedCountersState) {
        TIntrusivePtr<IDbCounters> counters;
        if (auto it = Counters.find(service); it != Counters.end()) {
            counters = it->second;
        } else {
            counters = CreateCountersForService(service);
        }
        if (!counters) {
            continue;
        }
        counters->FromProto(aggrCounters);
    }

    SVLOG_D("[" << TabletID() << "] TEvApplyCounters: "
        << "services count# " << AggregatedCountersState.size());

    ScheduleApplyCounters();
}

void TSysViewProcessor::Handle(TEvPrivate::TEvApplyLabeledCounters::TPtr&) {
    for (auto& [_, counters] : AggregatedLabeledState) {
        ResetStatefulCounters(&counters.Proto());
    }

    for (auto it = NodeLabeledCountersStates.begin(); it != NodeLabeledCountersStates.end(); ) {
        auto& state = it->second;
        if (state.FreshCount > 1) {
            it = NodeLabeledCountersStates.erase(it);
            continue;
        }
        ++state.FreshCount;
        for (const auto& [service, counters] : state.Simple) {
            AggregateStatefulCounters(&AggregatedLabeledState[service], counters.Proto());
        }
        ++it;
    }

    for (auto& [service, aggrCounters] : AggregatedLabeledState) {
        TIntrusivePtr<IDbCounters> counters;
        if (auto it = Counters.find(service); it != Counters.end()) {
            counters = it->second;
        } else {
            counters = CreateCountersForService(service);
        }
        if (!counters) {
            continue;
        }
        counters->FromProto(aggrCounters);
    }

    SVLOG_D("[" << TabletID() << "] TEvApplyLabeledCounters: "
        << "services count# " << AggregatedLabeledState.size());

    ScheduleApplyLabeledCounters();
}

void TSysViewProcessor::Handle(TEvPrivate::TEvSendNavigate::TPtr&) {
    SendNavigate();
}

void TSysViewProcessor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

    THolder<TNavigate> request(ev->Get()->Request.Release());
    if (request->ResultSet.size() != 1) {
        SVLOG_CRIT("[" << TabletID() << "] TEvNavigateKeySetResult failed: "
            << "result set size# " << request->ResultSet.size());
        ScheduleSendNavigate();
        return;
    }

    auto& entry = request->ResultSet.back();

    if (entry.Status != TNavigate::EStatus::Ok) {
        SVLOG_W("[" << TabletID() << "] TEvNavigateKeySetResult failed: "
            << "status# " << entry.Status);
        ScheduleSendNavigate();
        return;
    }

    for (const auto& [key, value] : entry.Attributes) {
        if (key == "cloud_id") {
            CloudId = value;
        } else if (key == "folder_id") {
            FolderId = value;
        } else if (key == "database_id") {
            DatabaseId = value;
        }
    }

    AttachExternalCounters();
    AttachInternalCounters();

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(entry.TableId.PathId));

    SVLOG_D("[" << TabletID() << "] TEvNavigateKeySetResult: "
        << "database# " << Database
        << ", pathId# " << entry.TableId.PathId
        << ", cloud_id# " << CloudId
        << ", folder_id# " << FolderId
        << ", database_id# " << DatabaseId);
}

void TSysViewProcessor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
    const auto& describeResult  = ev->Get()->Result;

    TString path = describeResult->GetPath();
    if (path != Database) {
        SVLOG_W("[" << TabletID() << "] TEvWatchNotifyUpdated, invalid path: "
            << "database# " << Database
            << ", path# " << path);
        return;
    }

    const auto& pathDescription = describeResult->GetPathDescription();
    const auto& userAttrs = pathDescription.GetUserAttributes();

    TString cloudId, folderId, databaseId;
    for (const auto& attr : userAttrs) {
        if (attr.GetKey() == "cloud_id") {
            cloudId = attr.GetValue();
        } else if (attr.GetKey() == "folder_id") {
            folderId = attr.GetValue();
        } else if (attr.GetKey() == "database_id") {
            databaseId = attr.GetValue();
        }
    }

    SVLOG_D("[" << TabletID() << "] TEvWatchNotifyUpdated: "
        << "database# " << Database
        << ", cloud_id# " << cloudId
        << ", folder_id# " << folderId
        << ", database_id# " << databaseId);

    if (cloudId != CloudId || folderId != FolderId || databaseId != DatabaseId) {
        DetachExternalCounters();
        CloudId = cloudId;
        FolderId = folderId;
        DatabaseId = databaseId;
        AttachExternalCounters();
    }
}

void TSysViewProcessor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
    Y_UNUSED(ev);
}

} // NSysView
} // NKikimr

