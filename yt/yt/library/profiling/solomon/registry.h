#pragma once

#include "public.h"
#include "sensor_set.h"
#include "producer.h"
#include "tag_registry.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>

#include <yt/yt/library/profiling/solomon/sensor_dump.pb.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TSensorInfo
{
    TString Name;
    int ObjectCount;
    int CubeSize;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

class TSolomonRegistry
    : public IRegistryImpl
{
public:
    explicit TSolomonRegistry();

    ICounterImplPtr RegisterCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeCounterImplPtr RegisterTimeCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IGaugeImplPtr RegisterGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeGaugeImplPtr RegisterTimeGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ISummaryImplPtr RegisterSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IGaugeImplPtr RegisterGaugeSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeGaugeImplPtr RegisterTimeGaugeSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimerImplPtr RegisterTimerSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimerImplPtr RegisterTimeHistogram(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IHistogramImplPtr RegisterGaugeHistogram(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IHistogramImplPtr RegisterRateHistogram(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    void RegisterFuncCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) override;

    void RegisterFuncGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) override;

    void RegisterProducer(
        const TString& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) override;

    void RenameDynamicTag(
        const TDynamicTagPtr& tag,
        const TString& name,
        const TString& value) override;

    static TSolomonRegistryPtr Get();

    void Disable();
    void SetDynamicTags(std::vector<TTag> dynamicTags);
    std::vector<TTag> GetDynamicTags();

    void SetGridFactor(std::function<int(const TString&)> gridFactor);
    void SetWindowSize(int windowSize);
    void SetProducerCollectionBatchSize(int batchSize);
    void ProcessRegistrations();
    void Collect(IInvokerPtr offloadInvoker = GetSyncInvoker());
    void ReadSensors(
        const TReadOptions& options,
        ::NMonitoring::IMetricConsumer* consumer) const;

    void ReadRecentSensorValues(
        const TString& name,
        const TTagList& tags,
        const TReadOptions& options,
        NYTree::TFluentAny fluent) const;

    std::vector<TSensorInfo> ListSensors() const;

    const TTagRegistry& GetTags() const;

    i64 GetNextIteration() const;
    int GetWindowSize() const;
    int IndexOf(i64 iteration) const;

    void Profile(const TProfiler& profiler);
    const TProfiler& GetSelfProfiler() const;

    NProto::TSensorDump DumpSensors();
    NProto::TSensorDump DumpSensors(std::vector<TTagId> extraTags);
    NProto::TSensorDump DumpSensors(const std::optional<TString>& host, const THashMap<TString, TString>& instanceTags);

private:
    i64 Iteration_ = 0;
    std::optional<int> WindowSize_;
    std::function<int(const TString&)> GridFactor_;
    TProfiler SelfProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DynamicTagsLock_);
    std::vector<TTag> DynamicTags_;

    std::atomic<bool> Disabled_ = false;
    TMpscStack<std::function<void()>> RegistrationQueue_;

    template <class TFn>
    void DoRegister(TFn fn);

    TTagRegistry Tags_;
    TProducerSet Producers_;

    THashMap<TString, TSensorSet> Sensors_;

    TSensorSet* FindSet(const TString& name, const TSensorOptions& options);

    TCounter RegistrationCount_;
    TEventTimer SensorCollectDuration_, ReadDuration_;
    TGauge SensorCount_, ProjectionCount_, TagCount_;

    friend class TRemoteRegistry;
};

DEFINE_REFCOUNTED_TYPE(TSolomonRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
