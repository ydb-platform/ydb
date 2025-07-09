#pragma once

#include "public.h"
#include "sensor_set.h"
#include "producer.h"
#include "tag_registry.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>

#include <yt/yt/library/profiling/solomon/sensor_dump.pb.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TSensorInfo
{
    std::string Name;
    int ObjectCount;
    int CubeSize;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

class TSolomonRegistry
    : public IRegistry
{
public:
    explicit TSolomonRegistry();

    ICounterPtr RegisterCounter(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeCounterPtr RegisterTimeCounter(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IGaugePtr RegisterGauge(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeGaugePtr RegisterTimeGauge(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ISummaryPtr RegisterSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IGaugePtr RegisterGaugeSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeGaugePtr RegisterTimeGaugeSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimerPtr RegisterTimerSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimerPtr RegisterTimeHistogram(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IHistogramPtr RegisterGaugeHistogram(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IHistogramPtr RegisterRateHistogram(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    void RegisterFuncCounter(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) override;

    void RegisterFuncGauge(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) override;

    void RegisterProducer(
        const std::string& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) override;

    void RenameDynamicTag(
        const TDynamicTagPtr& tag,
        const std::string& name,
        const std::string& value) override;

    static TSolomonRegistryPtr Get();

    void Disable();
    void SetDynamicTags(std::vector<TTag> dynamicTags);
    std::vector<TTag> GetDynamicTags();

    void SetGridFactor(std::function<int(const std::string&)> gridFactor);
    void SetWindowSize(int windowSize);
    void SetProducerCollectionBatchSize(int batchSize);
    void SetLabelSanitizationPolicy(ELabelSanitizationPolicy LabelSanitizationPolicy);
    void ProcessRegistrations();
    void Collect(IInvokerPtr offloadInvoker = GetSyncInvoker());
    void ReadSensors(
        const TReadOptions& options,
        ::NMonitoring::IMetricConsumer* consumer) const;

    void ReadRecentSensorValues(
        const std::string& name,
        const TTagList& tags,
        const TReadOptions& options,
        NYTree::TFluentAny fluent) const;

    std::vector<TSensorInfo> ListSensors() const;

    const TTagRegistry& GetTags() const;

    i64 GetNextIteration() const;
    int GetWindowSize() const;
    int IndexOf(i64 iteration) const;

    void Profile(const TWeakProfiler& profiler);
    const TWeakProfiler& GetSelfProfiler() const;

    NProto::TSensorDump DumpSensors();
    NProto::TSensorDump DumpSensors(std::vector<TTagId> extraTags);
    NProto::TSensorDump DumpSensors(const std::optional<std::string>& host, const THashMap<std::string, std::string>& instanceTags);

private:
    i64 Iteration_ = 0;
    std::optional<int> WindowSize_;
    std::function<int(const std::string&)> GridFactor_;
    TWeakProfiler SelfProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DynamicTagsLock_);
    std::vector<TTag> DynamicTags_;

    std::atomic<bool> Disabled_ = false;
    TMpscStack<std::function<void()>> RegistrationQueue_;

    template <class TFn>
    void DoRegister(TFn fn);

    TTagRegistry Tags_;
    TProducerSet Producers_;

    THashMap<std::string, TSensorSet> Sensors_;

    TSensorSet* FindSet(const std::string& name, const TSensorOptions& options);

    TCounter RegistrationCount_;
    TEventTimer SensorCollectDuration_;
    TEventTimer ReadDuration_;
    TGauge SensorCount_;
    TGauge ProjectionCount_;
    TGauge TagCount_;

    friend class TRemoteRegistry;
};

DEFINE_REFCOUNTED_TYPE(TSolomonRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
