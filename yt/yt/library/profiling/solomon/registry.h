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
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeCounterPtr RegisterTimeCounter(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IGaugePtr RegisterGauge(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeGaugePtr RegisterTimeGauge(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ISummaryPtr RegisterSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IGaugePtr RegisterGaugeSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimeGaugePtr RegisterTimeGaugeSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimerPtr RegisterTimerSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    ITimerPtr RegisterTimeHistogram(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IHistogramPtr RegisterGaugeHistogram(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    IHistogramPtr RegisterRateHistogram(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) override;

    void RegisterFuncCounter(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) override;

    void RegisterFuncGauge(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) override;

    void RegisterProducer(
        TStringBuf prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) override;

    void RenameDynamicTag(
        const TDynamicTagPtr& tag,
        TStringBuf name,
        TStringBuf value) override;

    static TSolomonRegistryPtr Get();

    void Disable();

    void SetDynamicTags(std::vector<TTag> dynamicTags);
    std::vector<TTag> GetDynamicTags() const;

    void SetGridFactor(std::function<int(TStringBuf)> gridFactor);
    void SetWindowSize(int windowSize);
    void SetProducerCollectionBatchSize(int batchSize);
    void SetLabelSanitizationPolicy(ELabelSanitizationPolicy LabelSanitizationPolicy);
    void ProcessRegistrations();
    void Collect(IInvokerPtr offloadInvoker = GetSyncInvoker());
    void ReadSensors(
        const TReadOptions& options,
        ::NMonitoring::IMetricConsumer* consumer) const;

    void ReadRecentSensorValues(
        TStringBuf name,
        const TTagList& tags,
        const TReadOptions& options,
        NYTree::TFluentAny fluent) const;

    std::vector<TSensorInfo> ListSensors() const;

    const TTagRegistry& GetTagRegistry() const;

    i64 GetNextIteration() const;
    int GetWindowSize() const;
    int IndexOf(i64 iteration) const;

    void Profile(const TWeakProfiler& profiler);
    const TWeakProfiler& GetSelfProfiler() const;

    NProto::TSensorDump DumpSensors();
    NProto::TSensorDump DumpSensors(TTagSet customTagSet);

private:
    i64 Iteration_ = 0;
    std::optional<int> WindowSize_;
    std::function<int(TStringBuf)> GridFactor_;
    TWeakProfiler SelfProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DynamicTagsLock_);
    std::vector<TTag> DynamicTags_;

    std::atomic<bool> Disabled_ = false;
    TMpscStack<std::function<void()>> RegistrationQueue_;

    template <class TFn>
    void DoRegister(TFn fn);

    TTagRegistry TagRegistry_;
    TProducerSet Producers_;

    THashMap<std::string, TSensorSet> Sensors_;

    TSensorSet* FindSet(TStringBuf name, const TSensorOptions& options);

    TCounter RegistrationCount_;
    TEventTimer SensorCollectDuration_;
    TEventTimer ReadDuration_;
    TGauge SensorCount_;
    TGauge ProjectionCount_;
    TGauge TagCount_;

    friend class TRemoteRegistry;

    TTagIdSet EncodeTagSet(const TTagSet& tagSet);
};

DEFINE_REFCOUNTED_TYPE(TSolomonRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
