#pragma once

#include "public.h"
#include "registry.h"
#include "remote.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/monlib/encode/format.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TSolomonExporter
    : public TRefCounted
{
public:
    explicit TSolomonExporter(
        TSolomonExporterConfigPtr config,
        TSolomonRegistryPtr registry = nullptr);

    void Register(const std::string& prefix, const NYT::NHttp::IServerPtr& server);
    void Register(const std::string& prefix, const NYT::NHttp::IRequestPathMatcherPtr& handlers);

    //! Attempts to read registered sensors in JSON format.
    //! Returns null if exporter is not ready.
    std::optional<std::string> ReadJson(const TReadOptions& options = {}, std::optional<std::string> shard = {});

    std::optional<std::string> ReadSpack(const TReadOptions& options = {}, std::optional<std::string> shard = {});

    bool ReadSensors(
        ::NMonitoring::IMetricEncoderPtr encoder,
        const TReadOptions& options,
        std::optional<std::string> shard);

    void AttachRemoteProcess(TCallback<TFuture<TSharedRef>()> dumpSensors);

    TSharedRef DumpSensors();

    // There must be at most 1 running exporter per registry.
    void Start();
    void Stop();

    NYTree::IYPathServicePtr GetSensorService();

    const TSolomonRegistryPtr& GetRegistry() const;

private:
    const TSolomonExporterConfigPtr Config_;
    const TSolomonRegistryPtr Registry_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NConcurrency::IThreadPoolPtr OffloadThreadPool_;
    const NConcurrency::IThreadPoolPtr EncodingOffloadThreadPool_;

    TFuture<void> CollectorFuture_;

    NConcurrency::TAsyncReaderWriterLock Lock_;
    std::vector<std::pair<i64, TInstant>> Window_;

    TInstant StartTime_ = TInstant::Now();

    TSpinLock StatusLock_;
    std::optional<TInstant> LastFetch_;
    THashMap<std::string, std::optional<TInstant>> LastShardFetch_;

    struct TCacheKey
    {
        std::optional<std::string> Shard;
        ::NMonitoring::EFormat Format;
        ::NMonitoring::ECompression Compression;

        TInstant Now;
        TDuration Period;
        std::optional<TDuration> Grid;

        bool operator == (const TCacheKey& other) const = default;

        operator size_t () const;
    };

    TSpinLock CacheLock_;
    THashMap<TCacheKey, TFuture<TSharedRef>> ResponseCache_;

    TEventTimer CollectionStartDelay_;
    TCounter WindowErrors_;
    TCounter ReadDelays_;
    TCounter ResponseCacheHit_, ResponseCacheMiss_;

    struct TRemoteProcess final
    {
        TRemoteProcess(TCallback<TFuture<TSharedRef>()> dumpSensors, TSolomonRegistry* registry)
            : DumpSensors(dumpSensors)
            , Registry(registry)
        { }

        TCallback<TFuture<TSharedRef>()> DumpSensors;
        TRemoteRegistry Registry;
    };

    TSpinLock RemoteProcessLock_;
    THashSet<TIntrusivePtr<TRemoteProcess>> RemoteProcessList_;

    void DoCollect();
    void TransferSensors();

    void HandleIndex(const std::string& prefix, const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void HandleStatus(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

    void HandleDebugSensors(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void HandleDebugTags(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

    void HandleShard(
        const std::optional<std::string>& name,
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp);

    void DoHandleShard(
        const std::optional<std::string>& name,
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp);

    void ValidatePeriodAndGrid(std::optional<TDuration> period, std::optional<TDuration> readGridStep, TDuration gridStep);

    TErrorOr<TReadWindow> SelectReadWindow(TInstant now, TDuration period, std::optional<TDuration> readGridStep, TDuration gridStep);

    void CleanResponseCache();

    bool FilterDefaultGrid(const std::string& sensorName);

    static void ValidateSummaryPolicy(ESummaryPolicy policy);

    // For locking.
    friend class TSensorService;
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporter)
YT_DEFINE_TYPEID(TSolomonExporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
