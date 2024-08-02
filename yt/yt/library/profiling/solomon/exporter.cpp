#include "exporter.h"
#include "private.h"
#include "sensor_service.h"
#include "helpers.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/datetime/base.h>

#include <util/stream/str.h>

namespace NYT::NProfiling {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SolomonLogger;

////////////////////////////////////////////////////////////////////////////////

void TShardConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("filter", &TThis::Filter)
        .Default();

    registrar.Parameter("grid_step", &TThis::GridStep)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TSolomonExporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("grid_step", &TThis::GridStep)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("linger_timeout", &TThis::LingerTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(12);

    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(1);
    registrar.Parameter("encoding_thread_pool_size", &TThis::EncodingThreadPoolSize)
        .Default(1);
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("encoding_thread_pool_polling_period", &TThis::EncodingThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("convert_counters_to_rate_for_solomon", &TThis::ConvertCountersToRateForSolomon)
        .Alias("convert_counters_to_rate")
        .Default(true);
    registrar.Parameter("rename_converted_counters", &TThis::RenameConvertedCounters)
        .Default(true);

    registrar.Parameter("export_summary", &TThis::ExportSummary)
        .Default(false);
    registrar.Parameter("export_summary_as_max", &TThis::ExportSummaryAsMax)
        .Default(true);
    registrar.Parameter("export_summary_as_avg", &TThis::ExportSummaryAsAvg)
        .Default(false);

    registrar.Parameter("mark_aggregates", &TThis::MarkAggregates)
        .Default(true);

    registrar.Parameter("strip_sensors_name_prefix", &TThis::StripSensorsNamePrefix)
        .Default(false);

    registrar.Parameter("enable_self_profiling", &TThis::EnableSelfProfiling)
        .Default(true);

    registrar.Parameter("report_build_info", &TThis::ReportBuildInfo)
        .Default(true);

    registrar.Parameter("report_kernel_version", &TThis::ReportKernelVersion)
        .Default(true);

    registrar.Parameter("report_restart", &TThis::ReportRestart)
        .Default(true);

    registrar.Parameter("read_delay", &TThis::ReadDelay)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("host", &TThis::Host)
        .Default();

    registrar.Parameter("instance_tags", &TThis::InstanceTags)
        .Default();

    registrar.Parameter("shards", &TThis::Shards)
        .Default();

    registrar.Parameter("response_cache_ttl", &TThis::ResponseCacheTtl)
        .Default(TDuration::Minutes(2));

    registrar.Parameter("update_sensor_service_tree_period", &TThis::UpdateSensorServiceTreePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("producer_collection_batch_size", &TThis::ProducerCollectionBatchSize)
        .Default(DefaultProducerCollectionBatchSize)
        .GreaterThan(0);

    registrar.Postprocessor([] (TThis* config) {
        if (config->LingerTimeout.GetValue() % config->GridStep.GetValue() != 0) {
            THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple of \"grid_step\"");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        for (const auto& [name, shard] : config->Shards) {
            if (!shard->GridStep) {
                continue;
            }

            if (shard->GridStep < config->GridStep) {
                THROW_ERROR_EXCEPTION("shard \"grid_step\" must be greater than global \"grid_step\"");
            }

            if (shard->GridStep->GetValue() % config->GridStep.GetValue() != 0) {
                THROW_ERROR_EXCEPTION("shard \"grid_step\" must be multiple of global \"grid_step\"");
            }

            if (config->LingerTimeout.GetValue() % shard->GridStep->GetValue() != 0) {
                THROW_ERROR_EXCEPTION("\"linger_timeout\" must be multiple shard \"grid_step\"");
            }
        }
    });
}

TShardConfigPtr TSolomonExporterConfig::MatchShard(const TString& sensorName)
{
    TShardConfigPtr matchedShard;
    int matchSize = -1;

    for (const auto& [name, config] : Shards) {
        for (auto prefix : config->Filter) {
            if (!sensorName.StartsWith(prefix)) {
                continue;
            }

            if (static_cast<int>(prefix.size()) > matchSize) {
                matchSize = prefix.size();
                matchedShard = config;
            }
        }
    }

    return matchedShard;
}

ESummaryPolicy TSolomonExporterConfig::GetSummaryPolicy() const
{
    auto policy = ESummaryPolicy::Default;
    if (ExportSummary) {
        policy |= ESummaryPolicy::All;
    }
    if (ExportSummaryAsMax) {
        policy |= ESummaryPolicy::Max;
    }
    if (ExportSummaryAsAvg) {
        policy |= ESummaryPolicy::Avg;
    }

    return policy;
}

////////////////////////////////////////////////////////////////////////////////

TSolomonExporter::TSolomonExporter(
    TSolomonExporterConfigPtr config,
    TSolomonRegistryPtr registry)
    : Config_(std::move(config))
    , Registry_(registry ? registry : TSolomonRegistry::Get())
    , ControlQueue_(New<TActionQueue>("ProfControl"))
    , OffloadThreadPool_(CreateThreadPool(
        Config_->ThreadPoolSize,
        "ProfOffload",
        {.PollingPeriod = Config_->ThreadPoolPollingPeriod}))
    , EncodingOffloadThreadPool_(CreateThreadPool(
        Config_->EncodingThreadPoolSize,
        "ProfEncode",
        {.PollingPeriod = Config_->EncodingThreadPoolPollingPeriod}))
{
    if (Config_->EnableSelfProfiling) {
        Registry_->Profile(TProfiler{Registry_, ""});
    }

    CollectionStartDelay_ = Registry_->GetSelfProfiler().Timer("/collection_delay");
    WindowErrors_ = Registry_->GetSelfProfiler().Counter("/window_error_count");
    ReadDelays_ = Registry_->GetSelfProfiler().Counter("/read_delay_count");
    ResponseCacheMiss_ = Registry_->GetSelfProfiler().Counter("/response_cache_miss");
    ResponseCacheHit_ = Registry_->GetSelfProfiler().Counter("/response_cache_hit");

    for (const auto& [name, config] : Config_->Shards) {
        LastShardFetch_[name] = std::nullopt;
    }

    Registry_->SetWindowSize(Config_->WindowSize);
    Registry_->SetProducerCollectionBatchSize(Config_->ProducerCollectionBatchSize);
    Registry_->SetGridFactor([config = Config_] (const TString& name) -> int {
        auto shard = config->MatchShard(name);
        if (!shard) {
            return 1;
        }

        if (!shard->GridStep) {
            return 1;
        }

        return shard->GridStep->GetValue() / config->GridStep.GetValue();
    });

    if (Config_->ReportBuildInfo) {
        TProfiler profiler{registry, ""};

        profiler
            .WithRequiredTag("version", GetVersion())
            .AddFuncGauge("/build/version", MakeStrong(this), [] { return 1.0; });
    }

    if (Config_->ReportKernelVersion) {
        TProfiler profiler{registry, ""};

        profiler
            .WithRequiredTag("kernel_version", GetLinuxKernelVersion())
            .AddFuncGauge("/host/kernel_version", MakeStrong(this), [] { return 1.0; });
    }

    if (Config_->ReportRestart) {
        TProfiler profiler{registry, ""};

        for (auto window : {1, 5, 30}) {
            profiler
                .WithRequiredTag("window", ToString(window) + "min")
                .AddFuncGauge("/server/restarted", MakeStrong(this), [this, window] {
                    return (TInstant::Now() - StartTime_ < TDuration::Minutes(window)) ? 1.0 : 0.0;
                });
        }
    }
}

void TSolomonExporter::Register(const TString& prefix, const NYT::NHttp::IServerPtr& server)
{
    Register(prefix, server->GetPathMatcher());
}

void TSolomonExporter::Register(const TString& prefix, const NYT::NHttp::IRequestPathMatcherPtr& handlers)
{
    handlers->Add(prefix + "/", BIND(&TSolomonExporter::HandleIndex, MakeStrong(this), prefix));
    handlers->Add(prefix + "/sensors", BIND(&TSolomonExporter::HandleDebugSensors, MakeStrong(this)));
    handlers->Add(prefix + "/tags", BIND(&TSolomonExporter::HandleDebugTags, MakeStrong(this)));
    handlers->Add(prefix + "/status", BIND(&TSolomonExporter::HandleStatus, MakeStrong(this)));
    handlers->Add(prefix + "/all", BIND(&TSolomonExporter::HandleShard, MakeStrong(this), std::nullopt));

    for (const auto& [shardName, shard] : Config_->Shards) {
        handlers->Add(
            prefix + "/shard/" + shardName,
            BIND(&TSolomonExporter::HandleShard, MakeStrong(this), shardName));
    }
}

void TSolomonExporter::Start()
{
    CollectorFuture_ = BIND([this, this_ = MakeStrong(this)] {
        try {
            DoCollect();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Sensor collector crashed");
        }
    })
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run();
}

void TSolomonExporter::Stop()
{
    CollectorFuture_.Cancel(TError("Stopped"));
    ControlQueue_->Shutdown();
    OffloadThreadPool_->Shutdown();
    EncodingOffloadThreadPool_->Shutdown();
}

void TSolomonExporter::TransferSensors()
{
    std::vector<std::pair<TFuture<TSharedRef>, TIntrusivePtr<TRemoteProcess>>> remoteFutures;
    {
        auto processGuard = Guard(RemoteProcessLock_);

        for (const auto& process : RemoteProcessList_) {
            try {
                auto asyncDump = process->DumpSensors();
                remoteFutures.emplace_back(asyncDump, process);
            } catch (const std::exception& ex) {
                remoteFutures.emplace_back(MakeFuture<TSharedRef>(TError(ex)), process);
            }
        }
    }

    std::vector<TIntrusivePtr<TRemoteProcess>> deadProcesses;
    for (const auto& [dumpFuture, process] : remoteFutures) {
        // Use blocking Get(), because we want to lock current thread while data structure is updating.
        auto result = dumpFuture.Get();

        if (result.IsOK()) {
            try {
                NProto::TSensorDump sensorDump;
                DeserializeProto(&sensorDump, result.Value());
                process->Registry.Transfer(sensorDump);
            } catch (const std::exception& ex) {
                result = TError(ex);
            }
        }

        if (!result.IsOK()) {
            process->Registry.Detach();
            deadProcesses.push_back(process);
        }
    }

    {
        auto processGuard = Guard(RemoteProcessLock_);
        for (const auto& process : deadProcesses) {
            RemoteProcessList_.erase(process);
        }
    }
}

void TSolomonExporter::DoCollect()
{
    auto nowUnix = TInstant::Now().GetValue();
    nowUnix -= (nowUnix % Config_->GridStep.GetValue());
    auto nextGridTime = TInstant::FromValue(nowUnix) + Config_->GridStep;

    auto waitUntil = [] (auto deadline) {
        auto now = TInstant::Now();
        if (now >= deadline) {
            Yield();
            return;
        }
        TDelayedExecutor::WaitForDuration(deadline - now);
    };

    // Compute start time. Zero iteration time should be aligned with each GridStep.
    while (true) {
        bool aligned = true;
        for (const auto& [name, shard] : Config_->Shards) {
            if (shard->GridStep && nextGridTime.GetValue() % shard->GridStep->GetValue() != 0) {
                aligned = false;
            }
        }

        if (aligned) {
            break;
        }

        nextGridTime += Config_->GridStep;
    }

    YT_LOG_DEBUG("Sensor collector started (StartTime: %v)", nextGridTime);
    while (true) {
        CleanResponseCache();

        waitUntil(nextGridTime);

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
            .ValueOrThrow();

        auto delay = TInstant::Now() - nextGridTime;
        CollectionStartDelay_.Record(delay);

        YT_LOG_DEBUG("Started sensor collection (Delay: %v)", delay);
        Registry_->ProcessRegistrations();

        auto iteration = Registry_->GetNextIteration();
        Registry_->Collect(OffloadThreadPool_->GetInvoker());

        TransferSensors();

        Window_.emplace_back(iteration, nextGridTime);
        if (Window_.size() > static_cast<size_t>(Registry_->GetWindowSize())) {
            Window_.erase(Window_.begin());
        }

        YT_LOG_DEBUG("Finished sensor collection (Delay: %v)", TInstant::Now() - nextGridTime);
        nextGridTime += Config_->GridStep;
    }
}

constexpr auto IndexPage = R"EOF(
<!DOCTYPE html>
<html>
<body>
<a href="%vsensors">sensors top</a>
<br/>
<a href="%vtags">tags top</a>
<br/>
<a href="%vstatus">status</a>
</body>
</html>
)EOF";

void TSolomonExporter::HandleIndex(const TString& prefix, const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    if (req->GetUrl().Path != prefix && req->GetUrl().Path != prefix + "/") {
        rsp->SetStatus(EStatusCode::NotFound);
        WaitFor(rsp->WriteBody(TSharedRef::FromString("Not found")))
            .ThrowOnError();
        return;
    }

    rsp->SetStatus(EStatusCode::OK);
    rsp->GetHeaders()->Add("Content-Type", "text/html; charset=UTF-8");

    auto prefixWithSlash = !prefix.empty() ? prefix + "/" : prefix;
    auto indexPageFormatted = Format(IndexPage, prefixWithSlash, prefixWithSlash, prefixWithSlash);

    WaitFor(rsp->WriteBody(TSharedRef::FromString(indexPageFormatted)))
        .ThrowOnError();
}

void TSolomonExporter::HandleStatus(const IRequestPtr&, const IResponseWriterPtr& rsp)
{
    auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
        .ValueOrThrow();

    auto sensors = Registry_->ListSensors();
    THashMap<TString, TError> invalidSensors;
    for (const auto& sensor : sensors) {
        if (sensor.Error.IsOK()) {
            continue;
        }

        invalidSensors[sensor.Name] = sensor.Error;
    }

    rsp->SetStatus(EStatusCode::OK);
    ReplyJson(rsp, [&] (auto consumer) {
        auto statusGuard = Guard(StatusLock_);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("start_time").Value(StartTime_)
                .Item("last_fetch").Value(LastFetch_)
                .Item("last_shard_fetch").DoMapFor(LastShardFetch_, [] (auto fluent, auto time) {
                    fluent
                        .Item(time.first).Value(time.second);
                })
                .Item("window").Value(Window_)
                .Item("sensor_errors").DoMapFor(invalidSensors, [] (auto fluent, auto sensor) {
                    fluent
                        .Item(sensor.first).Value(sensor.second);
                })
                .Item("dynamic_tags").Value(Registry_->GetDynamicTags())
            .EndMap();
    });
}

void TSolomonExporter::HandleDebugSensors(const IRequestPtr&, const IResponseWriterPtr& rsp)
{
    auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
        .ValueOrThrow();

    rsp->SetStatus(EStatusCode::OK);
    rsp->GetHeaders()->Add("Content-Type", "text/plain; charset=UTF-8");

    auto sensors = Registry_->ListSensors();
    std::sort(sensors.begin(), sensors.end(), [] (const auto& a, const auto& b) {
        return std::tie(a.CubeSize, a.Name) > std::tie(b.CubeSize, b.Name);
    });

    TStringStream out;
    out << "# cube_size object_count name error" << Endl;
    for (const auto& sensor : sensors) {
        out << sensor.CubeSize << " " << sensor.ObjectCount << " " << sensor.Name;

        if (!sensor.Error.IsOK()) {
            out << " " << ToString(sensor.Error);
        }

        out << Endl;
    }

    WaitFor(rsp->WriteBody(TSharedRef::FromString(out.Str())))
        .ThrowOnError();
}

void TSolomonExporter::HandleDebugTags(const IRequestPtr&, const IResponseWriterPtr& rsp)
{
    auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
        .ValueOrThrow();

    rsp->SetStatus(EStatusCode::OK);
    rsp->GetHeaders()->Add("Content-Type", "text/plain; charset=UTF-8");

    auto tags = Registry_->GetTags().GetTopByKey();
    std::vector<std::pair<TString, size_t>> tagList{tags.begin(), tags.end()};
    std::sort(tagList.begin(), tagList.end(), [] (auto a, auto b) {
        return std::tie(a.second, a.first) > std::tie(b.second, b.first);
    });

    TStringStream out;
    out << "# value_count tag_name" << Endl;
    for (const auto& tag : tagList) {
        out << tag.second << " " << tag.first << Endl;
    }

    WaitFor(rsp->WriteBody(TSharedRef::FromString(out.Str())))
        .ThrowOnError();
}

std::optional<TString> TSolomonExporter::ReadJson(const TReadOptions& options, std::optional<TString> shard)
{
    TStringStream buffer;
    auto encoder = NMonitoring::BufferedEncoderJson(&buffer);
    if (ReadSensors(std::move(encoder), options, shard)) {
        return buffer.Str();
    }
    return {};
}

std::optional<TString> TSolomonExporter::ReadSpack(const TReadOptions& options, std::optional<TString> shard)
{
    TStringStream buffer;
    auto encoder = NMonitoring::EncoderSpackV1(
        &buffer,
        NMonitoring::ETimePrecision::SECONDS,
        NMonitoring::ECompression::ZSTD);
    if (ReadSensors(std::move(encoder), options, shard)) {
        return buffer.Str();
    }
    return {};
}

bool TSolomonExporter::ReadSensors(
    NMonitoring::IMetricEncoderPtr encoder,
    const TReadOptions& options,
    std::optional<TString> shard)
{
    auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
        .ValueOrThrow();

    if (Window_.empty()) {
        return false;
    }

    // Read last value.
    auto readOptions = options;
    readOptions.Times.emplace_back(std::vector<int>{Registry_->IndexOf(Window_.back().first)}, TInstant::Zero());
    readOptions.ConvertCountersToRateGauge = false;
    readOptions.EnableHistogramCompat = true;

    readOptions.SummaryPolicy |= Config_->GetSummaryPolicy();
    ValidateSummaryPolicy(readOptions.SummaryPolicy);

    readOptions.MarkAggregates |= Config_->MarkAggregates;
    if (!readOptions.Host && Config_->Host) {
        readOptions.Host = Config_->Host;
    }
    if (readOptions.InstanceTags.empty() && !Config_->InstanceTags.empty()) {
        readOptions.InstanceTags.reserve(Config_->InstanceTags.size());
        for (auto&& [k, v] : Config_->InstanceTags) {
            readOptions.InstanceTags.emplace_back(k, v);
        }
    }

    if (shard) {
        readOptions.SensorFilter = [&, name = shard.value()] (const TString& sensorName) {
            return Config_->MatchShard(sensorName) == Config_->Shards[name];
        };
    } else {
        readOptions.SensorFilter = [this] (const TString& sensorName) {
            return FilterDefaultGrid(sensorName);
        };
    }

    encoder->OnStreamBegin();
    Registry_->ReadSensors(readOptions, encoder.Get());
    encoder->OnStreamEnd();
    guard->Release();
    encoder->Close();
    return true;
}

void TSolomonExporter::HandleShard(
    const std::optional<TString>& name,
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp)
{
    auto reply = BIND(&TSolomonExporter::DoHandleShard, MakeStrong(this), name, req, rsp)
        .AsyncVia(OffloadThreadPool_->GetInvoker())
        .Run();

    WaitFor(reply)
        .ThrowOnError();
}

void TSolomonExporter::DoHandleShard(
    const std::optional<TString>& name,
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp)
{
    TPromise<TSharedRef> responsePromise = NewPromise<TSharedRef>();

    auto Logger = NProfiling::Logger().WithTag("Shard: %v", name);

    try {
        auto outputEncodingContext = CreateOutputEncodingContextFromHeaders(req->GetHeaders());
        FillResponseHeaders(outputEncodingContext, rsp->GetHeaders());

        TCgiParameters params(req->GetUrl().RawQuery);

        std::optional<TDuration> period;
        if (auto it = params.Find("period"); it != params.end()) {
            period = TDuration::Parse(it->second);
        }

        std::optional<TInstant> now;
        if (auto it = params.Find("now"); it != params.end()) {
            now = TInstant::ParseIso8601(it->second);
        }

        std::optional<TDuration> readGridStep;
        if (auto gridHeader = req->GetHeaders()->Find("X-Solomon-GridSec"); gridHeader) {
            int gridSeconds;
            if (!TryFromString<int>(*gridHeader, gridSeconds)) {
                THROW_ERROR_EXCEPTION("Invalid value of \"X-Solomon-GridSec\" header")
                    << TErrorAttribute("value", *gridHeader);
            }
            readGridStep = TDuration::Seconds(gridSeconds);
        }

        if ((now && !period) || (period && !now)) {
            THROW_ERROR_EXCEPTION("Both \"period\" and \"now\" must be present in request")
                << TErrorAttribute("now", now)
                << TErrorAttribute("period", period);
        }

        auto gridStep = Config_->GridStep;
        if (name) {
            auto shardConfig = Config_->Shards[*name];
            if (shardConfig->GridStep) {
                gridStep = *shardConfig->GridStep;
            }
        }

        ValidatePeriodAndGrid(period, readGridStep, gridStep);

        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
            .ValueOrThrow();

        if (Window_.empty()) {
            WindowErrors_.Increment();
            THROW_ERROR_EXCEPTION("Window is empty");
        }

        std::optional<TCacheKey> cacheKey;
        if (now && period) {
            cacheKey = TCacheKey{
                .Shard = name,
                .Format = outputEncodingContext.Format,
                .Compression = outputEncodingContext.Compression,
                .Now = *now,
                .Period = *period,
                .Grid = readGridStep,
            };
        }

        auto solomonCluster = req->GetHeaders()->Find("X-Solomon-ClusterId");
        YT_LOG_DEBUG("Processing sensor pull (Format: %v, Compression: %v, SolomonCluster: %v, Now: %v, Period: %v, Grid: %v)",
            outputEncodingContext.Format,
            outputEncodingContext.Compression,
            solomonCluster ? *solomonCluster : "",
            now,
            period,
            readGridStep);

        if (cacheKey) {
            auto cacheGuard = Guard(CacheLock_);

            auto cacheHitIt = ResponseCache_.find(*cacheKey);
            if (cacheHitIt != ResponseCache_.end() && !(cacheHitIt->second.IsSet() && !cacheHitIt->second.Get().IsOK())) {
                YT_LOG_DEBUG("Replying from cache");

                ResponseCacheHit_.Increment();

                auto cachedResponse = cacheHitIt->second;
                cacheGuard.Release();
                guard->Release();

                rsp->SetStatus(EStatusCode::OK);
                WaitFor(rsp->WriteBody(WaitFor(cachedResponse).ValueOrThrow()))
                    .ThrowOnError();

                return;
            }

            ResponseCache_[*cacheKey] = responsePromise.ToFuture();
        }

        TReadWindow readWindow;
        if (period) {
            if (auto errorOrWindow = SelectReadWindow(*now, *period, readGridStep, gridStep); !errorOrWindow.IsOK()) {
                ReadDelays_.Increment();
                YT_LOG_DEBUG(errorOrWindow, "Delaying sensor read (Delay: %v)", Config_->ReadDelay);

                guard->Release();
                TDelayedExecutor::WaitForDuration(Config_->ReadDelay);
                guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Lock_))
                    .ValueOrThrow();

                if (auto errorOrWindow = SelectReadWindow(*now, *period, readGridStep, gridStep); !errorOrWindow.IsOK()) {
                    WindowErrors_.Increment();
                    THROW_ERROR errorOrWindow;
                } else {
                    readWindow = errorOrWindow.Value();
                }
            } else {
                readWindow = errorOrWindow.Value();
            }
        } else {
            YT_LOG_DEBUG("Timestamp query arguments are missing; returning last value");

            int gridFactor = gridStep / Config_->GridStep;
            for (auto i = static_cast<int>(Window_.size()) - 1; i >= 0; --i) {
                auto [iteration, time] = Window_[i];
                if (iteration % gridFactor == 0) {
                    readWindow.emplace_back(std::vector<int>{Registry_->IndexOf(iteration / gridFactor)}, time);
                    break;
                }
            }

            if (readWindow.empty()) {
                THROW_ERROR_EXCEPTION("Can't find latest timestamp")
                    << TErrorAttribute("first_iteration", Window_[0].first)
                    << TErrorAttribute("grid_factor", gridFactor)
                    << TErrorAttribute("window_size", Window_.size());
            }
        }

        if (cacheKey) {
            ResponseCacheMiss_.Increment();
        }

        TReadOptions options;
        options.Host = Config_->Host;
        options.InstanceTags = std::vector<TTag>{Config_->InstanceTags.begin(), Config_->InstanceTags.end()};

        if (Config_->ConvertCountersToRateForSolomon && outputEncodingContext.IsSolomonPull) {
            options.ConvertCountersToRateGauge = true;
            options.RenameConvertedCounters = Config_->RenameConvertedCounters;

            options.RateDenominator = gridStep.SecondsFloat();
            if (readGridStep) {
                options.RateDenominator = readGridStep->SecondsFloat();
            }
        }

        options.EnableSolomonAggregationWorkaround = outputEncodingContext.IsSolomonPull;
        options.Times = readWindow;
        options.SummaryPolicy = Config_->GetSummaryPolicy();
        options.MarkAggregates = Config_->MarkAggregates;
        options.StripSensorsNamePrefix = Config_->StripSensorsNamePrefix;
        options.LingerWindowSize = Config_->LingerTimeout / gridStep;

        if (name) {
            options.SensorFilter = [&] (const TString& sensorName) {
                return Config_->MatchShard(sensorName) == Config_->Shards[*name];
            };
        } else {
            options.SensorFilter = [this] (const TString& sensorName) {
                return FilterDefaultGrid(sensorName);
            };
        }

        {
            auto statusGuard = Guard(StatusLock_);
            if (name) {
                LastShardFetch_[*name] = TInstant::Now();
            } else {
                LastFetch_ = TInstant::Now();
            }
        }

        outputEncodingContext.Encoder->OnStreamBegin();
        Registry_->ReadSensors(options, outputEncodingContext.Encoder.Get());
        outputEncodingContext.Encoder->OnStreamEnd();

        guard->Release();

        // NB(eshcherbin): Offload inner representation to binary/text format encoding (including compression).
        auto encodeFuture = BIND([buffer = outputEncodingContext.EncoderBuffer, encoder = std::move(outputEncodingContext.Encoder)] {
            encoder->Close();
        })
            .AsyncVia(EncodingOffloadThreadPool_->GetInvoker())
            .Run();
        WaitFor(encodeFuture)
            .ThrowOnError();

        rsp->SetStatus(EStatusCode::OK);

        auto replyBlob = TSharedRef::FromString(outputEncodingContext.EncoderBuffer->Str());
        responsePromise.Set(replyBlob);

        WaitFor(rsp->WriteBody(replyBlob))
            .ThrowOnError();
    } catch(const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Failed to export sensors");
        responsePromise.TrySet(TError(ex));

        if (!rsp->AreHeadersFlushed()) {
            try {
                rsp->SetStatus(EStatusCode::InternalServerError);
                rsp->GetHeaders()->Remove("Content-Type");
                rsp->GetHeaders()->Remove("Content-Encoding");

                // Send only message. It should be displayed nicely in Solomon UI.
                WaitFor(rsp->WriteBody(TSharedRef::FromString(TError(ex).GetMessage())))
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to send export error");
            }
        }
    }
}

void TSolomonExporter::ValidatePeriodAndGrid(std::optional<TDuration> period, std::optional<TDuration> readGridStep, TDuration gridStep)
{
    if (!period) {
        return;
    }

    if (*period < gridStep) {
        THROW_ERROR_EXCEPTION("Period cannot be lower than grid step")
            << TErrorAttribute("period", *period)
            << TErrorAttribute("grid_step", gridStep);
    }

    if (period->GetValue() % gridStep.GetValue() != 0) {
        THROW_ERROR_EXCEPTION("Period must be multiple of grid step")
            << TErrorAttribute("period", *period)
            << TErrorAttribute("grid_step", gridStep);
    }

    if (readGridStep) {
        if (*readGridStep < gridStep) {
            THROW_ERROR_EXCEPTION("Server grid step cannot be lower than client grid step")
                << TErrorAttribute("server_grid_step", *readGridStep)
                << TErrorAttribute("grid_step", gridStep);
        }

        if (readGridStep->GetValue() % gridStep.GetValue() != 0) {
            THROW_ERROR_EXCEPTION("Server grid step must be multiple of client grid step")
                << TErrorAttribute("server_grid_step", *readGridStep)
                << TErrorAttribute("grid_step", gridStep);
        }

        if (*readGridStep > *period) {
            THROW_ERROR_EXCEPTION("Server grid step cannot be greater than fetch period")
                << TErrorAttribute("server_grid_step", *readGridStep)
                << TErrorAttribute("period", *period);
        }

        if (period->GetValue() % readGridStep->GetValue() != 0) {
            THROW_ERROR_EXCEPTION("Server grid step must be multiple of fetch period")
                << TErrorAttribute("server_grid_step", *readGridStep)
                << TErrorAttribute("period", *period);
        }
    }
}

IYPathServicePtr TSolomonExporter::GetSensorService()
{
    return CreateSensorService(Config_, Registry_, MakeStrong(this));
}

TErrorOr<TReadWindow> TSolomonExporter::SelectReadWindow(
    TInstant now,
    TDuration period,
    std::optional<TDuration> readGridStep,
    TDuration gridStep)
{
    TReadWindow readWindow;

    int gridSubsample = 1;
    if (readGridStep) {
        gridSubsample = *readGridStep / gridStep;
    }

    int gridFactor = gridStep / Config_->GridStep;

    for (auto [iteration, time] : Window_) {
        if (iteration % gridFactor != 0) {
            continue;
        }

        int index = Registry_->IndexOf(iteration / gridFactor);
        if (time >= now - period && time < now) {
            if (readWindow.empty() ||
                readWindow.back().first.size() >= static_cast<size_t>(gridSubsample))
            {
                readWindow.emplace_back(std::vector<int>{index}, time);
            } else {
                readWindow.back().first.push_back(index);
                readWindow.back().second = time;
            }
        }
    }

    auto readGrid = gridStep;
    if (readGridStep) {
        readGrid = *readGridStep;
    }

    if (readWindow.size() != period / readGrid ||
        readWindow.empty() ||
        readWindow.back().first.size() != static_cast<size_t>(gridSubsample))
    {
        return TError("Read query is outside of window")
            << TErrorAttribute("now", now)
            << TErrorAttribute("period", period)
            << TErrorAttribute("grid", readGridStep)
            << TErrorAttribute("window_first", Window_.front().second)
            << TErrorAttribute("window_last", Window_.back().second);
    }

    return readWindow;
}

void TSolomonExporter::CleanResponseCache()
{
    auto guard = Guard(CacheLock_);

    std::vector<TCacheKey> toRemove;
    for (const auto& [key, response] : ResponseCache_) {
        if (key.Now < TInstant::Now() - Config_->ResponseCacheTtl) {
            toRemove.push_back(key);
        }
    }

    for (const auto& removedKey : toRemove) {
        ResponseCache_.erase(removedKey);
    }
}

void TSolomonExporter::ValidateSummaryPolicy(ESummaryPolicy policy)
{
    static const TError SummaryPolicyError("Invalid summary policy in read options");

    auto summaryPolicyConflicts = GetSummaryPolicyConflicts(policy);
    if (summaryPolicyConflicts.AllPolicyWithSpecifiedAggregates) {
        THROW_ERROR SummaryPolicyError
            << TError("%Qlv policy can be used only without specified policies", ESummaryPolicy::All)
            << TErrorAttribute("policy", policy);
    }
    if (summaryPolicyConflicts.OmitNameLabelSuffixWithSeveralAggregates) {
        THROW_ERROR SummaryPolicyError
            << TError("%Qlv option can be used only with single specified policy", ESummaryPolicy::OmitNameLabelSuffix)
            << TErrorAttribute("policy", policy);
    }
}

bool TSolomonExporter::FilterDefaultGrid(const TString& sensorName)
{
    auto shard = Config_->MatchShard(sensorName);
    if (shard && shard->GridStep) {
        return Config_->GridStep == *shard->GridStep;
    }

    return true;
}

TSharedRef TSolomonExporter::DumpSensors()
{
    auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
        .ValueOrThrow();

    Registry_->ProcessRegistrations();
    Registry_->Collect(OffloadThreadPool_->GetInvoker());

    return SerializeProtoToRef(Registry_->DumpSensors(Config_->Host, Config_->InstanceTags));
}

void TSolomonExporter::AttachRemoteProcess(TCallback<TFuture<TSharedRef>()> dumpSensors)
{
    auto guard = Guard(RemoteProcessLock_);
    RemoteProcessList_.insert(New<TRemoteProcess>(dumpSensors, Registry_.Get()));
}

////////////////////////////////////////////////////////////////////////////////

TSolomonExporter::TCacheKey::operator size_t() const
{
    size_t hash = 0;
    HashCombine(hash, Shard);
    HashCombine(hash, static_cast<int>(Format));
    HashCombine(hash, static_cast<int>(Compression));
    HashCombine(hash, Now);
    HashCombine(hash, Period);
    HashCombine(hash, Grid);
    return hash;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
