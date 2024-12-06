#include "tcmalloc_manager.h"

#include "config.h"

#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>

#include <yt/yt/library/ytprof/external_pprof.h>
#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>

#include <library/cpp/yt/memory/leaky_singleton.h>
#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/exit.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/crash_handler.h>

#include <util/system/thread.h>
#include <util/system/shellcommand.h>

#include <tcmalloc/malloc_extension.h>

#include <thread>
#include <mutex>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TCMalloc");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TOomProfileManifest)

struct TOomProfileManifest
    : public NYTree::TYsonStruct
{
    TString CurrentProfilePath;
    TString PeakProfilePath;

    REGISTER_YSON_STRUCT(TOomProfileManifest);

    static void Register(TRegistrar registrar)
    {
        // TODO(babenko): fix names, see ytserver_mail_profile_link.py
        registrar.Parameter("heap_profile_path", &TThis::CurrentProfilePath)
            .Default();
        registrar.Parameter("peak_profile_path", &TThis::PeakProfilePath)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TOomProfileManifest)

////////////////////////////////////////////////////////////////////////////////

TString MakeIncompletePath(const TString& path)
{
    return NYT::Format("%v_incomplete", path);
}

void CollectAndDumpMemoryProfile(const TString& memoryProfilePath, tcmalloc::ProfileType profileType)
{
    auto profile = NYTProf::ReadHeapProfile(profileType);
    SymbolizeByExternalPProf(&profile, NYTProf::TSymbolizationOptions{
        .RunTool = [] (const std::vector<TString>& args) {
            TShellCommand command{args[0], TList<TString>{args.begin() + 1, args.end()}};
            command.Run();
        },
    });

    auto incompletePath = MakeIncompletePath(memoryProfilePath);

    TFileOutput output(incompletePath);
    NYTProf::WriteProfile(&output, profile);
    output.Finish();
    NFS::Rename(incompletePath, memoryProfilePath);
}

void MemoryProfileTimeoutHandler(int /*signal*/)
{
    AbortProcessDramatically(
        EProcessExitCode::GenericError,
        "Process hung while dumping heap profile");
}

void SetupMemoryProfileTimeout(int timeout)
{
    ::signal(SIGALRM, &MemoryProfileTimeoutHandler);
    ::alarm(timeout);
}

void DumpManifest(const TOomProfileManifestPtr& manifest, const TString& fileName)
{
    TFileOutput output(fileName);
    NYson::TYsonWriter writer(&output, NYson::EYsonFormat::Pretty);
    Serialize(manifest, &writer);
    writer.Flush();
    output.Finish();
}

////////////////////////////////////////////////////////////////////////////////

class TMemoryLimitHandler
    : public TRefCounted
{
public:
    explicit TMemoryLimitHandler(THeapSizeLimitConfigPtr config)
        : Config_(std::move(config))
    {
        Thread_ = std::thread([this] {
            Handle();
        });
    }

    ~TMemoryLimitHandler()
    {
        {
            std::unique_lock<std::mutex> lock(Mutex_);
            Fired_ = true;
            CV_.notify_all();
        }

        Thread_.join();
    }

    void Fire()
    {
        std::unique_lock<std::mutex> lock(Mutex_);
        Fired_ = true;
        NeedToHandle_ = true;
        CV_.notify_all();
    }

private:
    const THeapSizeLimitConfigPtr Config_;

    bool Fired_ = false;
    bool NeedToHandle_ = false;
    std::mutex Mutex_;
    std::condition_variable CV_;
    std::thread Thread_;

    void Handle()
    {
        std::unique_lock<std::mutex> lock(Mutex_);
        CV_.wait(lock, [&] {
            return Fired_;
        });

        if (!NeedToHandle_) {
            return;
        }

        auto timestamp = TInstant::Now().FormatLocalTime("%Y%m%dT%H%M%S");
        auto manifest = New<TOomProfileManifest>();
        auto manifestPath = GetManifestPath(timestamp);
        manifest->CurrentProfilePath = GetCurrentDumpPath(timestamp);
        manifest->PeakProfilePath = GetPeakDumpPath(timestamp);

        Cerr << "*** Forking process to write heap profiles" << Endl
            << "  current:  " << manifest->CurrentProfilePath << Endl
            << "  peak:     " << manifest->PeakProfilePath << Endl
            << "  manifest: " << manifestPath << Endl;

        SetupMemoryProfileTimeout(Config_->MemoryProfileDumpTimeout.Seconds());

        auto childPid = fork();
        if (childPid == 0) {
            NFS::MakeDirRecursive(*Config_->MemoryProfileDumpPath);
            SetupMemoryProfileTimeout(Config_->MemoryProfileDumpTimeout.Seconds());
            CollectAndDumpMemoryProfile(manifest->CurrentProfilePath, tcmalloc::ProfileType::kHeap);
            CollectAndDumpMemoryProfile(manifest->PeakProfilePath, tcmalloc::ProfileType::kPeakHeap);
            DumpManifest(manifest, manifestPath);

            Cerr << "*** Heap profiles are written" << Endl;
            AbortProcessSilently(EProcessExitCode::OK);
        }

        if (childPid < 0) {
            Cerr << "*** Fork failed: " << LastSystemErrorText() << Endl;
            AbortProcessSilently(EProcessExitCode::GenericError);
        }

        ExecWaitForChild(childPid);
        AbortProcessSilently(EProcessExitCode::OK);
    }

    auto MakeSuffixFormatter(const TString& timestamp) const
    {
        return NYT::MakeFormatterWrapper([this, &timestamp] (TStringBuilderBase* builder) {
            if (Config_->MemoryProfileDumpFilenameSuffix) {
                builder->AppendFormat("%v_", *Config_->MemoryProfileDumpFilenameSuffix);
            }
            FormatValue(builder, timestamp, "v");
        });
    }

    TString GetCurrentDumpPath(const TString& timestamp) const
    {
        return Format(
            "%v/current_%v.pb.gz",
            Config_->MemoryProfileDumpPath,
            MakeSuffixFormatter(timestamp));
    }

    TString GetPeakDumpPath(const TString& timestamp) const
    {
        return Format(
            "%v/peak_%v.pb.gz",
            Config_->MemoryProfileDumpPath,
            MakeSuffixFormatter(timestamp));
    }

    TString GetManifestPath(const TString& timestamp) const
    {
        return Format(
            "%v/oom_profile_paths_%v.yson",
            Config_->MemoryProfileDumpPath,
            MakeSuffixFormatter(timestamp));
    }

    void ExecWaitForChild(int pid)
    {
        Cerr << "*** Start waiting for the child" << Endl;

        auto command = Format("while [ -e /proc/%v ]; do sleep 1; done;", pid);
        execl("/bin/bash", "/bin/bash", "-c",  command.c_str(), (void*)nullptr);

        Cerr << "*** Failed to switch main process to dummy child waiter: "
            << LastSystemErrorText() << Endl;
    }
};

DEFINE_REFCOUNTED_TYPE(TMemoryLimitHandler);

////////////////////////////////////////////////////////////////////////////////

template <class TMallocExtension>
concept CSupportsLimitHandler = requires (TMallocExtension extension)
{
    { extension.GetSoftMemoryLimitHandler() };
};

template <typename TMallocExtension, typename THandler>
void SetSoftMemoryLimitHandler(THandler)
{
    WriteToStderr("TCMalloc does not support memory limit handler\n");
}

template <CSupportsLimitHandler TMallocExtension, typename THandler>
void SetSoftMemoryLimitHandler(THandler handler)
{
    TMallocExtension::SetSoftMemoryLimitHandler(handler);
}

////////////////////////////////////////////////////////////////////////////////

class TMemoryLimitHandlerInstaller
{
public:
    static TMemoryLimitHandlerInstaller* Get()
    {
        return LeakySingleton<TMemoryLimitHandlerInstaller>();
    }

    void Reconfigure(const THeapSizeLimitConfigPtr& config)
    {
        if (config->DumpMemoryProfileOnViolation) {
            Enable(config);
        } else {
            Disable();
        }
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();

    TAtomicIntrusivePtr<TMemoryLimitHandler> LimitHandler_;

    TMemoryLimitHandlerInstaller()
    {
        SetSoftMemoryLimitHandler<tcmalloc::MallocExtension>(&HandleTCMallocLimit);
    }

    static void HandleTCMallocLimit()
    {
        if (auto handler = Get()->LimitHandler_.Acquire()) {
            handler->Fire();
        }
    }

    void Enable(const THeapSizeLimitConfigPtr& config)
    {
        if (LimitHandler_.Acquire()) {
            return;
        }

        TAtomicIntrusivePtr<TMemoryLimitHandler>::TRawPtr expected = nullptr;
        LimitHandler_.CompareAndSwap(expected, New<TMemoryLimitHandler>(config));
    }

    void Disable()
    {
        LimitHandler_.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLimitsAdjuster
{
public:
    static TLimitsAdjuster* Get()
    {
        return LeakySingleton<TLimitsAdjuster>();
    }

    void Reconfigure(const TTCMallocConfigPtr& config)
    {
        i64 totalMemory = GetAnonymousMemoryLimit();
        AdjustPageHeapLimit(totalMemory, config);
        AdjustAggressiveReleaseThreshold(totalMemory, config);
    }

    i64 GetAggressiveReleaseThreshold()
    {
        return AggressiveReleaseThreshold_;
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();

    TLimitsAdjuster() = default;

    using TAllocatorMemoryLimit = tcmalloc::MallocExtension::MemoryLimit;

    TAllocatorMemoryLimit AppliedLimit_;
    i64 AggressiveReleaseThreshold_ = 0;


    void AdjustPageHeapLimit(i64 totalMemory, const TTCMallocConfigPtr& config)
    {
        auto proposed = ProposeHeapMemoryLimit(totalMemory, config);

        if (proposed.limit == AppliedLimit_.limit && proposed.hard == AppliedLimit_.hard) {
            // Already applied.
            return;
        }

        YT_LOG_INFO("Changing TCMalloc memory limit (Limit: %v, Hard: %v)",
            proposed.limit,
            proposed.hard);

        tcmalloc::MallocExtension::SetMemoryLimit(proposed);
        AppliedLimit_ = proposed;
    }

    void AdjustAggressiveReleaseThreshold(i64 totalMemory, const TTCMallocConfigPtr& config)
    {
        if (totalMemory && config->AggressiveReleaseThresholdRatio) {
            AggressiveReleaseThreshold_ = *config->AggressiveReleaseThresholdRatio * totalMemory;
        } else {
            AggressiveReleaseThreshold_ = config->AggressiveReleaseThreshold;
        }
    }

    i64 GetAnonymousMemoryLimit() const
    {
        return NProfiling::TResourceTracker::GetAnonymousMemoryLimit();
    }

    TAllocatorMemoryLimit ProposeHeapMemoryLimit(i64 totalMemory, const TTCMallocConfigPtr& config) const
    {
        const auto& heapSizeConfig = config->HeapSizeLimit;

        if (totalMemory == 0 || !heapSizeConfig->ContainerMemoryRatio && !heapSizeConfig->ContainerMemoryMargin) {
            return {};
        }

        TAllocatorMemoryLimit proposed;
        proposed.hard = heapSizeConfig->Hard;

        if (heapSizeConfig->ContainerMemoryMargin) {
            proposed.limit = totalMemory - *heapSizeConfig->ContainerMemoryMargin;
        } else {
            proposed.limit = *heapSizeConfig->ContainerMemoryRatio * totalMemory;
        }

        return proposed;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TTCMallocManagerImpl
{
public:
    static TTCMallocManagerImpl* Get()
    {
        return LeakySingleton<TTCMallocManagerImpl>();
    }

    void Configure(const TTCMallocConfigPtr& config)
    {
        tcmalloc::MallocExtension::SetBackgroundReleaseRate(
            tcmalloc::MallocExtension::BytesPerSecond{static_cast<size_t>(config->BackgroundReleaseRate)});

        tcmalloc::MallocExtension::SetMaxPerCpuCacheSize(config->MaxPerCpuCacheSize);

        if (config->GuardedSamplingRate) {
            tcmalloc::MallocExtension::SetGuardedSamplingRate(*config->GuardedSamplingRate);
            tcmalloc::MallocExtension::ActivateGuardedSampling();
        }

        Config_.Store(config);

        if (tcmalloc::MallocExtension::NeedsProcessBackgroundActions()) {
            std::call_once(InitAggressiveReleaseThread_, [&] {
                std::thread([&] {
                    ::TThread::SetCurrentThreadName("TCMallocBack");

                    while (true) {
                        auto config = Config_.Acquire();
                        TLimitsAdjuster::Get()->Reconfigure(config);
                        TMemoryLimitHandlerInstaller::Get()->Reconfigure(config->HeapSizeLimit);

                        auto freeBytes = tcmalloc::MallocExtension::GetNumericProperty("tcmalloc.page_heap_free");
                        YT_VERIFY(freeBytes);

                        if (static_cast<i64>(*freeBytes) > TLimitsAdjuster::Get()->GetAggressiveReleaseThreshold()) {

                            YT_LOG_DEBUG("Aggressively releasing memory (FreeBytes: %v, Threshold: %v)",
                                static_cast<i64>(*freeBytes),
                                TLimitsAdjuster::Get()->GetAggressiveReleaseThreshold());

                            tcmalloc::MallocExtension::ReleaseMemoryToSystem(config->AggressiveReleaseSize);
                        }

                        Sleep(config->AggressiveReleasePeriod);
                    }
                }).detach();
            });
        }
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();

    TTCMallocManagerImpl() = default;

    TAtomicIntrusivePtr<TTCMallocConfig> Config_;
    std::once_flag InitAggressiveReleaseThread_;
};

////////////////////////////////////////////////////////////////////////////////

void TTCMallocManager::Configure(const TTCMallocConfigPtr& config)
{
    TTCMallocManagerImpl::Get()->Configure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
