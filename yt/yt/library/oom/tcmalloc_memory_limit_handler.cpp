#include "oom.h"

#include <yt/yt/library/ytprof/external_pprof.h>
#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/spinlock_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/yson/writer.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/exit.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/shellcommand.h>

#include <mutex>
#include <thread>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void CollectAndDumpMemoryProfile(const TString& memoryProfilePath, tcmalloc::ProfileType profileType)
{
    auto profile = NYTProf::ReadHeapProfile(profileType);
    SymbolizeByExternalPProf(&profile, NYTProf::TSymbolizationOptions{
        .RunTool = [] (const std::vector<TString>& args) {
            TShellCommand command{args[0], TList<TString>{args.begin()+1, args.end()}};
            command.Run();
        },
    });

    TFileOutput output(memoryProfilePath);
    NYTProf::WriteProfile(&output, profile);
    output.Finish();
}

////////////////////////////////////////////////////////////////////////////////

void MemoryProfileTimeoutHandler(int /*signal*/)
{
    WriteToStderr("*** Process hung during dumping heap profile ***\n");
    AbortProcess(ToUnderlying(EProcessExitCode::GenericError));
}

void SetupMemoryProfileTimeout(int timeout)
{
    ::signal(SIGALRM, &MemoryProfileTimeoutHandler);
    ::alarm(timeout);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TOomProfilePaths)

struct TOomProfilePaths
    : public NYTree::TYsonStruct
{
    TString HeapProfilePath;
    TString PeakProfilePath;

    REGISTER_YSON_STRUCT(TOomProfilePaths);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("heap_profile_path", &TThis::HeapProfilePath)
            .Default();
        registrar.Parameter("peak_profile_path", &TThis::PeakProfilePath)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TOomProfilePaths)

////////////////////////////////////////////////////////////////////////////////

void DumpProfilePaths(const TOomProfilePathsPtr& links, const TString& fileName)
{
    TFileOutput output(fileName);
    NYson::TYsonWriter writer(&output, NYson::EYsonFormat::Pretty);
    Serialize(links, &writer);
    writer.Flush();
    output.Finish();
}

////////////////////////////////////////////////////////////////////////////////

class TTCMallocLimitHandler
    : public TRefCounted
{
public:
    explicit TTCMallocLimitHandler(TTCMallocLimitHandlerOptions options)
        : Options_(options)
    {
        Thread_ = std::thread([this] {
            Handle();
        });
    }

    ~TTCMallocLimitHandler()
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
    const TTCMallocLimitHandlerOptions Options_;

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
        auto profilePaths = New<TOomProfilePaths>();
        auto profilePathsFile = GetProfilePaths(timestamp);
        profilePaths->HeapProfilePath = GetHeapDumpPath(timestamp);
        profilePaths->PeakProfilePath = GetPeakDumpPath(timestamp);

        Cerr << "TTCMallocLimitHandler: Fork process to write heap profile: "
            << profilePaths->HeapProfilePath
            << " peak profile path: " << profilePaths->PeakProfilePath
            << " profiles path file: " << profilePathsFile
            << Endl;

        SetupMemoryProfileTimeout(Options_.Timeout.Seconds());
        auto childPid = fork();

        if (childPid == 0) {
            SetupMemoryProfileTimeout(Options_.Timeout.Seconds());
            CollectAndDumpMemoryProfile(profilePaths->HeapProfilePath, tcmalloc::ProfileType::kHeap);
            CollectAndDumpMemoryProfile(profilePaths->PeakProfilePath, tcmalloc::ProfileType::kPeakHeap);
            DumpProfilePaths(profilePaths, profilePathsFile);

            Cerr << "TTCMallocLimitHandler: Heap profiles are written" << Endl;
            AbortProcess(ToUnderlying(EProcessExitCode::OK));
        }

        if (childPid < 0) {
            Cerr << "TTCMallocLimitHandler: Fork failed: " << LastSystemErrorText() << Endl;
            AbortProcess(ToUnderlying(EProcessExitCode::GenericError));
        }

        ExecWaitForChild(childPid);
        AbortProcess(ToUnderlying(EProcessExitCode::OK));
    }

    TString GetHeapDumpPath(const TString& timestamp) const
    {
        return Format(
            "%v/heap_%v.pb.gz",
            Options_.HeapDumpDirectory,
            timestamp);
    }

    TString GetPeakDumpPath(const TString& timestamp) const
    {
        return Format(
            "%v/peak_%v.pb.gz",
            Options_.HeapDumpDirectory,
            timestamp);
    }

    TString GetProfilePaths(const TString& timestamp) const
    {
        return Format(
            "%v/oom_profile_paths_%v.pb.gz",
            Options_.HeapDumpDirectory,
            timestamp);
    }

    void ExecWaitForChild(int pid)
    {
        Cerr << "TTCMallocLimitHandler: Start waiting for the child" << Endl;

        auto command = Format("while [ -e /proc/%v ]; do sleep 1; done;", pid);
        execl("/bin/bash", "/bin/bash", "-c",  command.c_str(), (void*)nullptr);

        Cerr << "TTCMallocLimitHandler: Failed to switch main process to dummy child waiter: "
            << LastSystemErrorText() << Endl;
    }
};

DEFINE_REFCOUNTED_TYPE(TTCMallocLimitHandler);

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

namespace {

YT_DEFINE_GLOBAL(TAtomicIntrusivePtr<TTCMallocLimitHandler>, LimitHandler);

void HandleTCMallocLimit()
{
    if (auto handler = LimitHandler().Acquire()) {
        handler->Fire();
    }
}

} // namespace

void EnableTCMallocLimitHandler(TTCMallocLimitHandlerOptions options)
{
    {
        if (LimitHandler().Acquire()) {
            return;
        }

        TAtomicIntrusivePtr<TTCMallocLimitHandler>::TRawPtr expected = nullptr;
        LimitHandler().CompareAndSwap(expected, New<TTCMallocLimitHandler>(options));
    }

    SetSoftMemoryLimitHandler<tcmalloc::MallocExtension>(&HandleTCMallocLimit);
}

void DisableTCMallocLimitHandler()
{
    LimitHandler().Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
