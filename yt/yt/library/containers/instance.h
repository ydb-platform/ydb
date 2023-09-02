#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

using TResourceUsage = THashMap<EStatField, TErrorOr<ui64>>;

const std::vector<EStatField> InstanceStatFields{
    EStatField::CpuUsage,
    EStatField::CpuUserUsage,
    EStatField::CpuSystemUsage,
    EStatField::CpuWait,
    EStatField::CpuThrottled,
    EStatField::ContextSwitches,
    EStatField::ContextSwitchesDelta,
    EStatField::ThreadCount,
    EStatField::CpuLimit,
    EStatField::CpuGuarantee,

    EStatField::Rss,
    EStatField::MappedFile,
    EStatField::MajorPageFaults,
    EStatField::MinorPageFaults,
    EStatField::FileCacheUsage,
    EStatField::AnonMemoryUsage,
    EStatField::AnonMemoryLimit,
    EStatField::MemoryUsage,
    EStatField::MemoryGuarantee,
    EStatField::MemoryLimit,
    EStatField::MaxMemoryUsage,
    EStatField::OomKills,
    EStatField::OomKillsTotal,

    EStatField::IOReadByte,
    EStatField::IOWriteByte,
    EStatField::IOBytesLimit,
    EStatField::IOReadOps,
    EStatField::IOWriteOps,
    EStatField::IOOps,
    EStatField::IOOpsLimit,
    EStatField::IOTotalTime,
    EStatField::IOWaitTime,

    EStatField::NetTxBytes,
    EStatField::NetTxPackets,
    EStatField::NetTxDrops,
    EStatField::NetTxLimit,
    EStatField::NetRxBytes,
    EStatField::NetRxPackets,
    EStatField::NetRxDrops,
    EStatField::NetRxLimit,
};

struct TResourceLimits
{
    double CpuLimit;
    double CpuGuarantee;
    i64 Memory;
};

////////////////////////////////////////////////////////////////////////////////

struct IInstanceLauncher
    : public TRefCounted
{
    virtual bool HasRoot() const = 0;
    virtual const TString& GetName() const = 0;

    virtual void SetStdIn(const TString& inputPath) = 0;
    virtual void SetStdOut(const TString& outPath) = 0;
    virtual void SetStdErr(const TString& errorPath) = 0;
    virtual void SetCwd(const TString& pwd) = 0;

    // Null core dump handler implies disabled core dumps.
    virtual void SetCoreDumpHandler(const std::optional<TString>& handler) = 0;
    virtual void SetRoot(const TRootFS& rootFS) = 0;

    virtual void SetThreadLimit(i64 threadLimit) = 0;
    virtual void SetDevices(const std::vector<TDevice>& devices) = 0;

    virtual void SetEnablePorto(EEnablePorto enablePorto) = 0;
    virtual void SetIsolate(bool isolate) = 0;
    virtual void EnableMemoryTracking() = 0;
    virtual void SetGroup(int groupId) = 0;
    virtual void SetUser(const TString& user) = 0;
    virtual void SetIPAddresses(
        const std::vector<NNet::TIP6Address>& addresses,
        bool enableNat64 = false) = 0;
    virtual void DisableNetwork() = 0;
    virtual void SetHostName(const TString& hostName) = 0;

    virtual TFuture<IInstancePtr> Launch(
        const TString& path,
        const std::vector<TString>& args,
        const THashMap<TString, TString>& env) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstanceLauncher)

#ifdef _linux_
IInstanceLauncherPtr CreatePortoInstanceLauncher(const TString& name, IPortoExecutorPtr executor);
#endif

////////////////////////////////////////////////////////////////////////////////

struct IInstance
    : public TRefCounted
{
    virtual void Kill(int signal) = 0;
    virtual void Stop() = 0;
    virtual void Destroy() = 0;

    virtual TResourceUsage GetResourceUsage(
        const std::vector<EStatField>& fields = InstanceStatFields) const = 0;
    virtual TResourceLimits GetResourceLimits() const = 0;
    virtual void SetCpuGuarantee(double cores) = 0;
    virtual void SetCpuLimit(double cores) = 0;
    virtual void SetCpuWeight(double weight) = 0;
    virtual void SetIOWeight(double weight) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual void SetMemoryGuarantee(i64 memoryGuarantee) = 0;

    virtual TString GetStderr() const = 0;

    virtual TString GetName() const = 0;
    virtual std::optional<TString> GetParentName() const = 0;
    virtual std::optional<TString> GetRootName() const = 0;

    //! Returns externally visible pid of the root proccess inside container.
    //! Throws if container is not running.
    virtual pid_t GetPid() const = 0;
    //! Returns the list of externally visible pids of processes running inside container.
    virtual std::vector<pid_t> GetPids() const = 0;

    virtual i64 GetMajorPageFaultCount() const = 0;

    //! Future is set when container reaches terminal state (stopped or dead).
    //! Resulting error is OK iff container exited with code 0.
    virtual TFuture<void> Wait() = 0;
};

DEFINE_REFCOUNTED_TYPE(IInstance)

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_
TString GetSelfContainerName(const IPortoExecutorPtr& executor);

IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor);
IInstancePtr GetRootPortoInstance(IPortoExecutorPtr executor);
IInstancePtr GetPortoInstance(IPortoExecutorPtr executor, const TString& name);

//! Works only in Yandex.Deploy pod environment where env DEPLOY_VCPU_LIMIT is set.
//! Throws if this env is absent.
double GetSelfPortoInstanceVCpuFactor();
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
