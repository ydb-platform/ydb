#pragma once

#include "common.h"

#include <yt/yt/core/misc/error.h>

#include <util/system/file.h>

#include <errno.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! NYT::TError::FromSystem adds this value to a system errno. The enum
//! below lists several errno's that are used in our code.
constexpr int LinuxErrorCodeBase = 4200;
constexpr int LinuxErrorCodeCount = 2000;

DEFINE_ENUM(ELinuxErrorCode,
    ((NOENT)              ((LinuxErrorCodeBase + ENOENT)))
    ((IO)                 ((LinuxErrorCodeBase + EIO)))
    ((ACCESS)              ((LinuxErrorCodeBase + EACCES)))
    ((NFILE)              ((LinuxErrorCodeBase + ENFILE)))
    ((MFILE)              ((LinuxErrorCodeBase + EMFILE)))
    ((NOSPC)              ((LinuxErrorCodeBase + ENOSPC)))
    ((PIPE)               ((LinuxErrorCodeBase + EPIPE)))
    ((CONNRESET)          ((LinuxErrorCodeBase + ECONNRESET)))
    ((TIMEDOUT)           ((LinuxErrorCodeBase + ETIMEDOUT)))
    ((CONNREFUSED)        ((LinuxErrorCodeBase + ECONNREFUSED)))
    ((DQUOT)              ((LinuxErrorCodeBase + EDQUOT)))
);

////////////////////////////////////////////////////////////////////////////////

bool IsSystemErrorCode(TErrorCode errorCode);
bool IsSystemError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

#ifdef _win_
    using TFileDescriptor = SOCKET;
    using uid_t = i64;
#else
    using TFileDescriptor = int;
#endif

////////////////////////////////////////////////////////////////////////////////

std::vector<int> ListPids();
std::vector<int> GetPidsByUid(int uid = -1);
std::vector<int> GetPidsUnderParent(int targetPid);
std::optional<int> GetPidByChildNamespacePid(int childNamespacePid);

//! Gets the resident set size of a process.
/*!
   \note If |pid == -1| then self RSS is returned.
 */

struct TMemoryUsage
{
    ui64 Rss;
    ui64 Shared;
};

TMemoryUsage GetProcessMemoryUsage(int pid = -1);

struct TProcessCgroup
{
    int HierarchyId;
    TString ControllersName;
    std::vector<TString> Controllers;
    TString Path;
};

std::vector<TProcessCgroup> GetProcessCgroups(int pid = -1);

struct TCgroupCpuStat
{
    ui64 NrPeriods = 0;
    ui64 NrThrottled = 0;
    ui64 ThrottledTime = 0;
    ui64 WaitTime = 0;
};

TCgroupCpuStat GetCgroupCpuStat(
    const TString& controllerName,
    const TString& cgroupPath,
    const TString& cgroupMountPoint = "/sys/fs/cgroup");

struct TCgroupMemoryStat
{
    ui64 HierarchicalMemoryLimit = 0;

    ui64 Cache = 0;
    ui64 Rss = 0;
    ui64 RssHuge = 0;
    ui64 MappedFile = 0;
    ui64 Dirty = 0;
    ui64 Writeback = 0;
};

TCgroupMemoryStat GetCgroupMemoryStat(
    const TString& cgroupPath,
    const TString& cgroupMountPoint = "/sys/fs/cgroup");

THashMap<TString, i64> GetVmstat();

ui64 GetProcessCumulativeMajorPageFaults(int pid = -1);
size_t GetCurrentProcessId();
size_t GetCurrentThreadId();
std::vector<size_t> GetCurrentProcessThreadIds();
bool IsUserspaceThread(size_t tid);

void ChownChmodDirectory(
    const TString& path,
    const std::optional<uid_t>& userId,
    const std::optional<int>& permissions);

void ChownChmodDirectoriesRecursively(
    const TString& path,
    const std::optional<uid_t>& userId,
    const std::optional<int>& permissions);

void SetThreadPriority(int tid, int priority);

TString GetProcessName(int pid);
std::vector<TString> GetProcessCommandLine(int pid);

TError StatusToError(int status);

#ifdef _unix_
TError ProcessInfoToError(const siginfo_t& processInfo);
#endif

bool TryClose(TFileDescriptor fd, bool ignoreBadFD = true);
void SafeClose(TFileDescriptor fd, bool ignoreBadFD = true);

bool TryDup2(TFileDescriptor oldFD, TFileDescriptor newFD);
void SafeDup2(TFileDescriptor oldFD, TFileDescriptor newFD);

void SafeSetCloexec(TFileDescriptor fd);

bool TryExecve(const char* path, const char* const* argv, const char* const* env);

void SafeCreateStderrFile(TString fileName);

//! Returns a pipe with CLOSE_EXEC flag.
void SafePipe(TFileDescriptor fd[2]);

TFileDescriptor SafeDup(TFileDescriptor fd);

//! Returns a pty with CLOSE_EXEC flag on master channel.
void SafeOpenPty(TFileDescriptor* masterFD, TFileDescriptor* slaveFD, int height, int width);
void SafeLoginTty(TFileDescriptor fd);
void SafeSetTtyWindowSize(TFileDescriptor slaveFD, int height, int width);

bool TryMakeNonblocking(TFileDescriptor fd);
void SafeMakeNonblocking(TFileDescriptor fd);

bool TrySetPipeCapacity(TFileDescriptor fd, int capacity);
void SafeSetPipeCapacity(TFileDescriptor fd, int capacity);

bool TrySetUid(int uid);
void SafeSetUid(int uid);

TString SafeGetUsernameByUid(int uid);

void SetUid(int uid);

void CloseAllDescriptors(const std::vector<int>& exceptFor = std::vector<int>());

int GetFileDescriptorCount();

//! Return true iff ytserver was started with root permissions (e.g. via sudo or with suid bit).
bool HasRootPermissions();

struct TNetworkInterfaceStatistics
{
    struct TReceiveStatistics
    {
        ui64 Bytes = 0;
        ui64 Packets = 0;
        ui64 Errs = 0;
        ui64 Drop = 0;
        ui64 Fifo = 0;
        ui64 Frame = 0;
        ui64 Compressed = 0;
        ui64 Multicast = 0;
    };
    struct TTransmitStatistics
    {
        ui64 Bytes = 0;
        ui64 Packets = 0;
        ui64 Errs = 0;
        ui64 Drop = 0;
        ui64 Fifo = 0;
        ui64 Colls = 0;
        ui64 Carrier = 0;
        ui64 Compressed = 0;
    };

    TReceiveStatistics Rx;
    TTransmitStatistics Tx;
};

using TNetworkInterfaceStatisticsMap = THashMap<TString, TNetworkInterfaceStatistics>;
//! Returns a mapping from interface name to network statistics.
TNetworkInterfaceStatisticsMap GetNetworkInterfaceStatistics();

void SendSignal(const std::vector<int>& pids, const TString& signalName);
std::optional<int> FindSignalIdBySignalName(const TString& signalName);
void ValidateSignalName(const TString& signalName);

////////////////////////////////////////////////////////////////////////////////

//! The following structures represents content of /proc/[PID]/smaps.
//! Look into 'man 5 /proc' for the description.
struct TMemoryMappingStatistics
{
    ui64 Size = 0;
    ui64 KernelPageSize = 0;
    ui64 MMUPageSize = 0;
    ui64 Rss = 0;
    ui64 Pss = 0;
    ui64 SharedClean = 0;
    ui64 SharedDirty = 0;
    ui64 PrivateClean = 0;
    ui64 PrivateDirty = 0;
    ui64 Referenced = 0;
    ui64 Anonymous = 0;
    ui64 LazyFree = 0;
    ui64 AnonHugePages = 0;
    ui64 ShmemPmdMapped = 0;
    ui64 SharedHugetlb = 0;
    ui64 PrivateHugetlb = 0;
    ui64 Swap = 0;
    ui64 SwapPss = 0;
    ui64 Locked = 0;

    TMemoryMappingStatistics& operator+=(const TMemoryMappingStatistics& rhs);
};

TMemoryMappingStatistics operator+(TMemoryMappingStatistics lhs, const TMemoryMappingStatistics& rhs);

DEFINE_BIT_ENUM(EMemoryMappingPermission,
    ((None)           (0x0000))
    ((Read)           (0x0001))
    ((Write)          (0x0002))
    ((Execute)        (0x0004))
    ((Private)        (0x0008))
    ((Shared)         (0x0010))
);

DEFINE_BIT_ENUM(EVMFlag,
    ((None)            (0x000000000))
    ((RD)              (0x000000001))
    ((WR)              (0x000000002))
    ((EX)              (0x000000004))
    ((SH)              (0x000000008))
    ((MR)              (0x000000010))
    ((MW)              (0x000000020))
    ((ME)              (0x000000040))
    ((MS)              (0x000000080))
    ((GD)              (0x000000100))
    ((PF)              (0x000000200))
    ((DW)              (0x000000400))
    ((LO)              (0x000000800))
    ((IO)              (0x000001000))
    ((SR)              (0x000002000))
    ((RR)              (0x000004000))
    ((DC)              (0x000008000))
    ((DE)              (0x000010000))
    ((AC)              (0x000020000))
    ((NR)              (0x000040000))
    ((HT)              (0x000080000))
    ((NL)              (0x000100000))
    ((AR)              (0x000200000))
    ((DD)              (0x000400000))
    ((SD)              (0x000800000))
    ((MM)              (0x001000000))
    ((HG)              (0x002000000))
    ((NH)              (0x004000000))
    ((MG)              (0x008000000))
);

struct TMemoryMapping
{
    ui64 Start = 0;
    ui64 End = 0;

    EMemoryMappingPermission Permissions = EMemoryMappingPermission::None;

    ui64 Offset = 0;

    std::optional<int> DeviceId;

    std::optional<ui64> INode;

    std::optional<TString> Path;

    TMemoryMappingStatistics Statistics;

    EVMFlag VMFlags = EVMFlag::None;

    ui64 ProtectionKey = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TMemoryMapping> ParseMemoryMappings(const TString& rawSMaps);
std::vector<TMemoryMapping> GetProcessMemoryMappings(int pid);

////////////////////////////////////////////////////////////////////////////////

// See https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
// for fields info.
struct TDiskStat
{
    i32 MajorNumber = 0;
    i32 MinorNumber = 0;
    TString DeviceName;

    i64 ReadsCompleted = 0;
    i64 ReadsMerged = 0;
    i64 SectorsRead = 0;
    TDuration TimeSpentReading;

    i64 WritesCompleted = 0;
    i64 WritesMerged = 0;
    i64 SectorsWritten = 0;
    TDuration TimeSpentWriting;

    i64 IOCurrentlyInProgress = 0;
    TDuration TimeSpentDoingIO;
    TDuration WeightedTimeSpentDoingIO;

    i64 DiscardsCompleted = 0;
    i64 DiscardsMerged = 0;
    i64 SectorsDiscarded = 0;
    TDuration TimeSpentDiscarding;
};

TDiskStat ParseDiskStat(const TString& statLine);

// See https://docs.kernel.org/block/stat.html for more info.
struct TBlockDeviceStat
{
    i64 ReadsCompleted = 0;
    i64 ReadsMerged = 0;
    i64 SectorsRead = 0;
    TDuration TimeSpentReading;

    i64 WritesCompleted = 0;
    i64 WritesMerged = 0;
    i64 SectorsWritten = 0;
    TDuration TimeSpentWriting;

    i64 IOCurrentlyInProgress = 0;
    TDuration TimeSpentDoingIO;
    TDuration WeightedTimeSpentDoingIO;

    i64 DiscardsCompleted = 0;
    i64 DiscardsMerged = 0;
    i64 SectorsDiscarded = 0;
    TDuration TimeSpentDiscarding;

    i64 FlushesCompleted = 0;
    TDuration TimeSpentFlushing;
};

TBlockDeviceStat ParseBlockDeviceStat(const TString& statLine);

//! DeviceName to stat info
THashMap<TString, TDiskStat> GetDiskStats();
std::optional<TBlockDeviceStat> GetBlockDeviceStat(const TString& deviceName);
std::vector<TString> ListDisks();

////////////////////////////////////////////////////////////////////////////////

struct TTaskDiskStatistics
{
    i64 ReadBytes = 0;
    i64 WriteBytes = 0;
};

TTaskDiskStatistics GetSelfThreadTaskDiskStatistics();

////////////////////////////////////////////////////////////////////////////////

TFile MemfdCreate(const TString& name);

////////////////////////////////////////////////////////////////////////////////

const TString& GetLinuxKernelVersion();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

