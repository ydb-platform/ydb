#pragma once

/*!
 * \file fs.h
 * \brief File system functions
 */

#include "common.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/misc/error.h>

#include <util/system/file.h>

namespace NYT::NFS {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((IOError)(19000))
);

////////////////////////////////////////////////////////////////////////////////

//! File suffix for temporary files.
constexpr auto TempFileSuffix = TStringBuf("~");

//! Returns |true| if a given path points to an existing file or directory.
bool Exists(const std::string& path);

//! Returns |true| if a given path to an empty directory.
bool IsDirEmpty(const std::string& path);

//! Removes a given file or directory.
void Remove(const std::string& path);

//! Removes #destination if it exists. Then renames #destination into #source.
void Replace(const std::string& source, const std::string& destination);

//! Removes a given directory recursively.
void RemoveRecursive(const std::string& path);

//! Renames a given file or directory.
void Rename(const std::string& source, const std::string& destination);

//! Returns name of file.
std::string GetFileName(const std::string& path);

//! Returns extension of file.
std::string GetFileExtension(const std::string& path);

//! Returns name of file without extension.
std::string GetFileNameWithoutExtension(const std::string& path);

//! Returns path of directory containing the file.
std::string GetDirectoryName(const std::string& path);

//! Returns the absolute path for the given (possibly relative) path.
std::string GetRealPath(const std::string& path);

//! Checks that given path is relative and points somewhere inside the root directory.
bool IsPathRelativeAndInvolvesNoTraversal(const std::string& path);

//! Combines two strings into a path. Returns second path if it is absolute.
std::string CombinePaths(const std::string& path1, const std::string& path2);

//! Appends second path to the first one, handling delimiters.
std::string JoinPaths(const std::string& path1, const std::string& path2);

//! Combines a bunch of strings into a path.
std::string CombinePaths(const std::vector<std::string>& paths);

//! Deletes all files with extension #TempFileSuffix in a given directory.
void CleanTempFiles(const std::string& path);

//! Returns all files in a given directory.
std::vector<std::string> EnumerateFiles(const std::string& path, int depth = 1, bool sortByName = false);

//! Returns all directories in a given directory.
std::vector<std::string> EnumerateDirectories(const std::string& path, int depth = 1);

//! Returns path to `to` relative to `from`.
std::string GetRelativePath(const std::string& from, const std::string& to);

//! Returns path to `path` relative to working directory.
std::string GetRelativePath(const std::string& path);

//! Returns the shortest among absolute and relative to working directory path to `path`.
std::string GetShortestPath(const std::string& path);

//! Describes total, free, and available space on a disk drive.
struct TDiskSpaceStatistics
{
    i64 TotalSpace;
    i64 FreeSpace;
    i64 AvailableSpace;
};

//! Computes the space statistics for disk drive containing #path.
TDiskSpaceStatistics GetDiskSpaceStatistics(const std::string& path);

//! Creates the #path and parent directories if they don't exists.
void MakeDirRecursive(const std::string& path, int mode = 0777);

constexpr ui32 UnnamedDeviceMajor = 0;

//! Device major:minor pair.
using TDeviceId = std::pair<ui32, ui32>;

struct TPathStatistics
{
    i64 Size = -1;
    ui64 INode;
    TDeviceId DeviceId;
    TInstant ModificationTime;
    TInstant AccessTime;
};

//! Returns the path statistics.
TPathStatistics GetPathStatistics(const std::string& path);

//! Recursively calculates size of all regular files inside the directory.
i64 GetDirectorySize(
    const std::string& path,
    bool ignoreUnavailableFiles = true,
    bool deduplicateByINodes = false,
    bool checkDeviceId = false);

//! Sets the access and modification times to now.
void Touch(const std::string& path);

//! Converts all path separators to platform path separators.
std::string NormalizePathSeparators(const std::string& path);

//! Sets permissions for a file.
void SetPermissions(const std::string& path, int permissions);

//! Sets permissions for an fd.
void SetPermissions(int fd, int permissions);

//! Makes a symbolic link on file #fileName with #linkName.
void MakeSymbolicLink(const std::string& filePath, const std::string& linkPath);

//! Returns |true| if given paths refer to the same inode.
//! Always returns |false| under Windows.
bool AreInodesIdentical(const std::string& lhsPath, const std::string& rhsPath);

//! Returns the home directory of the current user.
//! Interestingly, implemented for both Windows and *nix.
std::string GetHomePath();

//! Flushes the directory's metadata. Useful for, e.g., committing renames happened in #path.
void FlushDirectory(const std::string& path);

struct TMountPoint
{
    std::string Name;
    std::string Path;
};

std::vector<TMountPoint> GetMountPoints(const std::string& mountsFile = "/proc/mounts");

//! Mount tmpfs at given path.
void MountTmpfs(const std::string& path, int userId, i64 size);

//! Unmount given path.
void Umount(const std::string& path, bool detach);

//! Set disk space and inodes quota for given user on filesystem determined by pathInFs.
//! The filesystem must be mounted with quotas enabled.
void SetQuota(
    int userId,
    const std::string& path,
    std::optional<i64> diskSpaceLimit,
    std::optional<i64> inodeLimit);

//! Wraps a given #func in with try/catch; makes sure that only IO-related
//! exceptions are being thrown. For all other exceptions, immediately terminates
//! with fatal error.
void WrapIOErrors(std::function<void()> func);

//! Sets a given mode on the path.
void Chmod(const std::string& path, int mode);

//! Copies file chunk after chunk, releasing thread between chunks.
void SendfileChunkedCopy(
    const std::string& existingPath,
    const std::string& newPath,
    i64 chunkSize);

void SendfileChunkedCopy(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize);

TFuture<void> ReadBuffer(
    int fromFd,
    int toFd,
    std::vector<ui8> buffer,
    int bufferSize);

TFuture<void> WriteBuffer(
    int fromFd,
    int toFd,
    std::vector<ui8> buffer,
    int bufferSize,
    int readSize);

TFuture<void> ReadWriteCopyAsync(
    const std::string& existingPath,
    const std::string& newPath,
    i64 chunkSize);

TFuture<void> ReadWriteCopyAsync(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize);

void ReadWriteCopySync(
    const std::string& existingPath,
    const std::string& newPath,
    i64 chunkSize);

void ReadWriteCopySync(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize);

//! Copies file chunk after chunk via splice syscall,
//! releasing thread between chunks.
void Splice(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize);

TError AttachLsofOutput(TError error, const std::string& path);
TError AttachFindOutput(TError error, const std::string& path);

//! Returns id of device (major:minor) path belongs to.
TDeviceId GetDeviceId(const std::string& path);

std::optional<std::string> FindBinaryPath(const std::string& binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFS
