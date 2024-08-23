#include "fs.h"
#include "finally.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/yt/system/handle_eintr.h>
#include <library/cpp/yt/system/exit.h>

#include <util/folder/dirut.h>
#include <util/folder/iterator.h>
#include <util/folder/filelist.h>
#include <util/string/split.h>
#include <util/system/shellcommand.h>

#include <array>

#if defined(_unix_)
    #include <sys/mount.h>
    #include <sys/stat.h>
    #include <fcntl.h>
#endif

#if defined(_linux_)
    #include <mntent.h>
    #include <sys/vfs.h>
    #include <sys/quota.h>
    #include <sys/types.h>
    #include <sys/sendfile.h>
#elif defined(_freebsd_) || defined(_darwin_)
    #include <sys/param.h>
    #include <sys/mount.h>
#elif defined (_win_)
    #include <comutil.h>
    #include <shlobj.h>
#endif

namespace NYT::NFS {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "FS");

////////////////////////////////////////////////////////////////////////////////

namespace {

[[maybe_unused]] void ThrowNotSupported()
{
    THROW_ERROR_EXCEPTION("Unsupported platform");
}

} // namespace

bool Exists(const TString& path)
{
#ifdef _win32_
    return GetFileAttributesA(path.data()) != 0xFFFFFFFF;
#else
    return access(path.data(), F_OK) == 0;
#endif
}

bool IsDirEmpty(const TString& path)
{
    if (!IsDir(path)) {
        THROW_ERROR_EXCEPTION("%v is not a directory",
            path);
    }

    TDirIterator dir(path, TDirIterator::TOptions(FTS_NOSTAT));
    for (auto it = dir.begin(); it != dir.end(); ++it) {
        switch (it->fts_info) {
            case FTS_F:
            case FTS_DEFAULT:
            case FTS_DP:
            case FTS_SL:
            case FTS_SLNONE:
                if (it->fts_level > 0) {
                    return false;
                }
        }
    }
    return true;
}

void Remove(const TString& path)
{
    bool ok;
#ifdef _win_
    ok = DeleteFileA(path.data());
#else
    struct stat sb;
    ok = lstat(path.data(), &sb) == 0;
    if (ok) {
        if (S_ISDIR(sb.st_mode)) {
            ok = rmdir(path.data()) == 0;
        } else {
            ok = remove(path.data()) == 0;
        }
    }
#endif
    if (!ok) {
        THROW_ERROR_EXCEPTION("Cannot remove %v",
            path)
            << TError::FromSystem();
    }
}

void Replace(const TString& source, const TString& destination)
{
    if (NFS::Exists(destination)) {
        NFS::Remove(destination);
    }
    NFS::Rename(source, destination);
}

void RemoveRecursive(const TString& path)
{
    RemoveDirWithContents(path);
}

void Rename(const TString& source, const TString& destination)
{
    bool ok;
#if defined(_win_)
    ok = MoveFileEx(source.data(), destination.data(), MOVEFILE_REPLACE_EXISTING) != 0;
#else
    ok = rename(source.data(), destination.data()) == 0;
#endif
    if (!ok) {
        THROW_ERROR_EXCEPTION("Cannot rename %v to %v",
            source,
            destination)
            << TError::FromSystem();
    }
}

TString GetFileName(const TString& path)
{
    size_t slashPosition = path.find_last_of(LOCSLASH_C);
    if (slashPosition == TString::npos) {
        return path;
    }
    return path.substr(slashPosition + 1);
}

TString GetDirectoryName(const TString& path)
{
    auto absPath = CombinePaths(NFs::CurrentWorkingDirectory(), path);
    size_t slashPosition = absPath.find_last_of(LOCSLASH_C);
    if (slashPosition == 0) {
        // Root.
        return TString(1, LOCSLASH_C);
    } else {
        return absPath.substr(0, slashPosition);
    }
}

TString GetRealPath(const TString& path)
{
    auto curPath = CombinePaths(NFs::CurrentWorkingDirectory(), path);
    std::vector<TString> parts;
    while (!Exists(curPath)) {
        auto filename = GetFileName(curPath);
        if (filename == ".") {
            // Do nothing.
        } else if (filename == ".." || parts.empty() || parts.back() != "..") {
            parts.push_back(filename);
        } else {
            parts.pop_back();
        }
        curPath = GetDirectoryName(curPath);
        if (curPath.empty()) {
            break;
        }
    }
    if (!curPath.empty()) {
        parts.push_back(RealPath(curPath));
    } else {
        parts.push_back(LOCSLASH_S);
    }

    Reverse(parts.begin(), parts.end());
    return CombinePaths(parts);
}

bool IsPathRelativeAndInvolvesNoTraversal(const TString& path)
{
    auto normalizedPath = NormalizePathSeparators(path);
    if (normalizedPath.StartsWith(LOCSLASH_C)) {
        return false;
    }

    TStringBuf currentPath(normalizedPath);
    int depth = 0;
    while (!currentPath.empty()) {
        size_t slashPosition = currentPath.find_first_of(LOCSLASH_C);
        if (slashPosition == 0) {
            currentPath = currentPath.substr(1);
            continue;
        }
        auto part = slashPosition == TString::npos ? currentPath : currentPath.substr(0, slashPosition);
        if (part == "..") {
            --depth;
            if (depth < 0) {
                return false;
            }
        } else if (normalizedPath == ".") {
            // Do nothing.
        } else {
            ++depth;
        }
        if (slashPosition == TString::npos) {
            break;
        }
        currentPath = currentPath.substr(slashPosition + 1);
    }
    return true;
}

TString GetFileExtension(const TString& path)
{
    size_t dotPosition = path.find_last_of('.');
    if (dotPosition == TString::npos) {
        return "";
    }
    size_t slashPosition = path.find_last_of(LOCSLASH_C);
    if (slashPosition != TString::npos && dotPosition < slashPosition) {
        return "";
    }
    return path.substr(dotPosition + 1);
}

TString GetFileNameWithoutExtension(const TString& path)
{
    auto fileName = GetFileName(path);
    size_t dotPosition = fileName.find_last_of('.');
    if (dotPosition == TString::npos) {
        return fileName;
    }
    return fileName.substr(0, dotPosition);
}

void CleanTempFiles(const TString& path)
{
    YT_LOG_INFO("Cleaning temp files in %v", path);

    // TODO(ignat): specify suffix in EnumerateFiles.
    auto entries = EnumerateFiles(path, std::numeric_limits<int>::max());
    for (const auto& entry : entries) {
        if (entry.EndsWith(TempFileSuffix)) {
            auto fileName = NFS::CombinePaths(path, entry);
            YT_LOG_DEBUG("Removing file (FileName: %v)",
                fileName);
            NFS::Remove(fileName);
        }
    }
}

std::vector<TString> EnumerateFiles(const TString& path, int depth)
{
    std::vector<TString> result;
    if (NFS::Exists(path)) {
        TFileList list;
        list.Fill(path, TStringBuf(), TStringBuf(), depth);
        int size = list.Size();
        for (int i = 0; i < size; ++i) {
            result.push_back(list.Next());
        }
    }
    return result;
}

std::vector<TString> EnumerateDirectories(const TString& path, int depth)
{
    std::vector<TString> result;
    if (NFS::Exists(path)) {
        TDirsList list;
        list.Fill(path, TStringBuf(), TStringBuf(), depth);
        int size = list.Size();
        for (int i = 0; i < size; ++i) {
            result.push_back(list.Next());
        }
    }
    return result;
}

TString GetRelativePath(const TString& from, const TString& to)
{
    std::vector<TString> tokensFrom;
    StringSplitter(GetRealPath(from)).Split(LOCSLASH_C).Collect(&tokensFrom);
    std::vector<TString> tokensTo;
    StringSplitter(GetRealPath(to)).Split(LOCSLASH_C).Collect(&tokensTo);

    int commonPrefixLength = 0;
    while (commonPrefixLength < std::min(std::ssize(tokensFrom), std::ssize(tokensTo)) &&
        tokensFrom[commonPrefixLength] == tokensTo[commonPrefixLength])
    {
        ++commonPrefixLength;
    }

    std::vector<TString> relativePathTokens;
    relativePathTokens.reserve(tokensFrom.size() + tokensTo.size() - 2 * commonPrefixLength);
    for (int index = 0; index < std::ssize(tokensFrom) - commonPrefixLength; ++index) {
        relativePathTokens.push_back("..");
    }
    for (int index = commonPrefixLength; index < std::ssize(tokensTo); ++index) {
        relativePathTokens.push_back(tokensTo[index]);
    }

    if (relativePathTokens.empty()) {
        return ".";
    }

    return CombinePaths(relativePathTokens);
}

TString GetRelativePath(const TString& path)
{
    return GetRelativePath(NFs::CurrentWorkingDirectory(), path);
}

TString GetShortestPath(const TString& path)
{
    auto absolutePath = GetRealPath(path);
    auto relativePath = GetRelativePath(path);
    if (absolutePath.length() < relativePath.length()) {
        return absolutePath;
    } else {
        return relativePath;
    }
}

TDiskSpaceStatistics GetDiskSpaceStatistics(const TString& path)
{
    TDiskSpaceStatistics result;
    bool ok;
#ifdef _win_
    ok = GetDiskFreeSpaceEx(
        path.data(),
        (PULARGE_INTEGER) &result.AvailableSpace,
        (PULARGE_INTEGER) &result.TotalSpace,
        (PULARGE_INTEGER) &result.FreeSpace) != 0;
#else
    struct statfs fsData;
    ok = statfs(path.data(), &fsData) == 0;
    result.TotalSpace = (i64) fsData.f_blocks * fsData.f_bsize;
    result.AvailableSpace = (i64) fsData.f_bavail * fsData.f_bsize;
    result.FreeSpace = (i64) fsData.f_bfree * fsData.f_bsize;
#endif

    if (!ok) {
        THROW_ERROR_EXCEPTION("Failed to get disk space statistics for %v",
            path)
            << TError::FromSystem();
    }

    return result;
}

void MakeDirRecursive(const TString& path, int mode)
{
    MakePathIfNotExist(path.data(), mode);
}

TPathStatistics GetPathStatistics(const TString& path)
{
#ifdef _unix_
    TPathStatistics statistics;

    struct stat fileStat;
    int result = ::stat(path.data(), &fileStat);

    if (result == -1) {
        THROW_ERROR_EXCEPTION("Failed to get statistics for %v",
            path)
            << TError::FromSystem();
    }

    statistics.Size = static_cast<i64>(fileStat.st_size);
    statistics.ModificationTime = TInstant::Seconds(fileStat.st_mtime);
    statistics.AccessTime = TInstant::Seconds(fileStat.st_atime);
    statistics.INode = fileStat.st_ino;
    statistics.DeviceId = fileStat.st_dev;

    return statistics;
#else
    ThrowNotSupported();
    // Suppress clang's error about reaching end of non-void function without return.
    Y_UNREACHABLE();
#endif
}

i64 GetDirectorySize(const TString& path, bool ignoreUnavailableFiles, bool deduplicateByINodes, bool checkDeviceId)
{
    auto wrapNoEntryError = [&] (std::function<void()> func) {
        try {
            func();
        } catch (const TSystemError& ex) { // For util functions.
            if (ignoreUnavailableFiles && ex.Status() == ENOENT) {
                // Do nothing
            } else {
                throw;
            }
        } catch (const TErrorException& ex) { // For YT functions.
            if (ignoreUnavailableFiles && ex.Error().FindMatching(ELinuxErrorCode::NOENT)) {
                // Do nothing
            } else {
                throw;
            }
        }
    };

    std::queue<TString> directories;
    directories.push(path);

    TPathStatistics rootDirStatistics;
    wrapNoEntryError([&] {
        rootDirStatistics = GetPathStatistics(path);
    });

    THashSet<ui64> visitedInodes;

    i64 size = 0;


    while (!directories.empty()) {
        const auto& directory = directories.front();

        wrapNoEntryError([&] {
            auto subdirectories = EnumerateDirectories(directory);
            for (const auto& subdirectory : subdirectories) {
                directories.push(CombinePaths(directory, subdirectory));
            }
        });

        std::vector<TString> files;
        wrapNoEntryError([&] {
            files = EnumerateFiles(directory);
        });

        for (const auto& file : files) {
            wrapNoEntryError([&] {
                auto fileStatistics = GetPathStatistics(CombinePaths(directory, file));
                if (deduplicateByINodes) {
                    auto insertResult = visitedInodes.insert(fileStatistics.INode);
                    if (!insertResult.second) { // File already visited
                        return;
                    }
                }
                if (checkDeviceId && fileStatistics.DeviceId != rootDirStatistics.DeviceId) {
                    return;
                }
                if (fileStatistics.Size > 0) {
                    size += fileStatistics.Size;
                }
            });
        }

        directories.pop();
    }

    return size;
}

void Touch(const TString& path)
{
#ifdef _unix_
    int result = ::utimes(path.data(), nullptr);
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to touch %v",
            path)
            << TError::FromSystem();
    }
#else
    ThrowNotSupported();
#endif
}

namespace {

#ifdef _win_
    const char PATH_DELIM = '\\';
    const char PATH_DELIM2 = '/';
#else
    const char PATH_DELIM = '/';
    const char PATH_DELIM2 = 0;
#endif

bool IsAbsolutePath(const TString& path)
{
    if (path.empty())
        return false;
    if (path[0] == PATH_DELIM)
        return true;
#ifdef _win_
    if (path[0] == PATH_DELIM2)
        return true;
    if (path[0] > 0 && isalpha(path[0]) && path[1] == ':')
        return true;
#endif
    return false;
}

} // namespace

TString CombinePaths(const TString& path1, const TString& path2)
{
    return IsAbsolutePath(path2) ? NormalizePathSeparators(path2) : JoinPaths(path1, path2);
}

TString CombinePaths(const std::vector<TString>& paths)
{
    YT_VERIFY(!paths.empty());
    if (paths.size() == 1) {
        return paths[0];
    }
    auto result = CombinePaths(paths[0], paths[1]);
    for (int index = 2; index < std::ssize(paths); ++index) {
        result = CombinePaths(result, paths[index]);
    }
    return result;
}

TString JoinPaths(const TString& path1, const TString& path2)
{
    if (path1.empty())
        return path2;
    if (path2.empty())
        return path1;

    auto path = path1;
    int delim = 0;
    if (path1.back() == PATH_DELIM || path1.back() == PATH_DELIM2)
        ++delim;
    if (path2[0] == PATH_DELIM || path2[0] == PATH_DELIM2)
        ++delim;
    if (delim == 0)
        path.append(1, PATH_DELIM);
    path.append(path2, delim == 2 ? 1 : 0, TString::npos);
    return NormalizePathSeparators(path);
}

TString NormalizePathSeparators(const TString& path)
{
#ifdef _unix_
    constexpr char platformPathSeparator = '/';
    constexpr char foreignPathSeparator = '\\';
#else
    constexpr char platformPathSeparator = '\\';
    constexpr char foreignPathSeparator = '/';
#endif
    TString result;
    result.reserve(path.length());
    for (int i = 0; i < std::ssize(path); ++i) {
        if (path[i] == foreignPathSeparator) {
            result.append(platformPathSeparator);
        } else {
            result.append(path[i]);
        }
    }
    return result;
}

void SetPermissions(const TString& path, int permissions)
{
#ifdef _linux_
    auto res = HandleEintr(::chmod, path.data(), permissions);
    if (res == -1) {
        THROW_ERROR_EXCEPTION("Failed to set permissions for descriptor")
            << TErrorAttribute("path", path)
            << TErrorAttribute("permissions", permissions)
            << TError::FromSystem();
    }
#else
    Y_UNUSED(path, permissions);
#endif
}

void SetPermissions(int fd, int permissions)
{
    const auto& procPath = Format("/proc/self/fd/%v", fd);
    SetPermissions(procPath, permissions);
}

void MakeSymbolicLink(const TString& filePath, const TString& linkPath)
{
#ifdef _win_
    // From MSDN: If the function succeeds, the return value is nonzero.
    // If the function fails, the return value is zero. To get extended error information, call GetLastError.
    bool ok = CreateSymbolicLink(linkPath.data(), filePath.data(), 0) != 0;
#else
    bool ok = symlink(filePath.data(), linkPath.data()) == 0;
#endif

    if (!ok) {
        THROW_ERROR_EXCEPTION(
            "Failed to link %v to %v",
            filePath,
            linkPath)
            << TError::FromSystem();
    }
}

bool AreInodesIdentical(const TString& lhsPath, const TString& rhsPath)
{
#ifdef _unix_
    auto checkedStat = [] (const TString& path, struct stat* buffer) {
        auto result = stat(path.data(), buffer);
        if (result) {
            THROW_ERROR_EXCEPTION(
                "Failed to check for identical inodes: stat failed for %v",
                path)
                << TError::FromSystem();
        }
    };

    struct stat lhsBuffer, rhsBuffer;
    checkedStat(lhsPath, &lhsBuffer);
    checkedStat(rhsPath, &rhsBuffer);

    return
        lhsBuffer.st_dev == rhsBuffer.st_dev &&
        lhsBuffer.st_ino == rhsBuffer.st_ino;
#else
    return false;
#endif
}

TString GetHomePath()
{
#ifdef _win_
    std::array<char, 1024> buffer;
    SHGetSpecialFolderPath(0, buffer.data(), CSIDL_PROFILE, 0);
    return TString(buffer.data());
#else
    return std::getenv("HOME");
#endif
}

void FlushDirectory(const TString& path)
{
#ifdef _unix_
    int fd = ::open(path.data(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        THROW_ERROR_EXCEPTION("Failed to open directory %v", path)
            << TError::FromSystem();
    }

    int result = ::fsync(fd);
    if (result < 0) {
        SafeClose(fd, false);
        THROW_ERROR_EXCEPTION("Failed to flush directory %v", path)
            << TError::FromSystem();
    }

    SafeClose(fd, false);
#else
    // No-op.
#endif
}

std::vector<TMountPoint> GetMountPoints(const TString& mountsFile)
{
#ifdef _linux_
    std::unique_ptr<FILE, decltype(&endmntent)> file(::setmntent(mountsFile.data(), "r"), endmntent);

    if (!file.get()) {
        THROW_ERROR_EXCEPTION("Failed to open mounts file %v", mountsFile);
    }

    std::vector<TMountPoint> mountPoints;

    ::mntent* entry;
    while ((entry = getmntent(file.get()))) {
        TMountPoint point;
        point.Name = entry->mnt_fsname;
        point.Path = entry->mnt_dir;
        mountPoints.push_back(point);
    }

    return mountPoints;
#else
    Y_UNUSED(mountsFile);
    ThrowNotSupported();
    YT_ABORT();
#endif
}

void MountTmpfs(const TString& path, int userId, i64 size)
{
#ifdef _linux_
    auto opts = Format("mode=0777,uid=%v,size=%v", userId, size);
    int result = ::mount("none", path.data(), "tmpfs", 0, opts.data());
    if (result < 0) {
        THROW_ERROR_EXCEPTION("Failed to mount tmpfs at %v", path)
            << TErrorAttribute("user_id", userId)
            << TErrorAttribute("size", size)
            << TError::FromSystem();
    }
#else
    Y_UNUSED(path, userId, size);
    ThrowNotSupported();
    YT_ABORT();
#endif
}

void Umount(const TString& path, bool detach)
{
#ifdef _linux_
    int flags = 0;
    if (detach) {
        flags |= MNT_DETACH;
    }
    int result = ::umount2(path.data(), flags);
    // EINVAL for ::umount means that nothing mounted at this point.
    // ENOENT means 'No such file or directory'.
    if (result < 0 && LastSystemError() != EINVAL && LastSystemError() != ENOENT) {
        auto error = TError("Failed to umount %v", path)
            << TError::FromSystem();
        if (LastSystemError() == EBUSY) {
            error = AttachLsofOutput(error, path);
            error = AttachFindOutput(error, path);
        }
        THROW_ERROR error;
    }

#else
    Y_UNUSED(path, detach);
    ThrowNotSupported();
#endif
}

struct stat Stat(TStringBuf path)
{
    struct stat statInfo;
    int result = ::stat(path.data(), &statInfo);
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to execute ::stat for %v", path)
            << TError::FromSystem();
    }
    return statInfo;
}

i64 GetBlockSize(TStringBuf device)
{
#ifdef _unix_
    struct stat statInfo = Stat(device);
    return static_cast<i64>(statInfo.st_blksize);
#else
    ThrowNotSupported();
    Y_UNREACHABLE();
#endif
}

TString GetFilesystemName(TStringBuf path)
{
    struct stat statInfo = Stat(path);
    auto dev = statInfo.st_dev;

    for (const auto& mountPoint : GetMountPoints()) {
        struct stat currentStatInfo;
        if (::stat(mountPoint.Path.c_str(), &currentStatInfo) != 0) {
            continue;
        }

        if (currentStatInfo.st_dev == dev) {
            return mountPoint.Name;
        }
    }

    THROW_ERROR_EXCEPTION("Failed to find mount point for %v", path);
}

void SetQuota(
    int userId,
    TStringBuf path,
    std::optional<i64> diskSpaceLimit,
    std::optional<i64> inodeLimit)
{
#ifdef _linux_
    dqblk info;
    const i64 blockSize = GetBlockSize(path);
    const auto filesystem = GetFilesystemName(path);
    u_int32_t flags = 0;
    if (diskSpaceLimit) {
        const auto diskSpaceLimitValue = (*diskSpaceLimit + blockSize - 1) / blockSize;
        info.dqb_bhardlimit = static_cast<u_int64_t>(diskSpaceLimitValue);
        info.dqb_bsoftlimit = info.dqb_bhardlimit;
        flags |= QIF_BLIMITS;
    }
    if (inodeLimit) {
        info.dqb_ihardlimit = static_cast<u_int64_t>(*inodeLimit);
        info.dqb_isoftlimit = info.dqb_ihardlimit;
        flags |= QIF_ILIMITS;
    }
    info.dqb_valid = flags;
    int result = ::quotactl(
        QCMD(Q_SETQUOTA, USRQUOTA),
        filesystem.c_str(),
        userId,
        reinterpret_cast<caddr_t>(&info));
    if (result < 0) {
        THROW_ERROR_EXCEPTION("Failed to set FS quota for user")
            << TErrorAttribute("user_id", userId)
            << TErrorAttribute("disk_space_limit", diskSpaceLimit.value_or(0))
            << TErrorAttribute("inode_limit", inodeLimit.value_or(0))
            << TErrorAttribute("path", path)
            << TError::FromSystem();
    }
#else
    Y_UNUSED(userId, path, diskSpaceLimit, inodeLimit);
    ThrowNotSupported();
#endif
}

void WrapIOErrors(std::function<void()> func)
{
    try {
        func();
    } catch (const TSystemError& ex) {
        auto status = ex.Status();
        switch (status) {
            case ENOMEM:
                fprintf(stderr, "Out-of-memory condition detected during I/O operation; terminating\n");
                AbortProcess(ToUnderlying(EProcessExitCode::OutOfMemory));
                break;

            case EIO:
            case ENOSPC:
            case EROFS:
            case EWOULDBLOCK: // aka EAGAIN
#ifdef _linux_
            case EUCLEAN:
#endif
                THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "I/O error")
                    << TErrorAttribute("status", status)
                    << TError(ex);

            default: {
                TError error(ex);
                YT_LOG_FATAL(error, "Unexpected exception thrown during I/O operation");
                break;
            }
        }
    }
}

void Chmod(const TString& path, int mode)
{
#ifdef _linux_
    int result = ::Chmod(path.data(), mode);
    if (result < 0) {
        THROW_ERROR_EXCEPTION("Failed to change mode of %v", path)
            << TErrorAttribute("mode", Format("%04o", mode))
            << TError::FromSystem();
    }
#else
    Y_UNUSED(path, mode);
    ThrowNotSupported();
#endif
}

void SendfileChunkedCopy(
    const TString& existingPath,
    const TString& newPath,
    i64 chunkSize)
{
#ifdef _linux_
    try {
        TFile src(existingPath, OpenExisting | RdOnly | Seq | CloseOnExec);
        TFile dst(newPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        dst.Flock(LOCK_EX);
        SendfileChunkedCopy(src, dst, chunkSize);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to copy %v to %v",
            existingPath,
            newPath)
            << ex;
    }
#else
    Y_UNUSED(existingPath, newPath, chunkSize);
    ThrowNotSupported();
#endif
}

void SendfileChunkedCopy(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize)
{
#ifdef _linux_
    try {
        i64 srcSize = source.GetLength();
        if (srcSize == -1) {
            THROW_ERROR_EXCEPTION("Cannot get source file length: stat failed for %v",
                destination.GetName())
                << TError::FromSystem();
        }

        int srcFd = source.GetHandle();
        int dstFd = destination.GetHandle();

        while (true) {
            i64 currentChunkSize = 0;
            while (currentChunkSize < chunkSize && srcSize > 0) {
                auto size = sendfile(dstFd, srcFd, nullptr, chunkSize);
                if (size == -1) {
                    THROW_ERROR_EXCEPTION("Error while doing chunked copy: sendfile failed")
                        << TError::FromSystem();
                }
                currentChunkSize += size;
                srcSize -= size;
            }

            if (srcSize == 0) {
                break;
            }

            NConcurrency::Yield();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to copy %v to %v",
            source.GetName(),
            destination.GetName())
            << ex;
    }
#else
    Y_UNUSED(source, destination, chunkSize);
    ThrowNotSupported();
#endif
}

TFuture<void> ReadBuffer(
    int fromFd,
    int toFd,
    std::vector<ui8> buffer,
    int bufferSize)
{
    YT_VERIFY(bufferSize);

    auto readSize = read(fromFd, buffer.data(), bufferSize);

    if (readSize == -1) {
        THROW_ERROR_EXCEPTION("Error while doing read")
            << TError::FromSystem();
    }

    if (readSize == 0) {
        return VoidFuture;
    }

    return BIND(&WriteBuffer)
        .AsyncVia(GetCurrentInvoker())
        .Run(fromFd, toFd, std::move(buffer), bufferSize, readSize);
}

TFuture<void> WriteBuffer(
    int fromFd,
    int toFd,
    std::vector<ui8> buffer,
    int bufferSize,
    int readSize)
{
    YT_VERIFY(readSize);
    YT_VERIFY(bufferSize);

    auto size = write(toFd, buffer.data(), readSize);

    if (size == -1) {
        THROW_ERROR_EXCEPTION("Error while doing write")
            << TError::FromSystem();
    }

    return BIND(&ReadBuffer)
        .AsyncVia(GetCurrentInvoker())
        .Run(fromFd, toFd, std::move(buffer), bufferSize);
}

TFuture<void> ReadWriteCopyAsync(
    const TString& existingPath,
    const TString& newPath,
    i64 chunkSize)
{
#ifdef _linux_
    try {
        TFile src(existingPath, OpenExisting | RdOnly | Seq | CloseOnExec);
        TFile dst(newPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        dst.Flock(LOCK_EX);
        return ReadWriteCopyAsync(src, dst, chunkSize);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to copy %v to %v",
            existingPath,
            newPath)
            << ex;
    }
#else
    Y_UNUSED(existingPath, newPath, chunkSize);
    ThrowNotSupported();
    return VoidFuture;
#endif
}

TFuture<void> ReadWriteCopyAsync(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize)
{
#ifdef _linux_
    int srcFd = source.GetHandle();
    int dstFd = destination.GetHandle();
    std::vector<ui8> buffer(chunkSize);

    return ReadBuffer(srcFd, dstFd, std::move(buffer), chunkSize)
        .Apply(BIND([=] (const TErrorOr<void>& result) {
            THROW_ERROR_EXCEPTION_IF_FAILED(result,
                TError("Failed to copy %v to %v",
                    source.GetName(),
                    destination.GetName()));
        }));
#else
    Y_UNUSED(source, destination, chunkSize);
    ThrowNotSupported();
    return VoidFuture;
#endif
}

void ReadWriteCopySync(
    const TString& existingPath,
    const TString& newPath,
    i64 chunkSize)
{
#ifdef _linux_
    try {
        TFile src(existingPath, OpenExisting | RdOnly | Seq | CloseOnExec);
        TFile dst(newPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        dst.Flock(LOCK_EX);
        ReadWriteCopySync(src, dst, chunkSize);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to copy %v to %v",
            existingPath,
            newPath)
            << ex;
    }
#else
    Y_UNUSED(existingPath, newPath, chunkSize);
    ThrowNotSupported();
#endif
}

void ReadWriteCopySync(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize)
{
#ifdef _linux_
    int srcFd = source.GetHandle();
    int dstFd = destination.GetHandle();
    std::vector<ui8> buffer(chunkSize);

    while (true) {
        auto readByteCount = read(srcFd, buffer.data(), chunkSize);

        if (readByteCount == -1) {
            THROW_ERROR_EXCEPTION("Error while doing read")
                << TError::FromSystem();
        }

        if (readByteCount == 0) {
            return;
        }

        for (int writtenByteCount = 0; writtenByteCount < readByteCount;) {
            auto byteCount = write(
                dstFd,
                buffer.data() + writtenByteCount,
                readByteCount - writtenByteCount);

            if (byteCount == -1) {
                THROW_ERROR_EXCEPTION("Error while doing write")
                    << TError::FromSystem();
            }

            writtenByteCount += byteCount;
        }
    }
#else
    Y_UNUSED(source, destination, chunkSize);
    ThrowNotSupported();
#endif
}

void Splice(
    const TFile& source,
    const TFile& destination,
    i64 chunkSize)
{
#ifdef _linux_
    try {
        int srcFd = source.GetHandle();
        int dstFd = destination.GetHandle();

        loff_t offset = 0;

        bool completed = false;
        while (!completed) {
            i64 currentChunkSize = 0;
            while (currentChunkSize < chunkSize) {
                auto size = splice(srcFd, nullptr, dstFd, &offset, chunkSize, SPLICE_F_MOVE | SPLICE_F_MORE);
                if (size == -1) {
                    THROW_ERROR_EXCEPTION("Error while doing splice")
                        << TErrorAttribute("source_path", source.GetName())
                        << TErrorAttribute("destination_path", destination.GetName())
                        << TError::FromSystem();
                } else if (size == 0) {
                    completed = true;
                    break;
                } else {
                    currentChunkSize += size;
                }
            }

            NConcurrency::Yield();
        }

    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to copy %v to %v via splice",
            source.GetName(),
            destination.GetName())
            << ex;
    }
#else
    Y_UNUSED(source, destination, chunkSize);
    ThrowNotSupported();
#endif
}

TError AttachLsofOutput(TError error, const TString& path)
{
    auto lsofOutput = TShellCommand("lsof", {path})
        .Run()
        .Wait()
        .GetOutput();
    return error
        << TErrorAttribute("lsof_output", lsofOutput);
}

TError AttachFindOutput(TError error, const TString& path)
{
    auto findOutput = TShellCommand("find", {path, "-name", "*"})
        .Run()
        .Wait()
        .GetOutput();
    return error
        << TErrorAttribute("find_output", findOutput);
}

int GetDeviceId(const TString& path)
{
#ifdef _linux_
    return Stat(path).st_dev;
#else
    Y_UNUSED(path);
    YT_UNIMPLEMENTED();
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFS
