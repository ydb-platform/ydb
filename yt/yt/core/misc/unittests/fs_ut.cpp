#include <gtest/gtest.h>

#include <library/cpp/yt/system/handle_eintr.h>
#include <library/cpp/yt/system/proc.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>

#include <util/system/file.h>
#include <util/system/fstat.h>

#include <cstdlib>

#if defined(_linux_) || defined(_darwin_)
    #include <sys/stat.h>
    #include <sys/types.h>
#endif
#ifdef _linux_
    #include <sys/sysmacros.h>
#endif

#ifdef _unix_
    #include <unistd.h>
#endif

namespace NYT::NFS {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_
TEST(TFSDeathTest, GetHomePathEnvironment)
{
    EXPECT_EXIT(
        {
            EXPECT_EQ(::setenv("HOME", "/tmp/test-home", 1), 0);
            EXPECT_EQ(GetHomePath(), "/tmp/test-home");
            EXPECT_EQ(::setenv("HOME", "", 1), 0);
            EXPECT_EQ(GetHomePath(), "");
            EXPECT_EQ(::unsetenv("HOME"), 0);
            EXPECT_THROW_WITH_SUBSTRING(GetHomePath(), "HOME environment variable is not set");
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(TFSTest, SetPermissions)
{
    const TTempDir tempDir;
    std::string path = tempDir.Path().Child("file").GetPath();
    TFile file(TString(path), CreateAlways | WrOnly | CloseOnExec);

    SetPermissions(path, /*permissions*/ 0600);
    EXPECT_EQ(TFileStat(file).Mode & 0777, 0600u);

    SetPermissions(path, /*permissions*/ 0640);
    EXPECT_EQ(TFileStat(file).Mode & 0777, 0640u);

    std::string directory = tempDir.Name();
    SetPermissions(directory, /*permissions*/ 0750);
    EXPECT_EQ(TFileStat(TString(directory)).Mode & 0777, 0750u);
}

TEST(TFSTest, SetPermissionsForFileDescriptor)
{
    const TTempDir tempDir;
    std::string path = tempDir.Path().Child("file").GetPath();
    TFile file(TString(path), CreateAlways | WrOnly | CloseOnExec);

    SetPermissions(file.GetHandle(), /*permissions*/ 0640);
    EXPECT_EQ(TFileStat(file).Mode & 0777, 0640u);

    Remove(path);
    SetPermissions(file.GetHandle(), /*permissions*/ 0600);
    EXPECT_EQ(TFileStat(file).Mode & 0777, 0600u);
}

TEST(TFSTest, SetPermissionsReportsMissingPath)
{
    const TTempDir tempDir;
    std::string path = tempDir.Path().Child("missing").GetPath();

    EXPECT_THROW_WITH_ERROR_CODE(SetPermissions(path, /*permissions*/ 0600), ELinuxErrorCode::NOENT);
}
#endif

#if defined(_linux_) || defined(_darwin_)
TEST(TFSTest, GetDeviceId)
{
    auto dir = TTempDir{};
    auto path = std::string(dir.Name()) + "/file";
    auto file = TFile(path.c_str(), CreateAlways | WrOnly);
    struct stat status{};
    ASSERT_EQ(::stat(path.c_str(), &status), 0);
    auto deviceId = GetDeviceId(path);
    EXPECT_EQ(static_cast<dev_t>(makedev(deviceId.first, deviceId.second)), status.st_dev);
}

TEST(TFSTest, GetDeviceIdMatchesStat)
{
    auto dir = TTempDir{};
    struct stat status{};
    ASSERT_EQ(::stat(dir.Name().c_str(), &status), 0);
    auto deviceId = GetDeviceId(std::string(dir.Name()));
    EXPECT_EQ(static_cast<dev_t>(makedev(deviceId.first, deviceId.second)), status.st_dev);
}

TEST(TFSTest, GetDeviceIdMissingPath)
{
    auto dir = TTempDir{};
    EXPECT_THROW_WITH_SUBSTRING(GetDeviceId(std::string(dir.Name()) + "/missing"), "Failed to execute ::stat");
}
#endif

TEST(TFSTest, GetRealPath)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    auto rootPrefix = cwd.substr(0, cwd.find(LOCSLASH_C));
    EXPECT_EQ(CombinePaths(cwd, "dir"), GetRealPath("dir"));
    EXPECT_EQ(cwd, GetRealPath("dir/.."));
    EXPECT_EQ(CombinePaths(cwd, "dir"), GetRealPath("dir/./a/b/../../."));
    EXPECT_EQ(GetRealPath("/a"), rootPrefix + NormalizePathSeparators("/a"));
    EXPECT_EQ(GetRealPath("/a/b"), rootPrefix + NormalizePathSeparators("/a/b"));
    EXPECT_EQ(GetRealPath("/a/b/c/.././../d/."), rootPrefix + NormalizePathSeparators("/a/d"));
}

TEST(TFSTest, GetDirectoryName)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    EXPECT_EQ(GetDirectoryName("/a/b/c"), NormalizePathSeparators("/a/b"));
    EXPECT_EQ(GetDirectoryName("a/b/c"), NormalizePathSeparators(cwd + "/a/b"));
    EXPECT_EQ(GetDirectoryName("."), NormalizePathSeparators(cwd));
    EXPECT_EQ(GetDirectoryName("/"), NormalizePathSeparators("/"));
    EXPECT_EQ(GetDirectoryName("/a"), NormalizePathSeparators("/"));
}

TEST(TFSTest, IsDirEmpty)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    auto dir = CombinePaths(cwd, "test");
    MakeDirRecursive(dir);
    EXPECT_TRUE(IsDirEmpty(dir));
    MakeDirRecursive(CombinePaths(dir, "nested"));
    EXPECT_FALSE(IsDirEmpty(dir));
    RemoveRecursive(dir);
}

TEST(TFSTest, MakeDirRecursive)
{
    auto dir = CombinePaths(NFs::CurrentWorkingDirectory(), "test");

    for (const auto& path : {
        CombinePaths(dir, ""),
        CombinePaths(dir, "child"),
        CombinePaths(dir, "child"),
        CombinePaths(dir, "a/b/c"),
        CombinePaths(dir, "child/a/b/c"),
    }) {
        EXPECT_NO_THROW(MakeDirRecursive(path));
        EXPECT_TRUE(Exists(path));
    }

    RemoveRecursive(dir);
}

#ifdef _linux_
TEST(TFSTest, MakeDirRecursiveReportsRealError)
{
    if (::geteuid() != 0) {
        auto readOnlyDir = CombinePaths(NFs::CurrentWorkingDirectory(), "read_only");
        MakeDirRecursive(readOnlyDir);
        Chmod(readOnlyDir, 0500);

        for (const auto& path : {
            CombinePaths(readOnlyDir, "child"),
            CombinePaths(readOnlyDir, "a/b/c"),
        }) {
            EXPECT_THROW_WITH_ERROR_CODE(MakeDirRecursive(path), ELinuxErrorCode::ACCESS);
        }

        Chmod(readOnlyDir, 0700);
        RemoveRecursive(readOnlyDir);
    }

    const auto danglingLink = CombinePaths(NFs::CurrentWorkingDirectory(), "dangling");
    MakeSymbolicLink("missing_target", danglingLink);

    // TODO(dann239): Introduce ELinuxErrorCode::EXIST and use it here.
    constexpr auto expectedCode = TErrorCode(LinuxErrorCodeBase + EEXIST);

    for (const auto& path : {
        danglingLink,
        CombinePaths(danglingLink, "a/b/c"),
    }) {
        EXPECT_THROW_WITH_ERROR_CODE(MakeDirRecursive(path), expectedCode);
    }

    Remove(danglingLink);
}
#endif

TEST(TFSTest, IsPathRelativeAndInvolvesNoTraversal)
{
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal(""));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some/file"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("."));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("./some/file"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("./some/./file"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("./some/.."));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/../b"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/./../b"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some/"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some//"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a//b"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/b/"));

    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("/"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("//"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("/some"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal(".."));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("../some"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/../.."));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/../../b"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("./.."));
}

TEST(TFSTest, GetRelativePath)
{
    EXPECT_EQ(GetRelativePath("/", "/"), ".");
    EXPECT_EQ(GetRelativePath("/a", "/"), "..");
    EXPECT_EQ(GetRelativePath("/a/b", "/"), NormalizePathSeparators("../.."));
    EXPECT_EQ(GetRelativePath("/", "/a"), "a");
    EXPECT_EQ(GetRelativePath("/", "/a/b"), NormalizePathSeparators("a/b"));
    EXPECT_EQ(GetRelativePath("/a", "/a/b"), "b");
    EXPECT_EQ(GetRelativePath("/a/b", "/a"), "..");
    EXPECT_EQ(GetRelativePath("/a/b/c", "/d/e"), NormalizePathSeparators("../../../d/e"));
    EXPECT_EQ(GetRelativePath("/a/b/c/d", "/a/b"), NormalizePathSeparators("../.."));
    EXPECT_EQ(GetRelativePath("/a/b/c/d/e", "/a/b/c/f/g/h"), NormalizePathSeparators("../../f/g/h"));
    EXPECT_EQ(GetRelativePath("a/b/c", "d/e"), NormalizePathSeparators("../../../d/e"));
    EXPECT_EQ(GetRelativePath("/a/b", "/a/b"), ".");

    EXPECT_EQ(GetRelativePath(CombinePaths(NFs::CurrentWorkingDirectory(), "dir")), "dir");
}

#ifdef _unix_
TEST(TFSTest, CombinePathsWithBackslashUnix)
{
    EXPECT_EQ(CombinePaths("/", "path/with/back\\slashed/file"), "/path/with/back\\slashed/file");
}
#endif

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

template <class... Ts>
auto DoSyscall(std::invocable<Ts...> auto syscall, Ts... args)
{
    auto result = HandleEintr(syscall, args...);
    if (result == -1) {
        auto errorText = Format(
            "Syscall failed at with error %qv (errno %v)",
            LastSystemErrorText(),
            LastSystemError());
        YT_ABORT(errorText);
    }
    return result;
}

class TSpliceAsyncTest
    : public ::testing::Test
{
public:
    TSpliceAsyncTest()
        : Poller_(NConcurrency::CreateThreadPoolPoller(1, "test"))
        , Invoker_(Poller_->GetInvoker())
        , Dir_()
        , Filename_(Dir_.Name() + "/file")
        , Filename2_(Dir_.Name() + "/file2")
        , PipeFilename_(Dir_.Name() + "/pipe")
        , PipeFilename2_(Dir_.Name() + "/pipe2")
    {
        DoSyscall(::mkfifo, PipeFilename_.c_str(), 0600);
        DoSyscall(::mkfifo, PipeFilename2_.c_str(), 0600);
    }

protected:
    const NConcurrency::IPollerPtr Poller_;
    const IInvokerPtr Invoker_;
    const TTempDir Dir_;

    const std::string Filename_;
    const std::string Filename2_;

    const std::string PipeFilename_;
    const std::string PipeFilename2_;

    static constexpr i64 DataSize = 1_MB;

    static TFile OpenPipe(const std::string& name, int flags)
    {
        auto pipe = TFile(DoSyscall(::open, name.c_str(), flags | O_NONBLOCK));
        DoSyscall(::fcntl, pipe.GetHandle(), F_SETFL, flags);
        return pipe;
    }

    static std::vector<ui8> MakeRandomData()
    {
        auto data = std::vector<ui8>(DataSize);
        for (ui8& elem : data) {
            elem = RandomNumber<ui8>();
        }
        return data;
    }

    static void WriteToFile(const std::string& filename, const std::vector<ui8>& data)
    {
        TFile(filename.c_str(), WrOnly | CreateAlways | CloseOnExec).Write(data.data(), data.size());
    }

    static void ReadExpectedByte(TFile& file, ui8 expected)
    {
        ui8 actual = {};
        auto count = file.Read(&actual, 1);
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(actual, expected);
    }

    static void ReadExpectedFile(const std::string& filename, const std::vector<ui8>& expected)
    {
        auto file = TFile(filename.c_str(), RdOnly);
        auto result = std::vector<ui8>(expected.size());
        EXPECT_EQ(file.Read(result.data(), result.size()), expected.size());
        EXPECT_EQ(file.Read(result.data(), result.size()), 0u);
        EXPECT_EQ(result, expected);
    }

    static void WaitForSplice(TFuture<TSpliceResult> future)
    {
        auto result = NConcurrency::WaitForFast(future)
            .ValueOrThrow();
        EXPECT_EQ(result.BytesSpliced, DataSize);
        EXPECT_TRUE(result.Error.IsOK());
    }
};

TEST_F(TSpliceAsyncTest, ReadFileSimple)
{
    auto data = MakeRandomData();
    WriteToFile(Filename_, data);

    auto pipe = OpenPipe(PipeFilename_, O_RDONLY);

    auto future = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename_, O_WRONLY),
        Invoker_,
        Poller_);

    for (ui8 elem : data) {
        ReadExpectedByte(pipe, elem);
    }

    WaitForSplice(future);

    ui8 readElem = 0;
    EXPECT_EQ(pipe.Read(&readElem, 1), 0u);
}

TEST_F(TSpliceAsyncTest, ReadFileConcurrent)
{
    auto data = MakeRandomData();
    WriteToFile(Filename_, data);

    auto pipe = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipe2 = OpenPipe(PipeFilename2_, O_RDONLY);

    auto future = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename_, O_WRONLY),
        Invoker_,
        Poller_);

    auto future2 = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename2_, O_WRONLY),
        Invoker_,
        Poller_);

    constexpr i64 chunk = 100_KB;
    i64 i = 0;
    i64 i2 = 0;

    while (i != ssize(data) && i2 != ssize(data)) {
        for (i64 j = 0; j != chunk && i != ssize(data); ++j, ++i) {
            ReadExpectedByte(pipe, data[i]);
        }

        for (i64 j = 0; j != chunk && i2 != ssize(data); ++j, ++i2) {
            ReadExpectedByte(pipe2, data[i2]);
        }
    }

    WaitForSplice(future);
    WaitForSplice(future2);

    ui8 readElem = 0;
    EXPECT_EQ(pipe.Read(&readElem, 1), 0u);
    EXPECT_EQ(pipe2.Read(&readElem, 1), 0u);
}

TEST_F(TSpliceAsyncTest, ReadFileBrokenPipe)
{
    auto data = MakeRandomData();
    WriteToFile(Filename_, data);

    auto pipe = OpenPipe(PipeFilename_, O_RDONLY);

    auto future = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename_, O_WRONLY),
        Invoker_,
        Poller_);

    int bytesBeforeClose = 100_KB;

    for (int i = 0; i < bytesBeforeClose; ++i) {
        ReadExpectedByte(pipe, data[i]);
    }

    pipe.Close();

    auto result = NConcurrency::WaitForFast(future)
        .ValueOrThrow();
    EXPECT_GE(result.BytesSpliced, bytesBeforeClose);
    EXPECT_THROW_WITH_SUBSTRING(result.Error.ThrowOnError(), "Broken pipe");
}

TEST_F(TSpliceAsyncTest, WriteFileSimple)
{
    auto data = MakeRandomData();
    auto pipeRead = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipeWrite = OpenPipe(PipeFilename_, O_WRONLY);

    auto future = SpliceAsync(
        pipeRead,
        TFile(Filename_.c_str(), CreateAlways | WrOnly),
        Invoker_,
        Poller_);

    for (ui8 elem : data) {
        pipeWrite.Write(&elem, 1);
        if (future.IsSet()) {
            YT_ABORT();
        }
    }

    pipeWrite.Close();

    WaitForSplice(future);

    ReadExpectedFile(Filename_, data);
}

TEST_F(TSpliceAsyncTest, WriteFileConcurrent)
{
    auto data = MakeRandomData();
    auto pipeRead = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipeWrite = OpenPipe(PipeFilename_, O_WRONLY);

    auto data2 = MakeRandomData();
    auto pipeRead2 = OpenPipe(PipeFilename2_, O_RDONLY);
    auto pipeWrite2 = OpenPipe(PipeFilename2_, O_WRONLY);

    auto future = SpliceAsync(
        pipeRead,
        TFile(Filename_.c_str(), CreateAlways | WrOnly),
        Invoker_,
        Poller_);

    auto future2 = SpliceAsync(
        pipeRead2,
        TFile(Filename2_.c_str(), CreateAlways | WrOnly),
        Invoker_,
        Poller_);

    constexpr i64 chunk = 100_KB;
    i64 i = 0;
    i64 i2 = 0;

    while (i != ssize(data) && i2 != ssize(data)) {
        for (i64 j = 0; j != chunk && i != ssize(data); ++j, ++i) {
            pipeWrite.Write(&data[i], 1);
        }

        for (i64 j = 0; j != chunk && i2 != ssize(data); ++j, ++i2) {
            pipeWrite2.Write(&data2[i2], 1);
        }
    }

    pipeWrite.Close();
    pipeWrite2.Close();

    WaitForSplice(future);
    WaitForSplice(future2);

    auto result = std::vector<ui8>(data.size());

    ReadExpectedFile(Filename_, data);
    ReadExpectedFile(Filename2_, data2);
}

TEST_F(TSpliceAsyncTest, WriteFileCancelFuture)
{
    auto data = MakeRandomData();
    auto pipeRead = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipeWrite = OpenPipe(PipeFilename_, O_WRONLY);

    auto future = SpliceAsync(
        pipeRead,
        TFile(Filename_.c_str(), CreateAlways | WrOnly),
        Invoker_,
        Poller_);

    int bytesBeforeCancel = 1_KB;
    for (int i = 0; i < bytesBeforeCancel; ++i) {
        pipeWrite.Write(&data[i], 1);
    }

    EXPECT_TRUE(future.Cancel({}));

    auto result = NConcurrency::WaitForFast(future)
        .ValueOrThrow();
    EXPECT_LE(result.BytesSpliced, bytesBeforeCancel);
    EXPECT_THROW_WITH_SUBSTRING(result.Error.ThrowOnError(), "Operation canceled");

    int bytesAfterCancel = 1_KB;
    for (int i = 0; i < bytesAfterCancel; ++i) {
        pipeWrite.Write(&data[i], 1);
    }

    pipeWrite.Close();

    auto buffer = std::vector<ui8>(bytesBeforeCancel + bytesAfterCancel);
    auto remainder = pipeRead.Read(buffer.data(), buffer.size());
    EXPECT_GE(remainder, static_cast<size_t>(bytesAfterCancel));
}

#endif // _linux

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFS
