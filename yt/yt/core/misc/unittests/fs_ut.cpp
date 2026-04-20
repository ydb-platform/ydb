#include <gtest/gtest.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>


namespace NYT::NFS {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFSTest, TestGetRealPath)
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

TEST(TFSTest, TestGetDirectoryName)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    EXPECT_EQ(GetDirectoryName("/a/b/c"), NormalizePathSeparators("/a/b"));
    EXPECT_EQ(GetDirectoryName("a/b/c"), NormalizePathSeparators(cwd + "/a/b"));
    EXPECT_EQ(GetDirectoryName("."), NormalizePathSeparators(cwd));
    EXPECT_EQ(GetDirectoryName("/"), NormalizePathSeparators("/"));
    EXPECT_EQ(GetDirectoryName("/a"), NormalizePathSeparators("/"));
}

TEST(TFSTest, TestIsDirEmpty)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    auto dir = CombinePaths(cwd, "test");
    MakeDirRecursive(dir);
    EXPECT_TRUE(IsDirEmpty(dir));
    MakeDirRecursive(CombinePaths(dir, "nested"));
    EXPECT_FALSE(IsDirEmpty(dir));
    RemoveRecursive(dir);
}

TEST(TFSTest, TestIsPathRelativeAndInvolvesNoTraversal)
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
}

TEST(TFSTest, TestGetRelativePath)
{
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
TEST(TFSTest, TestCombinePathsWithBackslashUnix)
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
    NConcurrency::IPollerPtr Poller_;
    IInvokerPtr Invoker_;
    TTempDir Dir_;

    std::string Filename_;
    std::string Filename2_;

    std::string PipeFilename_;
    std::string PipeFilename2_;

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
        EXPECT_EQ(count, size_t{1});
        EXPECT_EQ(actual, expected);
    }

    static void ReadExpectedFile(const std::string& filename, const std::vector<ui8>& expected)
    {
        auto file = TFile(filename.c_str(), RdOnly);
        auto result = std::vector<ui8>(expected.size());
        EXPECT_EQ(file.Read(result.data(), result.size()), expected.size());
        EXPECT_EQ(file.Read(result.data(), result.size()), size_t{0});
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

TEST_F(TSpliceAsyncTest, TestReadFileSimple)
{
    auto data = MakeRandomData();
    WriteToFile(Filename_, data);

    auto pipe = OpenPipe(PipeFilename_, O_RDONLY);

    auto future = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename_, O_WRONLY),
        /*pipeIsSrc*/ false,
        Invoker_,
        Poller_);

    for (ui8 elem : data) {
        ReadExpectedByte(pipe, elem);
    }

    WaitForSplice(future);

    ui8 readElem = 0;
    EXPECT_EQ(pipe.Read(&readElem, 1), size_t{0});
}

TEST_F(TSpliceAsyncTest, TestReadFileConcurrent)
{
    auto data = MakeRandomData();
    WriteToFile(Filename_, data);

    auto pipe = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipe2 = OpenPipe(PipeFilename2_, O_RDONLY);

    auto future = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename_, O_WRONLY),
        /*pipeIsSrc*/ false,
        Invoker_,
        Poller_);

    auto future2 = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename2_, O_WRONLY),
        /*pipeIsSrc*/ false,
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
    EXPECT_EQ(pipe.Read(&readElem, 1), size_t{0});
    EXPECT_EQ(pipe2.Read(&readElem, 1), size_t{0});
}

TEST_F(TSpliceAsyncTest, TestReadFileBrokenPipe)
{
    auto data = MakeRandomData();
    WriteToFile(Filename_, data);

    auto pipe = OpenPipe(PipeFilename_, O_RDONLY);

    auto future = SpliceAsync(
        TFile(Filename_.c_str(), RdOnly),
        OpenPipe(PipeFilename_, O_WRONLY),
        /*pipeIsSrc*/ false,
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

TEST_F(TSpliceAsyncTest, TestWriteFileSimple)
{
    auto data = MakeRandomData();
    auto pipeRead = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipeWrite = OpenPipe(PipeFilename_, O_WRONLY);

    auto future = SpliceAsync(
        pipeRead,
        TFile(Filename_.c_str(), CreateAlways | WrOnly),
        /*pipeIsSrc*/ true,
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

TEST_F(TSpliceAsyncTest, TestWriteFileConcurrent)
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
        /*pipeIsSrc*/ true,
        Invoker_,
        Poller_);

    auto future2 = SpliceAsync(
        pipeRead2,
        TFile(Filename2_.c_str(), CreateAlways | WrOnly),
        /*pipeIsSrc*/ true,
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


TEST_F(TSpliceAsyncTest, TestWriteFileCancelFuture)
{
    auto data = MakeRandomData();
    auto pipeRead = OpenPipe(PipeFilename_, O_RDONLY);
    auto pipeWrite = OpenPipe(PipeFilename_, O_WRONLY);

    auto future = SpliceAsync(
        pipeRead,
        TFile(Filename_.c_str(), CreateAlways | WrOnly),
        /*pipeIsSrc*/ true,
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
