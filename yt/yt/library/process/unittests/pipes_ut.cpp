#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/library/process/pipe.h>

#include <random>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NNet;

#ifndef _win_

//! NB: You can't set size smaller than that of a page.
constexpr int SmallPipeCapacity = 4096;

TEST(TPipeIOHolder, CanInstantiate)
{
    auto pipe = TPipeFactory().Create();

    auto readerHolder = pipe.CreateAsyncReader();
    auto writerHolder = pipe.CreateAsyncWriter();

    readerHolder->Abort().Get();
    writerHolder->Abort().Get();
}

TEST(TPipeTest, PrematureEOF)
{
    auto pipe = TNamedPipe::Create("./namedpipe");
    auto reader = pipe->CreateAsyncReader();

    auto buffer = TSharedMutableRef::Allocate(1024 * 1024);
    EXPECT_THROW(reader->Read(buffer).WithTimeout(TDuration::Seconds(1)).Get().ValueOrThrow(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

TBlob ReadAll(IConnectionReaderPtr reader, bool useWaitFor)
{
    auto buffer = TSharedMutableRef::Allocate(1_MB, {.InitializeStorage = false});
    auto whole = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>());

    while (true)  {
        TErrorOr<size_t> result;
        auto future = reader->Read(buffer);
        if (useWaitFor) {
            result = WaitFor(future);
        } else {
            result = future.Get();
        }

        if (result.ValueOrThrow() == 0) {
            break;
        }

        whole.Append(buffer.Begin(), result.Value());
    }
    return whole;
}

void WriteAll(IConnectionWriterPtr writer, const char* data, size_t size, size_t blockSize)
{
    while (size > 0) {
        const size_t currentBlockSize = std::min(blockSize, size);
        auto buffer = TSharedRef(data, currentBlockSize, nullptr);
        auto error = WaitFor(writer->Write(buffer));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        size -= currentBlockSize;
        data += currentBlockSize;
    }

    {
        auto error = WaitFor(writer->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }
}

TEST(TAsyncWriterTest, AsyncCloseFail)
{
    auto pipe = TPipeFactory().Create();

    auto reader = pipe.CreateAsyncReader();
    auto writer = pipe.CreateAsyncWriter();

    auto queue = New<NConcurrency::TActionQueue>();
    auto readFromPipe =
        BIND(&ReadAll, reader, false)
            .AsyncVia(queue->GetInvoker())
            .Run();

    int length = 200*1024;
    auto buffer = TSharedMutableRef::Allocate(length);
    ::memset(buffer.Begin(), 'a', buffer.Size());

    auto writeResult = writer->Write(buffer).Get();

    EXPECT_TRUE(writeResult.IsOK())
        << ToString(writeResult);

    auto error = writer->Close();

    auto readResult = readFromPipe.Get();
    ASSERT_TRUE(readResult.IsOK())
        << ToString(readResult);

    auto closeStatus = error.Get();
}

TEST(TAsyncWriterTest, WriteFailed)
{
    auto pipe = TPipeFactory().Create();
    auto reader = pipe.CreateAsyncReader();
    auto writer = pipe.CreateAsyncWriter();

    int length = 200*1024;
    auto buffer = TSharedMutableRef::Allocate(length);
    ::memset(buffer.Begin(), 'a', buffer.Size());

    auto asyncWriteResult = writer->Write(buffer);
    YT_UNUSED_FUTURE(reader->Abort());

    EXPECT_FALSE(asyncWriteResult.Get().IsOK())
        << ToString(asyncWriteResult.Get());
}

////////////////////////////////////////////////////////////////////////////////

class TPipeReadWriteTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        auto pipe = TPipeFactory().Create();

        Reader = pipe.CreateAsyncReader();
        Writer = pipe.CreateAsyncWriter();
    }

    void TearDown() override
    { }

    IConnectionReaderPtr Reader;
    IConnectionWriterPtr Writer;
};

class TNamedPipeReadWriteTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        auto pipe = TNamedPipe::Create("./namedpipe");
        Reader = pipe->CreateAsyncReader();
        Writer = pipe->CreateAsyncWriter();
    }

    void TearDown() override
    { }

    void SetUpWithCapacity(int capacity)
    {
        auto pipe = TNamedPipe::Create("./namedpipewcap", 0660, capacity);
        Reader = pipe->CreateAsyncReader();
        Writer = pipe->CreateAsyncWriter();
    }

    void SetUpWithDeliveryFence()
    {
        auto pipe = TNamedPipe::Create("./namedpipewcap", 0660);
        Reader = pipe->CreateAsyncReader();
        Writer = pipe->CreateAsyncWriter(/*useDeliveryFence*/ true);
    }

    IConnectionReaderPtr Reader;
    IConnectionWriterPtr Writer;
};

TEST_F(TPipeReadWriteTest, ReadSomethingSpin)
{
    TString message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);
    Writer->Write(buffer).Get().ThrowOnError();
    Writer->Close().Get().ThrowOnError();

    auto data = TSharedMutableRef::Allocate(1);
    auto whole = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>());

    while (true) {
        auto result = Reader->Read(data).Get();
        if (result.ValueOrThrow() == 0) {
            break;
        }
        whole.Append(data.Begin(), result.Value());
    }

    EXPECT_EQ(message, TString(whole.Begin(), whole.End()));
}

TEST_F(TNamedPipeReadWriteTest, ReadSomethingSpin)
{
    TString message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);

    Writer->Write(buffer).Get().ThrowOnError();
    Writer->Close().Get().ThrowOnError();

    auto data = TSharedMutableRef::Allocate(1);
    auto whole = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>());

    while (true) {
        auto result = Reader->Read(data).Get();
        if (result.ValueOrThrow() == 0) {
            break;
        }
        whole.Append(data.Begin(), result.Value());
    }
    EXPECT_EQ(message, TString(whole.Begin(), whole.End()));
}


TEST_F(TPipeReadWriteTest, ReadSomethingWait)
{
    TString message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);
    EXPECT_TRUE(Writer->Write(buffer).Get().IsOK());
    WaitFor(Writer->Close())
        .ThrowOnError();
    auto whole = ReadAll(Reader, false);
    EXPECT_EQ(message, TString(whole.Begin(), whole.End()));
}

TEST_F(TNamedPipeReadWriteTest, ReadSomethingWait)
{
    TString message("Hello pipe!\n");
    auto buffer = TSharedRef::FromString(message);
    EXPECT_TRUE(Writer->Write(buffer).Get().IsOK());
    WaitFor(Writer->Close())
        .ThrowOnError();
    auto whole = ReadAll(Reader, false);
    EXPECT_EQ(message, TString(whole.Begin(), whole.End()));
}

TEST_F(TPipeReadWriteTest, ReadWrite)
{
    TString text("Hello cruel world!\n");
    auto buffer = TSharedRef::FromString(text);
    Writer->Write(buffer).Get();
    auto errorsOnClose = Writer->Close();

    auto textFromPipe = ReadAll(Reader, false);

    auto error = errorsOnClose.Get();
    EXPECT_TRUE(error.IsOK()) << error.GetMessage();
    EXPECT_EQ(text, TString(textFromPipe.Begin(), textFromPipe.End()));
}

TEST_F(TNamedPipeReadWriteTest, ReadWrite)
{
    TString text("Hello cruel world!\n");
    auto buffer = TSharedRef::FromString(text);
    Writer->Write(buffer).Get();
    auto errorsOnClose = Writer->Close();

    auto textFromPipe = ReadAll(Reader, false);

    auto error = errorsOnClose.Get();
    EXPECT_TRUE(error.IsOK()) << error.GetMessage();
    EXPECT_EQ(text, TString(textFromPipe.Begin(), textFromPipe.End()));
}

TEST_F(TNamedPipeReadWriteTest, CapacityJustWorks)
{
    SetUpWithCapacity(SmallPipeCapacity);

    TString text(5, 'a');
    text.push_back('\n');
    auto writeBuffer = TSharedRef::FromString(text);

    auto writeFuture = Writer->Write(writeBuffer);
    EXPECT_TRUE(writeFuture.Get().IsOK());

    auto readBuffer = TSharedMutableRef::Allocate(5000, {.InitializeStorage = false});
    auto readResult = Reader->Read(readBuffer).Get();

    EXPECT_EQ(text, TString(readBuffer.Begin(), readResult.Value()));
}

TEST_F(TNamedPipeReadWriteTest, CapacityOverflow)
{
    SetUpWithCapacity(SmallPipeCapacity);
    auto readerQueue = New<NConcurrency::TActionQueue>("Reader");

    TString text(5000, 'a');
    text.push_back('\n');
    auto writeBuffer = TSharedRef::FromString(text);
    auto writeFuture = Writer->Write(writeBuffer);

    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    EXPECT_FALSE(writeFuture.IsSet());

    auto readFuture = BIND([&] {
        auto readBuffer = TSharedMutableRef::Allocate(6000, {.InitializeStorage = false});
        auto readResult = Reader->Read(readBuffer).Get();

        EXPECT_TRUE(readResult.IsOK());
        EXPECT_EQ(text.substr(0, 4096), TString(readBuffer.Begin(), readResult.Value()));
    })
        .AsyncVia(readerQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(readFuture.Get().IsOK());
    EXPECT_TRUE(writeFuture.Get().IsOK());
}

TEST_F(TNamedPipeReadWriteTest, CapacityDontDiscardSurplus)
{
    SetUpWithCapacity(SmallPipeCapacity);
    auto readerQueue = New<NConcurrency::TActionQueue>("Reader");
    auto writerQueue = New<NConcurrency::TActionQueue>("Writer");

    TString text(5000, 'a');
    text.push_back('\n');

    auto writeFuture = BIND(&WriteAll, Writer, text.data(), text.size(), text.size())
        .AsyncVia(writerQueue->GetInvoker())
        .Run();

    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    EXPECT_FALSE(writeFuture.IsSet());

    auto readFuture = BIND(&ReadAll, Reader, false)
        .AsyncVia(readerQueue->GetInvoker())
        .Run();

    auto readResult = readFuture.Get().ValueOrThrow();
    EXPECT_EQ(text, TString(readResult.Begin(), readResult.End()));

    EXPECT_TRUE(writeFuture.Get().IsOK());
}

#ifdef _linux_

TEST_F(TNamedPipeReadWriteTest, SyncWriteJustWorks)
{
    SetUpWithDeliveryFence();

    TString text("aabbb");
    auto writeBuffer = TSharedRef::FromString(text);
    auto writeFuture = Writer->Write(writeBuffer);

    auto readBuffer = TSharedMutableRef::Allocate(2, {.InitializeStorage = false});
    auto readResult = Reader->Read(readBuffer).Get();
    EXPECT_EQ(TString("aa"), TString(readBuffer.Begin(), readResult.Value()));

    EXPECT_FALSE(writeFuture.IsSet());

    readBuffer = TSharedMutableRef::Allocate(10, {.InitializeStorage = false});
    readResult = Reader->Read(readBuffer).Get();
    EXPECT_EQ(TString("bbb"), TString(readBuffer.Begin(), readResult.Value()));

    // Future is set only after the entire buffer is read.
    EXPECT_TRUE(writeFuture.Get().IsOK());
}

#else

TEST_F(TNamedPipeReadWriteTest, SyncWriteUnsupportedPlatform)
{
    SetUpWithDeliveryFence();

    TString text("aabbb");
    auto writeBuffer = TSharedRef::FromString(text);
    auto writeFuture = Writer->Write(writeBuffer);

    // Future is set with error because platform is not supported
    auto error = writeFuture.Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(error.GetMessage().Contains("Delivery fenced write failed: FIONDREAD is not supported on your platform"));
}

#endif

////////////////////////////////////////////////////////////////////////////////

class TPipeBigReadWriteTest
    : public TPipeReadWriteTest
    , public ::testing::WithParamInterface<std::pair<size_t, size_t>>
{ };

TEST_P(TPipeBigReadWriteTest, RealReadWrite)
{
    size_t dataSize, blockSize;
    std::tie(dataSize, blockSize) = GetParam();

    auto queue = New<NConcurrency::TActionQueue>();

    std::vector<char> data(dataSize, 'a');

    YT_UNUSED_FUTURE(BIND([&] {
        auto dice = std::bind(
            std::uniform_int_distribution<int>(0, 127),
            std::default_random_engine());
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = dice();
        }
    })
    .AsyncVia(queue->GetInvoker()).Run());

    auto writeError =  BIND(&WriteAll, Writer, data.data(), data.size(), blockSize)
        .AsyncVia(queue->GetInvoker())
        .Run();
    auto readFromPipe = BIND(&ReadAll, Reader, true)
        .AsyncVia(queue->GetInvoker())
        .Run();

    auto textFromPipe = readFromPipe.Get().ValueOrThrow();
    EXPECT_EQ(data.size(), textFromPipe.Size());
    auto result = std::mismatch(textFromPipe.Begin(), textFromPipe.End(), data.begin());
    EXPECT_TRUE(std::equal(textFromPipe.Begin(), textFromPipe.End(), data.begin())) <<
        (result.first - textFromPipe.Begin()) << " " << (int)(*result.first);
}

INSTANTIATE_TEST_SUITE_P(
    ValueParametrized,
    TPipeBigReadWriteTest,
    ::testing::Values(
        std::pair(2000 * 4096, 4096),
        std::pair(100 * 4096, 10000),
        std::pair(100 * 4096, 100),
        std::pair(100, 4096)));

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
