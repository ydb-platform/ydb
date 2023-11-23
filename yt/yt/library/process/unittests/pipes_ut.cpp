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
    reader->Abort();

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

    BIND([&] () {
        auto dice = std::bind(
            std::uniform_int_distribution<int>(0, 127),
            std::default_random_engine());
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = dice();
        }
    })
    .AsyncVia(queue->GetInvoker()).Run();

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
        std::make_pair(2000 * 4096, 4096),
        std::make_pair(100 * 4096, 10000),
        std::make_pair(100 * 4096, 100),
        std::make_pair(100, 4096)));

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
