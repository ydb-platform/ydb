#include "helpers.h"

#include <yt/cpp/mapreduce/common/abortable_stream.h>
#include <yt/cpp/mapreduce/http/framing.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/tests/native/test_helpers/simple_server.h>
#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/testing/gtest_extensions/assertions.h>

#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

auto CreateUrl(const std::unique_ptr<TSimpleServer>& server)
{
    auto address = server->GetAddress();

    if (auto it = address.find("localhost"); it != TString::npos) {
        address.replace(it, 9, "127.0.0.1");
    }
    return "http://" + address + "/concatenate";
}

NYT::NHttpClient::IHttpClientPtr CreateClient() {
    if (NYT::NTesting::UseDefaultHttpClient()) {
        return NYT::NHttpClient::CreateDefaultHttpClient();
    }
    return NYT::NHttpClient::CreateCoreHttpClient(false, NYT::TConfig::Get());
}

TEST(TClientFramingTest, FramingSimple)
{
    auto server = CreateFramingEchoServer();

    NYT::THttpHeader header("POST", "concatenate");
    auto url = CreateUrl(server);
    auto client = CreateClient();

    auto request = client->StartRequest(url, "0-0-0-0", NYT::NHttpClient::THttpConfig(), header);

    auto* requestStream = request->GetStream();
    *requestStream << "Some funny data";

    auto response = request->Finish();
    auto responseStream = response->GetResponseStream();

    EXPECT_EQ(responseStream->ReadAll(), "Some funny data");
}

TEST(TClientFramingTest, FramingLarge)
{
    auto server = CreateFramingEchoServer();

    auto client = CreateClient();

    NYT::THttpHeader header("POST", "concatenate");
    auto url = CreateUrl(server);

    auto request = client->StartRequest(url, "0-0-0-0", NYT::NHttpClient::THttpConfig(), header);

    auto* requestStream = request->GetStream();
    auto data = TString(100000, 'x');
    *requestStream << data;

    auto response = request->Finish();
    auto responseStream = response->GetResponseStream();

    EXPECT_EQ(responseStream->ReadAll(), data);
}

namespace {

TString ReadResponsePayload(TString payload, bool framed, bool compressed)
{
    if (compressed) {
        TStringStream output;
        TZLibCompress compressor(&output, ZLib::GZip);
        compressor.Write(payload);
        compressor.Finish();
        payload = std::move(output.Str());
    }
    auto port = NTesting::GetFreePort();
    auto server = std::make_unique<TSimpleServer>(
        port,
        [payload, framed, compressed] (IInputStream* input, IOutputStream* output) {
            THttpInput request(input);
            request.ReadAll();

            *output << "HTTP/1.1 200 OK\r\n"
                << "Connection: close\r\n"
                << "Content-Length: " << payload.size() << "\r\n";
            if (framed) {
                *output << "X-YT-Framing: 1\r\n";
            }
            if (compressed) {
                *output << "Content-Encoding: gzip\r\n";
            }
            *output << "\r\n" << payload;
            output->Flush();
        });

    NYT::THttpHeader header("GET", "concatenate");
    header.SetResponseCompression(compressed ? "gzip" : "identity");
    auto client = CreateClient();
    auto response = client->Request(CreateUrl(server), "0-0-0-0", header);

    auto queue = NYT::New<NYT::NConcurrency::TActionQueue>("FramingTest");
    auto result = BIND([response = std::move(response)] {
        return response->GetResponseStream()->ReadAll();
    }).AsyncVia(queue->GetInvoker()).Run();

    return result.WithTimeout(TDuration::Seconds(10)).BlockingGet().ValueOrThrow();
}

void CheckResponseDecoding(bool framed, bool compressed, TStringBuf framingPrefix = {})
{
    const TString expected = "hello framing";
    TString payload = expected;
    if (framed) {
        TStringStream output;
        output.Write(framingPrefix);
        WriteDataFrame(payload, &output);
        payload = output.Str();
    }
    EXPECT_EQ(ReadResponsePayload(std::move(payload), framed, compressed), expected);
}

} // namespace

TEST(TClientFramingTest, ResponseWithoutFraming)
{
    CheckResponseDecoding(/*framed*/ false, /*compressed*/ false);
}

TEST(TClientFramingTest, GzipWithoutFraming)
{
    CheckResponseDecoding(/*framed*/ false, /*compressed*/ true);
}

TEST(TClientFramingTest, FramingWithoutCompression)
{
    CheckResponseDecoding(/*framed*/ true, /*compressed*/ false);
}

TEST(TClientFramingTest, FramingWithGzip)
{
    CheckResponseDecoding(/*framed*/ true, /*compressed*/ true);
}

TEST(TClientFramingTest, FramingAfterKeepAliveBlock)
{
    // Some adapters fill 64_KB blocks and then flush
    // Checks that full buffer of keep-alive frames is not parsed as EOF
    const TString keepAliveFrames(64_KB * 3, '\x02');
    CheckResponseDecoding(/*framed*/ true, /*compressed*/ false, keepAliveFrames);
}

TEST(TClientFramingTest, FramingAfterKeepAliveBlockWithGzip)
{
    // same as previous test
    const TString keepAliveFrames(64_KB * 3, '\x02');
    CheckResponseDecoding(/*framed*/ true, /*compressed*/ true, keepAliveFrames);
}

TEST(TClientFramingTest, TruncatedFrameHeader)
{
    using NYT::NTesting::UseDefaultHttpClient;
    SKIP_IF_DEFAULT_HTTP();

    for (bool compressed : {false, true}) {
        SCOPED_TRACE(compressed);
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ReadResponsePayload(TString("\x01\x03\x00", 3), /*framed*/ true, compressed),
            NYT::TErrorException,
            "Unexpected end of stream inside data frame header");
    }
}

TEST(TClientFramingTest, TruncatedFrameData)
{
    using NYT::NTesting::UseDefaultHttpClient;
    SKIP_IF_DEFAULT_HTTP();

    TStringStream output;
    WriteDataFrame("abc", &output);
    auto payload = output.Str();
    payload.pop_back();
    for (bool compressed : {false, true}) {
        SCOPED_TRACE(compressed);
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ReadResponsePayload(payload, /*framed*/ true, compressed),
            NYT::TErrorException,
            "Unexpected end of stream inside data frame");
    }
}

TString Unframe(const TString& framed)
{
    TStringInput input(framed);
    auto stream = NYT::NHttpClient::CreateUnframingInputStream(NYT::NDetail::CreateAbortableInputStreamAdapterFallback(&input));
    return stream->ReadAll();
}

TEST(TUnframingInputStream, SkipsKeepAliveAndEmptyFrames)
{
    TStringStream framed;
    framed << "\x02\x02";
    WriteDataFrame("", &framed);
    WriteDataFrame("hello ", &framed);
    framed << '\x02';
    WriteDataFrame("framing", &framed);
    EXPECT_EQ(Unframe(framed.Str()), "hello framing");
}

TEST(TUnframingInputStream, TruncatedHeader)
{
    EXPECT_THROW(Unframe(TString("\x01\x03\x00", 3)), yexception);
}
