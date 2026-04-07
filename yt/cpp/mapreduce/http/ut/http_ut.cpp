#include "simple_server.h"

#include <yt/cpp/mapreduce/http/http.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/testing/common/network.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/testing/gtest_extensions/assertions.h>

#include <util/system/byteorder.h>

using namespace NYT;
using namespace testing;

void WriteDataFrame(TStringBuf string, IOutputStream* stream)
{
    stream->Write("\x01");
    ui32 size = string.size();
    auto littleEndianSize = HostToLittle(size);
    stream->Write(&littleEndianSize, sizeof(littleEndianSize));
    stream->Write(string);
}

std::unique_ptr<TSimpleServer> CreateFramingEchoServer()
{
    auto port = NTesting::GetFreePort();
    return std::make_unique<TSimpleServer>(
        port,
        [] (IInputStream* input, IOutputStream* output) {
            try {
                THttpInput httpInput(input);
                if (!httpInput.Headers().FindHeader("X-YT-Accept-Framing")) {
                    FAIL() << "X-YT-Accept-Framing header not found";
                }
                auto input = httpInput.ReadAll();

                THttpOutput httpOutput(output);
                httpOutput << "HTTP/1.1 200 OK\r\n";
                httpOutput << "X-YT-Framing: 1\r\n";
                httpOutput << "\r\n";
                httpOutput << "\x02\x02"; // Two KeepAlive frames.
                WriteDataFrame("", &httpOutput);
                WriteDataFrame(TStringBuf(input).SubString(0, 10), &httpOutput);
                httpOutput << "\x02"; // KeepAlive.
                WriteDataFrame("", &httpOutput);
                WriteDataFrame(TStringBuf(input).SubString(10, std::string::npos), &httpOutput);
                httpOutput << "\x02"; // KeepAlive.

                httpOutput.Flush();
            } catch (const std::exception& exc) {
            }
        });
}

struct TMisbehavingServerConfig
{
    bool DontStartServer = false;
    TDuration AcceptDelay;
    bool DontReadData = false;
    TDuration FirstLineDelay;
    bool SendInvalidFirstLine = false;
    TDuration HeaderDelay;
    bool SendInvalidHeader = false;
    TDuration BodyDelay;
};

std::unique_ptr<TSimpleServer> CreateMisbehavingServer(TMisbehavingServerConfig config)
{
    auto port = NTesting::GetFreePort();
    return std::make_unique<TSimpleServer>(
        port,
        [config] (IInputStream* input, IOutputStream* output) {
            try {
                if (config.DontReadData) {
                    return;
                }

                THttpInput httpInput(input);
                httpInput.ReadAll();

                if (config.FirstLineDelay) {
                    Sleep(config.FirstLineDelay);
                }

                if (config.SendInvalidFirstLine) {
                    return;
                }

                // THttpOutput sends first line + headers in one chunk,
                // so use raw stream instead
                *output << "HTTP/1.1 200 OK\r\n";

                if (config.HeaderDelay) {
                    Sleep(config.HeaderDelay);
                }

                *output << "X-TestHeader";

                if (config.SendInvalidHeader) {
                    output->Flush();
                    return;
                }

                *output << ": TestValue\r\n";

                if (config.BodyDelay) {
                    Sleep(config.BodyDelay);
                }

                *output << "\r\n";
                *output << "42";
                output->Flush();
            } catch (const std::exception& exc) {
            }
        },
        config.DontStartServer,
        config.AcceptDelay);
}

TEST(THttpHeaderTest, AddParameter)
{
    THttpHeader header("POST", "/foo");
    header.AddMutationId();

    auto id1 = header.GetParameters()["mutation_id"].AsString();

    header.AddMutationId();

    auto id2 = header.GetParameters()["mutation_id"].AsString();

    EXPECT_TRUE(id1 != id2);
}

TEST(TFramingTest, FramingSimple)
{
    auto server = CreateFramingEchoServer();

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "concatenate"), TDuration::Zero());
    auto requestStream = request.StartRequest();
    *requestStream << "Some funny data";
    request.FinishRequest();
    auto response = request.GetResponseStream()->ReadAll();
    EXPECT_EQ(response, "Some funny data");
}

TEST(TFramingTest, FramingLarge)
{
    auto server = CreateFramingEchoServer();

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "concatenate"), TDuration::Zero());
    auto requestStream = request.StartRequest();
    auto data = TString(100000, 'x');
    *requestStream << data;
    request.FinishRequest();
    auto response = request.GetResponseStream()->ReadAll();
    EXPECT_EQ(response, data);
}

TEST(TWrapSystemErrorTest, ConnectTimeout)
{
    auto server = CreateMisbehavingServer({
        .DontStartServer = true,
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Seconds(1));

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.StartRequest(),
        TTransportError,
        "can not connect to");
}

TEST(TWrapSystemErrorTest, ConnectAcceptTimeout)
{
    auto server = CreateMisbehavingServer({
        .AcceptDelay = TDuration::Seconds(3),
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Seconds(1));
    request.StartRequest();
    request.FinishRequest();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.GetResponseStream()->ReadAll(),
        TTransportError,
        "can not read from socket input stream");
}

TEST(TWrapSystemErrorTest, WriteTimeout)
{
    auto server = CreateMisbehavingServer({
        .DontReadData = true,
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Seconds(1));
    auto requestStream = request.StartRequest();

    auto data = TString(100000, 'x');

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        *requestStream << data,
        TTransportError,
        "can not writev to socket output stream");
}

TEST(TWrapSystemErrorTest, FirstLineTimeout)
{
    auto server = CreateMisbehavingServer({
        .FirstLineDelay = TDuration::Seconds(3),
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Seconds(1));
    request.StartRequest();
    request.FinishRequest();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.GetResponseStream()->ReadAll(),
        TTransportError,
        "can not read from socket input stream");
}

TEST(TWrapSystemErrorTest, EmptyFirstLine)
{
    auto server = CreateMisbehavingServer({
        .SendInvalidFirstLine = true,
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Zero());
    request.StartRequest();
    request.FinishRequest();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.GetResponseStream()->ReadAll(),
        TTransportError,
        "Failed to get first line");
}

TEST(TWrapSystemErrorTest, HeaderTimeout)
{
    auto server = CreateMisbehavingServer({
        .HeaderDelay = TDuration::Seconds(3),
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Seconds(1));
    request.StartRequest();
    request.FinishRequest();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.GetResponseStream()->ReadAll(),
        TTransportError,
        "can not read from socket input stream");
}

TEST(TWrapSystemErrorTest, InvalidHeader)
{
    auto server = CreateMisbehavingServer({
        .SendInvalidHeader = true,
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Zero());
    request.StartRequest();
    request.FinishRequest();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.GetResponseStream()->ReadAll(),
        TTransportError,
        "can not parse http header");
}

TEST(TWrapSystemErrorTest, BodyTimeout)
{
    auto server = CreateMisbehavingServer({
        .BodyDelay = TDuration::Seconds(3),
    });

    THttpRequest request("0-0-0-0", server->GetAddress(), THttpHeader("POST", "reply"), TDuration::Seconds(1));
    request.StartRequest();
    request.FinishRequest();

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        request.GetResponseStream()->ReadAll(),
        TTransportError,
        "can not read from socket input stream");
}
