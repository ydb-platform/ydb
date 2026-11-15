#include "helpers.h"

#include <library/cpp/http/io/stream.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/byteorder.h>

#include <string>

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
