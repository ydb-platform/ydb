#include "simple_server.h"

#include <yt/cpp/mapreduce/http/http.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/threading/future/async.h>

#include <library/cpp/http/io/stream.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/testing/common/network.h>

#include <util/string/builder.h>
#include <util/stream/tee.h>
#include <util/system/thread.h>

using namespace NYT;

namespace {
    void ParseFirstLine(const TString firstLine, TString& method, TString& host , ui64& port, TString& command)
    {
        size_t idx = firstLine.find_first_of(' ');
        method = firstLine.substr(0, idx);
        size_t idx2 = firstLine.find_first_of(':', idx + 1);
        host = firstLine.substr(idx + 1, idx2 - idx - 1);
        idx = firstLine.find_first_of('/', idx2 + 1);
        port = std::atoi(firstLine.substr(idx2 + 1, idx - idx2 - 1).c_str());
        idx2 = firstLine.find_first_of(' ', idx + 1);
        command = firstLine.substr(idx, idx2 - idx);
    }
} // namespace

THolder<TSimpleServer> CreateSimpleHttpServer()
{
    auto port = NTesting::GetFreePort();
    return MakeHolder<TSimpleServer>(
        port,
        [] (IInputStream* input, IOutputStream* output) {
            try {
                while (true) {
                    THttpInput httpInput(input);
                    httpInput.ReadAll();

                    THttpOutput httpOutput(output);
                    httpOutput.EnableKeepAlive(true);
                    httpOutput << "HTTP/1.1 200 OK\r\n";
                    httpOutput << "\r\n";
                    for (size_t i = 0; i != 10000; ++i) {
                        httpOutput << "The grass was greener";
                    }
                    httpOutput.Flush();
                }
            } catch (const std::exception&) {
            }
        });
}

THolder<TSimpleServer> CreateProxyHttpServer()
{
    auto port = NTesting::GetFreePort();
    return MakeHolder<TSimpleServer>(
        port,
        [] (IInputStream* input, IOutputStream* output) {
            try {
                while (true) {
                    THttpInput httpInput(input);
                    const TString inputStr = httpInput.FirstLine();
                    auto headers = httpInput.Headers();
                    TString method, command, host;
                    ui64 port;
                    ParseFirstLine(inputStr, method, host, port, command);

                    THttpRequest request;
                    const TString hostName = ::TStringBuilder() << host << ":" << port;
                    request.Connect(hostName);
                    auto header = THttpHeader(method, command);
                    request.StartRequest(header);
                    request.FinishRequest();
                    auto res = request.GetResponseStream();
                    THttpOutput httpOutput(output);
                    httpOutput.EnableKeepAlive(true);
                    auto strRes = res->ReadAll();
                    httpOutput << "HTTP/1.1 200 OK\r\n";
                    httpOutput << "\r\n";
                    httpOutput << strRes;
                    httpOutput.Flush();
                }
            } catch (const std::exception&) {
            }
        });
}


class TConnectionPoolConfigGuard
{
public:
    TConnectionPoolConfigGuard(int newSize)
    {
        OldValue_ = TConfig::Get()->ConnectionPoolSize;
        TConfig::Get()->ConnectionPoolSize = newSize;
    }

    ~TConnectionPoolConfigGuard()
    {
        TConfig::Get()->ConnectionPoolSize = OldValue_;
    }

private:
    int OldValue_;
};

class TFuncThread
    : public ISimpleThread
{
public:
    using TFunc = std::function<void()>;

public:
    TFuncThread(const TFunc& func)
        : Func_(func)
    { }

    void* ThreadProc() noexcept override {
        Func_();
        return nullptr;
    }

private:
    TFunc Func_;
};

TEST(TConnectionPool, TestReleaseUnread)
{
    auto simpleServer = CreateSimpleHttpServer();

    const TString hostName = ::TStringBuilder() << "localhost:" << simpleServer->GetPort();

    for (size_t i = 0; i != 10; ++i) {
        THttpRequest request;
        request.Connect(hostName);
        request.StartRequest(THttpHeader("GET", "foo"));
        request.FinishRequest();
        request.GetResponseStream();
    }
}

TEST(TConnectionPool, TestProxy)
{
    auto simpleServer = CreateSimpleHttpServer();
    auto simpleServer2 = CreateProxyHttpServer();

    const TString hostName = ::TStringBuilder() << "localhost:" << simpleServer->GetPort();
    const TString hostName2 = ::TStringBuilder() << "localhost:" << simpleServer2->GetPort();

    for (size_t i = 0; i != 10; ++i) {
        THttpRequest request;
        request.Connect(hostName2);
        auto header = THttpHeader("GET", "foo");
        header.SetProxyAddress(hostName2);
        header.SetHostPort(hostName);
        request.StartRequest(header);
        request.FinishRequest();
        request.GetResponseStream();
    }
}

TEST(TConnectionPool, TestConcurrency)
{
    TConnectionPoolConfigGuard g(1);

    auto simpleServer = CreateSimpleHttpServer();
    const TString hostName = ::TStringBuilder() << "localhost:" << simpleServer->GetPort();
    auto threadPool = CreateThreadPool(20);

    const auto func = [&] {
        for (int i = 0; i != 100; ++i) {
            THttpRequest request;
            request.Connect(hostName);
            request.StartRequest(THttpHeader("GET", "foo"));
            request.FinishRequest();
            auto res = request.GetResponseStream();
            res->ReadAll();
        }
    };

    TVector<THolder<TFuncThread>> threads;
    for (int i = 0; i != 10; ++i) {
        threads.push_back(MakeHolder<TFuncThread>(func));
    };

    for (auto& t : threads) {
        t->Start();
    }
    for (auto& t : threads) {
        t->Join();
    }
}
