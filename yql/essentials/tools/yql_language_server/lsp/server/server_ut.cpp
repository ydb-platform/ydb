#include "server.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>
#include <yql/essentials/tools/yql_language_server/lsp/server/base_protocol.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/thread/pool.h>

using namespace NLsp;
using namespace NLsp::NJsonRpc;
using namespace NYql::NLog;

Y_UNIT_TEST_SUITE(JsonRpcServerTests) {

class TJsonRpcListener final: public NJsonRpc::TJsonRpcListener {
public:
    explicit TJsonRpcListener(NJsonRpc::TJsonRpcOutbox::TPtr out)
        : Out_(std::move(out))
    {
    }

    ~TJsonRpcListener() override {
        UNIT_ASSERT(IsEnded_);
    }

    void Receive(TJsonRpcRequest request) override {
        if (RandomNumber<bool>()) {
            Out_->Receive({.Result = {}, .Id = request.Id.GetOrElse({})});
        } else {
            throw TLspException::Unknown() << "TT";
        }
    }

    void Stop() override {
        if (!std::exchange(IsEnded_, true)) {
            Out_->Stop();
            return;
        }

        UNIT_FAIL("Stop called twice");
    }

private:
    bool IsEnded_ = false;
    NJsonRpc::TJsonRpcOutbox::TPtr Out_;
};

void TestInput(TString input, TLspServerOptions options, TStringStream Serr = {}) try {
    SetRandomSeed(123);

    TStringStream Sin(input);
    TStringStream Sout;

    LspServe(Sin, Sout, options, [](auto out) {
        return new TJsonRpcListener(std::move(out));
    });
} catch (const TLspBaseProtocolException& e) {
    Y_UNUSED(e);
} catch (...) {
    Cerr << Serr.Str() << Endl;
    UNIT_FAIL("Server panicked");
}

void TestInput(TString input) {
    TestInput(input, {.Threads = 1});
    TestInput(input, {.Threads = 4});
}

TString Frame(size_t length, TStringBuf body) {
    return TStringBuilder()
           << "Content-Length: " << length << "\r\n"
           << "\r\n"
           << body;
}

TString Frame(TStringBuf body) {
    return Frame(body.size(), body);
}

constexpr TStringBuf ValidRequest = R"({"jsonrpc":"2.0","method":"foo","id":1})";

Y_UNIT_TEST(Empty) {
    TestInput("");
    TestInput(" ");
    TestInput("  ");
    TestInput("\n");
    TestInput("\r\n");
    TestInput("\r\n\r");
    TestInput("\r\n\r\n");
    TestInput("\r\n\r\n\r\n");
    TestInput(TString(1024, '\n'));
    TestInput(TString(1024, ' '));
    TestInput(TString(1024, '\0'));
}

Y_UNIT_TEST(HeaderOnly) {
    TestInput("Content-Length: 0");
    TestInput("Content-Length: 0\r\n");
    TestInput("Content-Length: 10");
    TestInput("Content-Type: application/vscode-jsonrpc; charset=utf-8");
    TestInput("Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n");
    TestInput("Content-Length: 5\r\nContent-Type: application/vscode-jsonrpc; charset=utf-8\r\n");
}

Y_UNIT_TEST(ContentLengthZero) {
    TestInput(Frame(""));
    TestInput(Frame("") + Frame(""));
    TestInput("Content-Length: 0\r\n\r\n");
    TestInput("Content-Length: 00000\r\n\r\n");
}

Y_UNIT_TEST(ContentLengthBad) {
    TestInput("Content-Length: abc\r\n\r\n");
    TestInput("Content-Length: -1\r\n\r\n");
    TestInput("Content-Length: +5\r\n\r\nhello");
    TestInput("Content-Length:  5\r\n\r\nhello");
    TestInput("Content-Length: 5 \r\n\r\nhello");
    TestInput("Content-Length: 0x10\r\n\r\n");
    TestInput("Content-Length: 3.5\r\n\r\n");
    TestInput("Content-Length: 1e3\r\n\r\n");
    TestInput("Content-Length: \r\n\r\n");
    TestInput("Content-Length: 99999999999999999999999999\r\n\r\n");
}

Y_UNIT_TEST(ContentLengthCaseAndSpacing) {
    TestInput("content-length: 5\r\n\r\nhello");
    TestInput("CONTENT-LENGTH: 5\r\n\r\nhello");
    TestInput("Content-length: 5\r\n\r\nhello");
    TestInput("Content-Length:5\r\n\r\nhello");
    TestInput("Content-Length : 5\r\n\r\nhello");
    TestInput(" Content-Length: 5\r\n\r\nhello");
}

Y_UNIT_TEST(ContentLengthMismatch) {
    TestInput(Frame(100, "short"));
    TestInput(Frame(1, ""));
    TestInput(Frame(1000000, ValidRequest));
    TestInput(Frame(4, "12345678"));
    TestInput(Frame(0, ValidRequest));
    TestInput(Frame(size_t(-1), "x"));
}

Y_UNIT_TEST(ContentTypeHeader) {
    TestInput("Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n" + Frame(ValidRequest));
    TestInput("Content-Type: application/vscode-jsonrpc; charset=utf8\r\n" + Frame(ValidRequest));
    TestInput(TStringBuilder()
              << "Content-Length: " << ValidRequest.size() << "\r\n"
              << "Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n"
              << "\r\n"
              << ValidRequest);
    TestInput("Content-Type: text/plain\r\n\r\n");
    TestInput("Content-Type: application/json\r\n\r\n");
    TestInput("Content-Type: application/vscode-jsonrpc; charset=ascii\r\n\r\n");
    TestInput("Content-Type: \r\n\r\n");
}

Y_UNIT_TEST(UnknownHeaders) {
    TestInput("Foo: bar\r\n\r\n");
    TestInput("X-Custom: 1\r\nContent-Length: 5\r\n\r\nhello");
    TestInput(Frame(ValidRequest) + "Trailing-Garbage: yes\r\n");
    TestInput("Content-Length: 5\r\nBogus\r\n\r\nhello");
    TestInput("Content-Length: 5\r\nBogus: x\r\n\r\nhello");
}

Y_UNIT_TEST(TooManyHeaderLines) {
    TStringBuilder many;
    for (size_t i = 0; i < 100; ++i) {
        many << "Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n";
    }
    TestInput(many);

    TStringBuilder boundary;
    for (size_t i = 0; i < 32; ++i) {
        boundary << "Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n";
    }
    boundary << "\r\n"
             << Frame(ValidRequest);
    TestInput(boundary);
}

Y_UNIT_TEST(LineEndings) {
    TestInput("Content-Length: 5\n\nhello");
    TestInput("Content-Length: 5\n\r\nhello");
    TestInput(TStringBuilder() << "Content-Length: " << ValidRequest.size() << "\n\n"
                               << ValidRequest);
    TestInput("\rContent-Length: 5\r\n\r\nhello");
    TestInput("Content-Length: 5\r\r\n\r\nhello");
}

Y_UNIT_TEST(ValidRequests1) {
    TestInput(
        TStringBuilder()
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":1})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":"abc"})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":null})")
        << Frame(R"({"jsonrpc":"2.0","method":"notify"})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":1,"params":{"a":1}})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":1,"params":[1,2,3]})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":-9223372036854775808})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":9223372036854775807})"));
}

Y_UNIT_TEST(ValidRequests2) {
    TStringBuilder batch;
    for (size_t i = 0; i < 64; ++i) {
        batch << Frame(ValidRequest);
    }
    TestInput(std::move(batch));
}

Y_UNIT_TEST(InvalidRequestShape) {
    TestInput(
        TStringBuilder()
        << Frame(R"({"jsonrpc":"1.0","method":"foo","id":1})")
        << Frame(R"({"method":"foo","id":1})")
        << Frame(R"({"jsonrpc":"2.0","id":1})")
        << Frame(R"({"jsonrpc":"2.0","method":123,"id":1})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":1.5})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":true})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":{}})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":[]})")
        << Frame(R"({"jsonrpc":"2.0","method":"foo","id":99999999999999999999})")
        << Frame(R"([{"jsonrpc":"2.0","method":"foo","id":1}])")
        << Frame("42")
        << Frame("\"hi\"")
        << Frame("null")
        << Frame("true"));
}

Y_UNIT_TEST(MalformedJson) {
    TestInput(Frame("{"));
    TestInput(Frame("}"));
    TestInput(Frame("{ ]"));
    TestInput(Frame(R"({"jsonrpc":"2.0",})"));
    TestInput(Frame(R"({"jsonrpc" "2.0"})"));
    TestInput(Frame(R"({jsonrpc:"2.0"})"));
    TestInput(Frame("{\"a\":\"\xFF\xFE\"}"));
    TestInput(Frame(TString(64, '{')));
    TestInput(Frame("not json at all"));
    TestInput(Frame("\x00\x01\x02\x03"));
}

Y_UNIT_TEST(NestedJson) {
    TStringBuilder deep;
    deep << R"({"jsonrpc":"2.0","method":"foo","id":1,"params":)";
    const size_t depth = 200;
    for (size_t i = 0; i < depth; ++i) {
        deep << "[";
    }
    for (size_t i = 0; i < depth; ++i) {
        deep << "]";
    }
    deep << "}";
    TestInput(Frame(deep));
}

Y_UNIT_TEST(LargeJson) {
    TStringBuilder big;
    big << R"({"jsonrpc":"2.0","method":"foo","id":1,"params":")"
        << TString(100000, 'a') << "\"}";
    TestInput(Frame(big));
}

Y_UNIT_TEST(UnicodeAndEscapes) {
    TestInput(
        TStringBuilder()
        << Frame(R"({"jsonrpc":"2.0","method":"метод","id":1})")
        << Frame(R"({"jsonrpc":"2.0","method":"emoji 😀","id":1})")
        << Frame(R"({"jsonrpc":"2.0","method":" ","id":1})")
        << Frame("{\"jsonrpc\":\"2.0\",\"method\":\"tab\ttab\",\"id\":1}")
        << Frame("{\"jsonrpc\":\"2.0\",\"method\":\"日本語テスト\",\"id\":1}"));
}

} // Y_UNIT_TEST_SUITE(JsonRpcServerTests)
