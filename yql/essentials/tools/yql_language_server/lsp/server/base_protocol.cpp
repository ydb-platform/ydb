#include "base_protocol.h"

#include <util/string/cast.h>

namespace NLsp {

namespace {

class TReader final {
    static constexpr size_t MaxHeaderLines = 8;
    static constexpr size_t MaxContentBytes = 64 * 1024 * 1024;

public:
    explicit TReader(IInputStream* in)
        : In_(in)
    {
    }

    TMaybe<TString> Read() {
        TMaybe<size_t> length = ReadHead();
        if (!length) {
            return Nothing();
        }

        std::string buffer;
        buffer.resize(*length);
        if (In_->Load(buffer.data(), *length) != length) {
            return Nothing();
        }

        return buffer;
    }

private:
    TMaybe<size_t> ReadHead() {
        TString buffer;
        TMaybe<size_t> length;
        for (size_t i = 0; i < MaxHeaderLines; ++i) {
            if (!In_->ReadLine(buffer)) {
                return Nothing();
            }

            if (buffer.empty()) {
                break;
            }

            length = length.OrElse(ParseLength(buffer));
        }

        if (!length) {
            ExpectEOF();
        }

        if (!length) {
            return Nothing();
        }

        return *length;
    }

    TMaybe<size_t> ParseLength(TStringBuf line) {
        if (TStringBuf value; TStringBuf(line).AfterPrefix("Content-Length: ", value)) {
            size_t length;
            if (!TryIntFromString<10>(value, length)) {
                throw TLspBaseProtocolException() << "bad Content-Length: " << value;
            }

            if (MaxContentBytes < length) {
                throw TLspBaseProtocolException() << "too big Content-Length: " << length;
            }

            return length;
        }

        if (TStringBuf value; TStringBuf(line).AfterPrefix("Content-Type: ", value)) {
            if (value != "application/vscode-jsonrpc; charset=utf-8" &&
                value != "application/vscode-jsonrpc; charset=utf8")
            {
                throw TLspBaseProtocolException() << "bad Content-Type: " << value;
            }

            return Nothing();
        }

        throw TLspBaseProtocolException() << "bad: " << line;
    }

    void ExpectEOF() {
        if (char c; !In_->ReadChar(c)) {
            return;
        }

        throw TLspBaseProtocolException()
            << "expected EOF, because of a Content-Length absence";
    }

    IInputStream* In_;
};

class TWriter final: public IConsumer<TString> {
public:
    explicit TWriter(IOutputStream* out)
        : Out_(out)
    {
    }

    void Receive(TString value) override {
        *Out_ << "Content-Length: " << value.size() << "\r\n";
        *Out_ << "\r\n";
        *Out_ << value;
        Out_->Flush();
    }

    void Stop() override {
        Out_->Finish();
    }

private:
    IOutputStream* Out_;
};

} // namespace

TLspBaseProtocolException::TLspBaseProtocolException()
    : TLspException(NJsonRpc::TJsonRpcError::CodeParseError)
{
}

void LspBaseProtocolReader(IInputStream& in, IConsumer<TString>::TPtr consumer) {
    TReader reader(&in);
    while (TMaybe<TString> content = reader.Read()) {
        consumer->Receive(std::move(*content));
    }
}

IConsumer<TString>::TPtr LspBaseProtocolWriter(IOutputStream& out) {
    return new TWriter(&out);
}

} // namespace NLsp
