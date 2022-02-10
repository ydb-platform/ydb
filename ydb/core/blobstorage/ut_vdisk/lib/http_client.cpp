#include "http_client.h"

#include <library/cpp/openssl/io/stream.h>
#include <util/network/socket.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/string/join.h>
#include <util/string/split.h>

THttpRequestException::THttpRequestException(int statusCode):
    StatusCode(statusCode)
{
}

int THttpRequestException::GetStatusCode() const {
    return StatusCode;
}

//-----------------------------------------------------------------------------------------------------------

THttpClient::THttpClient(const TString& host, ui32 port, TDuration socketTimeout, TDuration connectTimeout)
    : Host(CutHttpPrefix(host))
    , Port(port)
    , SocketTimeout(socketTimeout)
    , ConnectTimeout(connectTimeout)
    , IsHttps(host.StartsWith("https"))
{
}

THttpClient::~THttpClient() = default;

void THttpClient::SendHttpRequest(const TStringBuf relativeUrl,
                                  const TString& body,
                                  const TString& method,
                                  IOutputStream* output,
                                  const THeaders& headers) const
{
    auto addr = Resolve();
    auto socket = Connect(*addr);

    TVector<IOutputStream::TPart> parts;

    TString contentLength;
    parts.reserve(16);
    parts.push_back(IOutputStream::TPart(method));
    parts.push_back(TStringBuf(" "));
    parts.push_back(relativeUrl);
    parts.push_back(TStringBuf(" HTTP/1.1"));
    parts.push_back(IOutputStream::TPart::CrLf());
    parts.push_back(TStringBuf("Host: "));
    parts.push_back(TStringBuf(Host));
    parts.push_back(IOutputStream::TPart::CrLf());
    parts.push_back(TStringBuf("From: oxygen@yandex-team.ru"));
    parts.push_back(IOutputStream::TPart::CrLf());
    if (body.size() > 0) {
        contentLength = ToString(body.size());
        parts.push_back(TStringBuf("Content-Length: "));
        parts.push_back(TStringBuf(contentLength));
        parts.push_back(IOutputStream::TPart::CrLf());
    }
    for (const auto& entry: headers) {
        parts.push_back(IOutputStream::TPart(entry.first));
        parts.push_back(IOutputStream::TPart(TStringBuf(": ")));
        parts.push_back(IOutputStream::TPart(entry.second));
        parts.push_back(IOutputStream::TPart::CrLf());
    }
    parts.push_back(IOutputStream::TPart::CrLf());
    if (body.size() > 0) {
        parts.push_back(IOutputStream::TPart(body));
    }

    TSocketOutput sockOutput(*socket);

    if (IsHttps) {
        TSocketInput si(*socket);
        TOpenSslClientIO ssl(&si, &sockOutput);
        THttpOutput ho(&ssl);
        ho.Write(parts.data(), parts.size());
        ho.Finish();
        THttpInput hi(&ssl);
        ReadAndTransferHttp(relativeUrl, hi, output);
    } else {
        sockOutput.Write(parts.data(), parts.size());
        sockOutput.Finish();
        TSocketInput si(*socket);
        THttpInput hi(&si);
        ReadAndTransferHttp(relativeUrl, hi, output);
    }
}

std::unique_ptr<TNetworkAddress> THttpClient::Resolve() const {
    try {
        return std::make_unique<TNetworkAddress>(Host, Port);
    } catch (const yexception& e) {
        ythrow THttpRequestException() << "Resolve of " << Host << ": " << e.what();
    }
}

std::unique_ptr<TSocket> THttpClient::Connect(TNetworkAddress& addr) const {
    try {
        std::unique_ptr<TSocket> socket = std::make_unique<TSocket>(addr, ConnectTimeout);
        socket->SetSocketTimeout(SocketTimeout.Seconds());
        return socket;
    } catch (const yexception& e) {
        ythrow THttpRequestException() << "Connect to " << Host << ':' << Port << " failed: " << e.what();
    }
}


void THttpClient::ProcessResponse(const TStringBuf relativeUrl, THttpInput& input, IOutputStream* output,
    const unsigned statusCode) const
{
    if (output) {
       TransferData(&input, output);
    }

    if (!(statusCode >= 200 && statusCode < 300)) {
        TString rest = input.ReadAll();
        ythrow THttpRequestException(statusCode) << "Got " << statusCode << " at " << Host << relativeUrl << "\nFull http response:\n" << rest;
    }
}

void THttpClient::ReadAndTransferHttp(const TStringBuf relativeUrl, THttpInput& input, IOutputStream* output) const {
    unsigned statusCode;
    try {
        statusCode = ParseHttpRetCode(input.FirstLine());
    } catch (TFromStringException& e) {
        TString rest = input.ReadAll();
        ythrow THttpRequestException() << "Failed parse status code in response of " << Host << ": " << e.what() <<
            " (" << input.FirstLine() << ")" << "\nFull http response:\n" << rest;
    }

    ProcessResponse(relativeUrl, input, output, statusCode);
}
