#include "test_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>

#include <openssl/pem.h>
#include <openssl/ssl.h>

#include <util/generic/yexception.h>
#include <util/network/socket.h>
#include <util/system/env.h>
#include <util/system/tempfile.h>
#include <util/system/datetime.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

namespace {

// Self-signed localhost certificate and private key used only by the test server.
constexpr char TestCertificate[] = R"PEM(-----BEGIN CERTIFICATE-----
MIIDITCCAgmgAwIBAgIUUwx3TvJXvw6bLn1TDL4NXW6haQ8wDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDkxNTEwMjUxM1oYDzIxMjYw
ODIyMTAyNTEzWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCoEDP1zRgNbHt5nh5CmB/4f5Ic+znXohPE3nSpfxro
njCTcLmOYYN34yxij7yozebAMJV5ebdJIhy4H90nCQpXV448aEf3cVpl9s0kEdDG
t0GFp31AtK+U3SzAERmx+NG8hyA2GHYRYgirglpueVtDtiRu8Um2Uu2M+ieHXgJx
sn4bOgo1Y+9r1SMSJXJtbC2SMcxyJC4HjzORsse0+cazZ66Ey9Jw9fQ8gKJs0JjA
q5MKno6a/BMrXu1JDlY3FOFKM6saIRKfH6GtmrM6EYmwSdC2CjBXLKI/BDrzDPQb
CsMSYjlxkiykmmDFGQuSYEMOm5QO33NEGGSWBAtUfSxdAgMBAAGjaTBnMB0GA1Ud
DgQWBBTGCtXyTRdpK5HQDS0SyeCTQ9xeKzAfBgNVHSMEGDAWgBTGCtXyTRdpK5HQ
DS0SyeCTQ9xeKzAUBgNVHREEDTALgglsb2NhbGhvc3QwDwYDVR0TAQH/BAUwAwEB
/zANBgkqhkiG9w0BAQsFAAOCAQEAV2ajayHUpaXRW876j8Vfa4AueSa3buYaXrzc
d8aKrlpEcstVlCykhBIHnPzlWXqfTbkNBYer9C/xfyXJE9m6xTW1OIQPibq3iuRi
6+vz49gsLyhudoP3gz3oHM1of+5YEp3vh4gzxTojS19ffLlgBWEUTuuETNdAakW4
++4Q7eus0GUlrrZTZeqjnhzU1UudjKc1ntXBZTzOAsN3Vt0BrO/eFTix7MDI3ktK
wGPQRUTNm0E1ITQ+Vst4HDouCZI34zdOSsJvZouWzo6lSiRNm+KUn6cqay9cNxoQ
36XwMgo6i+KWJSo2dVJXGaQk51u1jptkCXKvYJo2xq8sLdXBfA==
-----END CERTIFICATE-----
)PEM";

constexpr char TestPrivateKey[] = R"PEM(-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCoEDP1zRgNbHt5
nh5CmB/4f5Ic+znXohPE3nSpfxronjCTcLmOYYN34yxij7yozebAMJV5ebdJIhy4
H90nCQpXV448aEf3cVpl9s0kEdDGt0GFp31AtK+U3SzAERmx+NG8hyA2GHYRYgir
glpueVtDtiRu8Um2Uu2M+ieHXgJxsn4bOgo1Y+9r1SMSJXJtbC2SMcxyJC4HjzOR
sse0+cazZ66Ey9Jw9fQ8gKJs0JjAq5MKno6a/BMrXu1JDlY3FOFKM6saIRKfH6Gt
mrM6EYmwSdC2CjBXLKI/BDrzDPQbCsMSYjlxkiykmmDFGQuSYEMOm5QO33NEGGSW
BAtUfSxdAgMBAAECggEAALFcaCQpzWMH7pw/wgTa2405vs6Bp7QT18kWUF06m6s3
RmGoaiqtvjtHWLqrS5jZstYgb56YVQAD1Uslqr5coWKLhA/mqAxQE+vctEwx1k01
bcXJ0Y/3yf8launxzKwwFSe2HXL5XaClVNZV5W8GTh+nQ8vRLXlnCvXnCXr85ezA
O4APlFi1qbpbNY9wzVXYOIfocq+CTEvQASOcvvwLAk1Tuh3Vcibl7xRFWlXkguea
LBSfvIRgVzPomMTgyJsve1W+w09C86I8yawa3zWiFS8O5HdCy8T7CvTCEQKA/hPI
fv7nDCsIuqjXZsuLw6PJN0sBiN18C/ARrlcH0wBPYQKBgQDbeGx6PAdfOBe4nUte
MJPQWBLnhLyWB44NOQA3FLoFW9VEu51Aa+7I/nOUxFFj29FxSJxkftvQu92laLow
o+sdXnjGfWd4yh/MeC7DOXOHmtX+PDs/FypPZMUbwawNkYwog16nd/z8BuBIsQkO
2bvfOBpDqA/wOVNCFawaCJH4rQKBgQDECVXvm5hAMTHeI2CbAQJ6PuT0eTYFJ4oa
jDqmK5UzMkxshinF/cDPWabwVz9kJffoQc6JrhoTxaewaeg7iqoTp5s2/5BzPTXC
qKb13RQtNqQ7yl0lBXjJ/VZQ2BVAUh3h/kpwRP6f7eMY5YYPfxvrbCru7Lb1dHi+
TcOmuY0IcQKBgQCYJItO0X5qy//lw2UUDqjpraStSp9Rgjs/f1xe0seCH39g/o6s
siX+wCZv4whpKWGwHp4MLMVFlna4zDkGrxu2aF9hel3YpoYUwNvqClHEl9nxPN/1
hKGYGEtsSn5ziYqYKzna7ps6O6oPumqFGPvcapAKht9FsPe+wDdmdLp8oQKBgH5h
Ll+cRZkMngOBdyQ2kGxS47Of+O11whi/UogSDMvGn3JPQ9r6bjS+rVrARIPB3oKC
+i3UacdZY3PdsvO/v0mQggYA2BUS3vexVoGmlv1W/qX1Hfth/a7qfZz80SZ4Sf+J
ul+Ke0SLTh6cycJvxYYOY9dID+NJxRWaeImhkYRhAoGABGWTKewptIvJT8n69Rmt
hiaSpksnRsjT/URNRD9oyGPeFbmh3BjoUJND/FhMwMr/y446Yo9n6+2Wi9U52n3I
Bmc27AR902ceiZY5oPAoe82wGTBqTMvfeLZSNGZ67WxDqNH/2Vy52/21IM/KAEVk
2GMqMlo0l4Zn5vQ9jTkfKBI=
-----END PRIVATE KEY-----
)PEM";

SSL_CTX* ServerContext();

class TTlsStreams: public THttpServerConn::ISocketStreams, public IInputStream, public IOutputStream {
public:
    explicit TTlsStreams(const TSocket& socket);
    IInputStream* Input() override;
    IOutputStream* Output() override;
    void Reset() override;

private:
    size_t DoRead(void* buffer, size_t size) override;
    void DoWrite(const void* buffer, size_t size) override;

    TSocket Socket;
    std::unique_ptr<SSL, decltype(&SSL_free)> Ssl;
};

SSL_CTX* ServerContext() {
    static const auto context = [] {
        std::unique_ptr<BIO, decltype(&BIO_free)> certificateBio(
            BIO_new_mem_buf(TestCertificate, sizeof(TestCertificate) - 1), BIO_free);
        std::unique_ptr<BIO, decltype(&BIO_free)> keyBio(
            BIO_new_mem_buf(TestPrivateKey, sizeof(TestPrivateKey) - 1), BIO_free);
        Y_ENSURE(certificateBio != nullptr && keyBio != nullptr);
        std::unique_ptr<X509, decltype(&X509_free)> certificate(
            PEM_read_bio_X509(certificateBio.get(), nullptr, nullptr, nullptr), X509_free);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            PEM_read_bio_PrivateKey(keyBio.get(), nullptr, nullptr, nullptr), EVP_PKEY_free);
        Y_ENSURE(certificate != nullptr && key != nullptr);
        std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> result(SSL_CTX_new(TLS_server_method()), SSL_CTX_free);
        Y_ENSURE(result != nullptr, "Cannot create test TLS context");
        Y_ENSURE(SSL_CTX_use_certificate(result.get(), certificate.get()) == 1);
        Y_ENSURE(SSL_CTX_use_PrivateKey(result.get(), key.get()) == 1);
        Y_ENSURE(SSL_CTX_check_private_key(result.get()) == 1);

        // The HTTP client loads trusted CAs from SSL_CERT_FILE. Keep this
        // temporary copy alive for the test process; TTempFileHandle removes it.
        static TTempFileHandle trustedCertificate;
        trustedCertificate.Write(TestCertificate, sizeof(TestCertificate) - 1);
        trustedCertificate.Close();
        SetEnv("SSL_CERT_FILE", trustedCertificate.Name());
        return result;
    }();
    return context.get();
}

TTlsStreams::TTlsStreams(const TSocket& socket)
    : Socket(socket)
    , Ssl(SSL_new(ServerContext()), SSL_free)
{
    Y_ENSURE(Ssl != nullptr);
    Y_ENSURE(SSL_set_fd(Ssl.get(), Socket) == 1);
    Y_ENSURE(SSL_accept(Ssl.get()) == 1, "Test TLS handshake failed");
}

IInputStream* TTlsStreams::Input() {
    return this;
}

IOutputStream* TTlsStreams::Output() {
    return this;
}

void TTlsStreams::Reset() {
}

size_t TTlsStreams::DoRead(void* buffer, size_t size) {
    const int result = SSL_read(Ssl.get(), buffer, std::min<size_t>(size, std::numeric_limits<int>::max()));
    if (result <= 0 && SSL_get_error(Ssl.get(), result) == SSL_ERROR_ZERO_RETURN) {
        return 0;
    }
    Y_ENSURE(result > 0, "Test TLS read failed");
    return result;
}

void TTlsStreams::DoWrite(const void* buffer, size_t size) {
    auto data = static_cast<const char*>(buffer);
    while (size) {
        const int written = SSL_write(Ssl.get(), data, std::min<size_t>(size, std::numeric_limits<int>::max()));
        Y_ENSURE(written > 0, "Test TLS write failed");
        data += written;
        size -= written;
    }
}

} // namespace

TOidcTestServer::TOidcTestServer()
    : Options(Ports.GetPort())
    , Server(this, Options)
{
    ServerContext();
    Y_ENSURE(Server.Start(), "Cannot start test OIDC server");
}

TOidcTestServer::~TOidcTestServer() {
    Server.Stop();
}

std::vector<std::vector<TString>> TOidcTestServer::HostHeaders() const {
    with_lock (Mutex) {
        return RecordedHosts;
    }
}

std::string TOidcTestServer::Issuer() const {
    return "https://localhost:" + std::to_string(Options.Port) + "/realm";
}

NYdb::NOidc::TOidcConfig TOidcTestServer::ClientConfig() const {
    NYdb::NOidc::TOidcConfig config;
    config.Issuer = Issuer();
    config.FlowConfig = NYdb::NOidc::TClientOidcConfig{"client", "secret +&", {"read", "write"}};
    return config;
}

void TOidcTestServer::Enqueue(TString body, HttpCodes status) {
    with_lock (Mutex) {
        Replies.push_back({status, std::move(body)});
    }
}

void TOidcTestServer::SetDiscoveryReply(TString body, HttpCodes status) {
    with_lock (Mutex) {
        DiscoveryReply = TReply{status, std::move(body)};
    }
}

void TOidcTestServer::SetTokenReplyDelay(TDuration delay) {
    with_lock (Mutex) {
        TokenReplyDelay = delay;
    }
}

void TOidcTestServer::BlockTokenRepliesUntil(NThreading::TFuture<void> released) {
    with_lock (Mutex) {
        TokenReplyGate = std::move(released);
    }
}

std::vector<TOidcTestServer::TRequestInfo> TOidcTestServer::Requests() const {
    with_lock (Mutex) {
        return Recorded;
    }
}

void TOidcTestServer::BlockTlsHandshakeUntil(NThreading::TFuture<void> released) {
    with_lock (Mutex) {
        TlsHandshakeGate = std::move(released);
    }
}

bool TOidcTestServer::WaitForTlsHandshake() {
    with_lock (Mutex) {
        return Changed.wait_for(Mutex, std::chrono::seconds(10), [&] { return TlsHandshakeStarted; });
    }
}

size_t TOidcTestServer::DiscoveryCount() const {
    with_lock (Mutex) {
        return Discoveries;
    }
}

bool TOidcTestServer::WaitRequests(size_t count) {
    with_lock (Mutex) {
        return Changed.wait_for(Mutex, std::chrono::seconds(10), [&] { return Recorded.size() >= count; });
    }
}

TOidcTestServer::TRequest::TRequest(TOidcTestServer& server)
    : Server(server)
{
}

bool TOidcTestServer::TRequest::DoReply(const TReplyParams& params) {
    const TParsedHttpFull parsed(params.Input.FirstLine());
    const TString body = params.Input.ReadAll();
    TReply reply;
    NThreading::TFuture<void> replyGate;
    TDuration replyDelay;
    with_lock (Server.Mutex) {
        std::vector<TString> hosts;
        for (const auto& header : params.Input.Headers()) {
            if (header.Name() == "Host") {
                hosts.push_back(header.Value());
            }
        }
        Server.RecordedHosts.push_back(std::move(hosts));
        if (parsed.Path == "/realm/.well-known/openid-configuration") {
            ++Server.Discoveries;
            NJson::TJsonValue metadata;
            metadata["issuer"] = Server.Issuer();
            metadata["token_endpoint"] = Server.Issuer() + "/token";
            metadata["device_authorization_endpoint"] = Server.Issuer() + "/device";
            reply.Body = NJson::WriteJson(metadata, false);
            if (Server.DiscoveryReply.has_value()) {
                reply = *Server.DiscoveryReply;
            }
        } else {
            TRequestInfo request{TString(parsed.Path), TString(parsed.Method), TCgiParameters(body), {}};
            for (const auto& header : params.Input.Headers()) {
                if (header.Name() == "Authorization") {
                    request.Authorization = header.Value();
                }
            }
            Server.Recorded.push_back(std::move(request));
            if (parsed.Path == "/realm/token") {
                replyGate = Server.TokenReplyGate;
                replyDelay = Server.TokenReplyDelay;
            }
            if (Server.Replies.empty()) {
                reply = {HTTP_BAD_REQUEST, R"({"error":"unexpected_request"})"};
            } else {
                reply = std::move(Server.Replies.front());
                Server.Replies.pop_front();
            }
            Server.Changed.notify_all();
        }
    }
    if (replyGate.Initialized()) {
        replyGate.Wait();
    }
    if (replyDelay) {
        params.Output << "HTTP/1.1 " << static_cast<unsigned>(reply.Status)
                      << " OK\r\nContent-Length: " << reply.Body.size() << "\r\n\r\n";
        for (const char c : reply.Body) {
            params.Output.Write(c);
            params.Output.Flush();
            Sleep(replyDelay);
        }
        return true;
    }
    THttpResponse response(reply.Status);
    response.SetContent(reply.Body);
    response.OutTo(params.Output);
    return true;
}

TClientRequest* TOidcTestServer::CreateClient() {
    return new TRequest(*this);
}

THolder<THttpServerConn> TOidcTestServer::TRequest::CreateHttpConnection(const TSocket& socket, size_t outputBuffer) {
    NThreading::TFuture<void> gate;
    with_lock (Server.Mutex) {
        gate = Server.TlsHandshakeGate;
        Server.TlsHandshakeStarted = true;
    }
    Server.Changed.notify_all();
    if (gate.Initialized()) {
        gate.Wait();
    }
    return MakeHolder<THttpServerConn>(MakeHolder<TTlsStreams>(socket), outputBuffer);
}
