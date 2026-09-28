#include "http.h"
#include "http_proxy.h"
#include "http_proxy_sock64.h"

#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/network/address.h>
#include <util/network/sock.h>
#include <util/network/socket.h>
#include <util/system/tempfile.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <cerrno>

namespace {

const TString MALFORMED_PEM = "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n";
const TString MALFORMED_KEY_PEM = "-----BEGIN PRIVATE KEY-----\nnot base64\n-----END PRIVATE KEY-----\n";

// P-256 key for the cross-algorithm cases; the generated server certificates are RSA.
const TString EC_PRIVATE_KEY_PEM = R"(-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgFYhfZuT+2Pg2mtDK
R0JpYZdspHN43fI5KE/u68B12sGhRANCAASJiGwwsZS0gssPpb5muSzpdcoF1VeC
c/dBwI6MamtSdFnf77WYOv8sFXvCDpJnfvfMW6wixRhheD7iFEHwrgxV
-----END PRIVATE KEY-----
)";

NKikimr::NCertTestUtils::TCertAndKey GenerateServerCertAndKey() {
    using namespace NKikimr::NCertTestUtils;
    TCertAndKey ca = GenerateCA(TProps::AsCA());
    return GenerateSignedCert(ca, TProps::AsServer());
}

TString GenerateServerPem() {
    NKikimr::NCertTestUtils::TCertAndKey server = GenerateServerCertAndKey();
    return TString(server.Certificate) + TString(server.PrivateKey);
}

int PemCertificateKeyType(const TString& pem) {
    NHttp::TSslHelpers::TSslHolder<BIO> bio(BIO_new_mem_buf(pem.c_str(), pem.size()));
    NHttp::TSslHelpers::TSslHolder<X509> cert(PEM_read_bio_X509(bio.Get(), nullptr, nullptr, nullptr));
    UNIT_ASSERT_C(cert, "The certificate fixture does not parse");
    return EVP_PKEY_base_id(X509_get0_pubkey(cert.Get()));
}

int PemPrivateKeyType(const TString& pem) {
    NHttp::TSslHelpers::TSslHolder<BIO> bio(BIO_new_mem_buf(pem.c_str(), pem.size()));
    NHttp::TSslHelpers::TSslHolder<EVP_PKEY> key(PEM_read_bio_PrivateKey(bio.Get(), nullptr, nullptr, nullptr));
    UNIT_ASSERT_C(key, "The private key fixture does not parse");
    return EVP_PKEY_base_id(key.Get());
}

enum class EProbeResult {
    Refused,
    Connected,
    Timeout,
    Other,
};

EProbeResult ProbeTcp(const TString& host, ui16 port) {
    try {
        TNetworkAddress addr(host, port);
        TSocket socket(addr, TDuration::Seconds(2));
        return static_cast<SOCKET>(socket) != INVALID_SOCKET ? EProbeResult::Connected : EProbeResult::Other;
    } catch (const TSystemError& e) {
        if (e.Status() == ECONNREFUSED) {
            return EProbeResult::Refused;
        }
        if (e.Status() == ETIMEDOUT) {
            return EProbeResult::Timeout;
        }
        return EProbeResult::Other;
    } catch (...) {
        return EProbeResult::Other;
    }
}

struct TSimulatedProxy {
    NActors::TTestActorRuntimeBase Runtime{1, false};
    ui32 Retries = 0;
    NActors::TActorId ProxyId;
    NActors::TActorId EdgeId;

    TSimulatedProxy() {
        Runtime.SetScheduledEventFilter([this](NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>& event, TDuration, TInstant&) {
            if (event->GetTypeRewrite() == NHttp::TEvHttpProxy::TEvAddListeningPort::EventType) {
                ++Retries;
                return false; // keep the acceptor's retry
            }
            return true; // default: drop other scheduled events
        });
        Runtime.SetDispatchTimeout(TDuration::Seconds(10));
        Runtime.Initialize();
        ProxyId = Runtime.Register(NHttp::CreateHttpProxy());
        EdgeId = Runtime.AllocateEdgeActor();
    }

    void AddListeningPort(THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add) {
        add->Address = "127.0.0.1";
        Runtime.Send(new NActors::IEventHandle(ProxyId, EdgeId, add.Release()), 0, true);
    }

    bool NoConfirmListenWithin(TDuration simTimeout) {
        TAutoPtr<NActors::IEventHandle> handle;
        try {
            return Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle, simTimeout) == nullptr;
        } catch (const NActors::TEmptyEventQueueException&) {
            return true;
        }
    }

    void AssertNotListeningAcrossRetries(ui16 port) {
        for (ui32 minimumRetries = 1; minimumRetries <= 3; ++minimumRetries) {
            UNIT_ASSERT(NoConfirmListenWithin(TDuration::Seconds(1)));
            UNIT_ASSERT_GE(Retries, minimumRetries);
            UNIT_ASSERT_EQUAL_C(ProbeTcp("127.0.0.1", port), EProbeResult::Refused,
                "Failed TLS initialization opened a TCP listener");
        }
    }
};

void AssertHttpsRequestSucceeds(NActors::TTestActorRuntimeBase& runtime, const NActors::TActorId& proxyId, ui16 port) {
    TAutoPtr<NActors::IEventHandle> handle;
    NActors::TActorId serverId = runtime.AllocateEdgeActor();
    runtime.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/ready", serverId)), 0, true);

    NActors::TActorId clientId = runtime.AllocateEdgeActor();
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("https://[::1]:" + ToString(port) + "/ready");
    runtime.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

    NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
    UNIT_ASSERT_EQUAL(request->Request->URL, "/ready");

    NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString(
        "HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nready\r\n0\r\n\r\n");
    runtime.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

    NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
    UNIT_ASSERT_EQUAL(response->Response->Status, "200");
    UNIT_ASSERT_EQUAL(response->Response->Body, "ready");
}

}

Y_UNIT_TEST_SUITE(HttpProxyTlsInitialization) {
    Y_UNIT_TEST(MalformedPemDoesNotListen) {
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        TSimulatedProxy proxy;
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->SslCertificatePem = MALFORMED_PEM;
        proxy.AddListeningPort(std::move(add));

        UNIT_ASSERT(proxy.NoConfirmListenWithin(TDuration::MilliSeconds(500)));
        UNIT_ASSERT_GE(proxy.Retries, 1u);
        UNIT_ASSERT_EQUAL_C(ProbeTcp("127.0.0.1", port), EProbeResult::Refused,
            "Invalid TLS configuration opened a TCP listener");

        UNIT_ASSERT(proxy.NoConfirmListenWithin(TDuration::Seconds(2)));
        UNIT_ASSERT_GE(proxy.Retries, 2u);
        UNIT_ASSERT_EQUAL_C(ProbeTcp("127.0.0.1", port), EProbeResult::Refused,
            "Retrying acceptor opened a TCP listener without a security context");
    }

    Y_UNIT_TEST(UnloadableCertificateFileDoesNotListen) {
        TTempFileHandle certificateFile;
        certificateFile.Write(MALFORMED_PEM.data(), MALFORMED_PEM.size());

        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        TSimulatedProxy proxy;
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->CertificateFile = certificateFile.Name();
        add->PrivateKeyFile = certificateFile.Name();
        proxy.AddListeningPort(std::move(add));

        UNIT_ASSERT(proxy.NoConfirmListenWithin(TDuration::MilliSeconds(500)));
        UNIT_ASSERT_GE(proxy.Retries, 1u);
        UNIT_ASSERT_EQUAL_C(ProbeTcp("127.0.0.1", port), EProbeResult::Refused,
            "Unloadable certificate file opened a TCP listener");
    }

    Y_UNIT_TEST(UnloadablePrivateKeyFileDoesNotListen) {
        NKikimr::NCertTestUtils::TCertAndKey server = GenerateServerCertAndKey();
        TTempFileHandle certificateFile;
        certificateFile.Write(server.Certificate.data(), server.Certificate.size());
        TTempFileHandle privateKeyFile;
        privateKeyFile.Write(MALFORMED_KEY_PEM.data(), MALFORMED_KEY_PEM.size());

        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        TSimulatedProxy proxy;
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->CertificateFile = certificateFile.Name();
        add->PrivateKeyFile = privateKeyFile.Name();
        proxy.AddListeningPort(std::move(add));

        proxy.AssertNotListeningAcrossRetries(port);
    }

    Y_UNIT_TEST(MismatchedInlinePemDoesNotListen) {
        NKikimr::NCertTestUtils::TCertAndKey server = GenerateServerCertAndKey();
        UNIT_ASSERT_UNEQUAL(PemCertificateKeyType(TString(server.Certificate)), PemPrivateKeyType(EC_PRIVATE_KEY_PEM));

        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        TSimulatedProxy proxy;
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->SslCertificatePem = TString(server.Certificate) + EC_PRIVATE_KEY_PEM;
        proxy.AddListeningPort(std::move(add));

        proxy.AssertNotListeningAcrossRetries(port);
    }

    Y_UNIT_TEST(MismatchedCertificateFilesDoNotListen) {
        NKikimr::NCertTestUtils::TCertAndKey server = GenerateServerCertAndKey();
        UNIT_ASSERT_UNEQUAL(PemCertificateKeyType(TString(server.Certificate)), PemPrivateKeyType(EC_PRIVATE_KEY_PEM));
        TTempFileHandle certificateFile;
        certificateFile.Write(server.Certificate.data(), server.Certificate.size());
        TTempFileHandle privateKeyFile;
        privateKeyFile.Write(EC_PRIVATE_KEY_PEM.data(), EC_PRIVATE_KEY_PEM.size());

        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        TSimulatedProxy proxy;
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->CertificateFile = certificateFile.Name();
        add->PrivateKeyFile = privateKeyFile.Name();
        proxy.AddListeningPort(std::move(add));

        proxy.AssertNotListeningAcrossRetries(port);
    }

    Y_UNIT_TEST(ValidInlinePemServesHttps) {
        NActors::TTestActorRuntimeBase runtime(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        runtime.Initialize();

        NActors::TActorId proxyId = runtime.Register(NHttp::CreateHttpProxy());
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->SslCertificatePem = GenerateServerPem();
        runtime.Send(new NActors::IEventHandle(proxyId, runtime.AllocateEdgeActor(), add.Release()), 0, true);

        TAutoPtr<NActors::IEventHandle> handle;
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);
        UNIT_ASSERT(handle);

        AssertHttpsRequestSucceeds(runtime, proxyId, port);
    }

    Y_UNIT_TEST(PreboundSocketStopsListeningAfterTlsFailure) {
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TIntrusivePtr<NHttp::TSocketDescriptor> socket = NHttp::TryBindListeningSocket("127.0.0.1", port);
        UNIT_ASSERT(socket);
        UNIT_ASSERT_EQUAL_C(ProbeTcp("127.0.0.1", port), EProbeResult::Connected,
            "The prebound socket must be listening before the acceptor sees it");

        TSimulatedProxy proxy;
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->SslCertificatePem = MALFORMED_PEM;
        add->PreboundSocket = socket;
        proxy.AddListeningPort(std::move(add));
        socket.Reset(); // The event is now the sole owner.

        UNIT_ASSERT(proxy.NoConfirmListenWithin(TDuration::Seconds(2)));
        UNIT_ASSERT_GE(proxy.Retries, 1u);
        UNIT_ASSERT_EQUAL_C(ProbeTcp("127.0.0.1", port), EProbeResult::Refused,
            "A prebound secure socket keeps accepting connections after the TLS context failed");
    }

    Y_UNIT_TEST(BindFailureRetriesWithValidTls) {
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        // No SO_REUSEADDR/SO_REUSEPORT here, so the acceptor's bind cannot succeed.
        auto occupier = MakeHolder<TInet64StreamSocket>();
        UNIT_ASSERT_EQUAL(occupier->Bind(occupier->MakeAddress(TString(), port).get()), 0);
        UNIT_ASSERT_EQUAL(occupier->Listen(1), 0);

        NActors::TTestActorRuntimeBase runtime(1, true);
        runtime.Initialize();

        NActors::TActorId proxyId = runtime.Register(NHttp::CreateHttpProxy());
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->SslCertificatePem = GenerateServerPem();
        runtime.Send(new NActors::IEventHandle(proxyId, runtime.AllocateEdgeActor(), add.Release()), 0, true);

        // Leave the port occupied across the one-second retry interval.
        Sleep(TDuration::Seconds(2));
        occupier.Reset();

        TAutoPtr<NActors::IEventHandle> handle;
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);
        UNIT_ASSERT(handle);

        AssertHttpsRequestSucceeds(runtime, proxyId, port);
    }
}
