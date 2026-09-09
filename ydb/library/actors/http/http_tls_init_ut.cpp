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
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <netinet/in.h>
#include <cerrno>

namespace {

// A syntactically framed but undecodable certificate: the acceptor must not be able
// to build a security context out of it.
const TString MALFORMED_PEM = "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n";

// Inline PEM as consumers pass it: the server certificate followed by its private key.
TString GenerateServerPem() {
    using namespace NKikimr::NCertTestUtils;
    TCertAndKey ca = GenerateCA(TProps::AsCA());
    TCertAndKey server = GenerateSignedCert(ca, TProps::AsServer());
    return TString(server.Certificate) + TString(server.PrivateKey);
}

enum class EProbeResult {
    Refused,
    Connected,
    Timeout,
    Other,
};

// A listening socket without a security context accepts nothing, so a connect() to it
// either succeeds into the backlog or times out. Both differ from a refused connect.
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

std::pair<TString, ui16> BoundHostAndPort(const TIntrusivePtr<NHttp::TSocketDescriptor>& socket) {
    sockaddr_storage ss{};
    socklen_t slen = sizeof(ss);
    Y_ABORT_UNLESS(getsockname(socket->GetDescriptor(), reinterpret_cast<sockaddr*>(&ss), &slen) == 0);
    if (ss.ss_family == AF_INET6) {
        return {"::1", ntohs(reinterpret_cast<sockaddr_in6*>(&ss)->sin6_port)};
    }
    return {"127.0.0.1", ntohs(reinterpret_cast<sockaddr_in*>(&ss)->sin_port)};
}

// Counts how many times the acceptor rescheduled its initialization. Scheduled events are
// dropped by default in simulated mode, so retries only happen because of this filter.
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
        Runtime.Send(new NActors::IEventHandle(ProxyId, EdgeId, add.Release()), 0, true);
    }

    bool NoConfirmListenWithin(TDuration simTimeout) {
        TAutoPtr<NActors::IEventHandle> handle;
        try {
            return Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle, simTimeout) == nullptr;
        } catch (const NActors::TEmptyEventQueueException&) {
            return true; // nothing left to dispatch, so no listener was confirmed either
        }
    }
};

class TCountingLogBackend : public TLogBackend {
public:
    explicit TCountingLogBackend(TStringBuf expectedSubstring)
        : ExpectedSubstring_(expectedSubstring)
    {
    }

    void WriteData(const TLogRecord& rec) override {
        if (TStringBuf(rec.Data, rec.Len).Contains(ExpectedSubstring_)) {
            TGuard<TMutex> g(Mutex_);
            ++Count_;
            CondVar_.BroadCast();
        }
    }

    void ReopenLog() override {}

    bool WaitForCount(ui32 expected, TDuration timeout) {
        TGuard<TMutex> g(Mutex_);
        return CondVar_.WaitT(Mutex_, timeout, [this, expected] { return Count_ >= expected; });
    }

private:
    TStringBuf ExpectedSubstring_;
    ui32 Count_ = 0;
    TMutex Mutex_;
    TCondVar CondVar_;
};

// Serves one HTTPS request through the proxy's own client and checks the answer.
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
    // A secure endpoint whose certificate cannot be parsed must not leave a listening
    // socket behind: a TCP probe would otherwise report the endpoint as ready while no
    // TLS handshake is ever served.
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

    // The file-based path carries the same contract, and its loaders report failure with 0
    // rather than a negative value, so an unusable certificate file must also stop the listener.
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

    // A caller-owned prebound socket stays owned by its caller when TLS initialization
    // fails: the acceptor neither confirms the listener nor closes somebody else's socket.
    Y_UNIT_TEST(InvalidPemPreservesPreboundSocket) {
        TIntrusivePtr<NHttp::TSocketDescriptor> socket = NHttp::TryBindListeningSocket(TString(), 0);
        UNIT_ASSERT(socket);
        const auto [host, port] = BoundHostAndPort(socket);
        UNIT_ASSERT(port != 0);

        {
            TSimulatedProxy proxy;
            THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
            add->Secure = true;
            add->SslCertificatePem = MALFORMED_PEM;
            add->PreboundSocket = socket;
            proxy.AddListeningPort(std::move(add));

            UNIT_ASSERT(proxy.NoConfirmListenWithin(TDuration::Seconds(2)));
            UNIT_ASSERT_GE(proxy.Retries, 1u);
            UNIT_ASSERT_EQUAL_C(ProbeTcp(host, port), EProbeResult::Connected,
                "Caller-owned prebound socket stopped listening");
        }

        UNIT_ASSERT_EQUAL_C(ProbeTcp(host, port), EProbeResult::Connected,
            "Prebound socket was closed by the destroyed actor system");

        socket.Reset();
        UNIT_ASSERT_EQUAL(ProbeTcp(host, port), EProbeResult::Refused);
    }

    // Valid TLS with an unavailable port keeps the retry path working: the acceptor must
    // pick the port up once it is free instead of giving up or confirming too early.
    Y_UNIT_TEST(BindFailureRetriesWithValidTls) {
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();

        // No SO_REUSEADDR/SO_REUSEPORT here, so the acceptor's bind cannot succeed.
        auto occupier = MakeHolder<TInet64StreamSocket>();
        UNIT_ASSERT_EQUAL(occupier->Bind(occupier->MakeAddress(TString(), port).get()), 0);
        UNIT_ASSERT_EQUAL(occupier->Listen(1), 0);

        TAutoPtr<TLogBackend> backend(new TCountingLogBackend("Failed to init - retrying..."));
        auto* countingBackend = dynamic_cast<TCountingLogBackend*>(backend.Get());

        NActors::TTestActorRuntimeBase runtime(1, true);
        runtime.SetLogBackend(backend);
        runtime.Initialize();

        NActors::TActorId proxyId = runtime.Register(NHttp::CreateHttpProxy());
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        add->Secure = true;
        add->SslCertificatePem = GenerateServerPem();
        runtime.Send(new NActors::IEventHandle(proxyId, runtime.AllocateEdgeActor(), add.Release()), 0, true);

        UNIT_ASSERT_C(countingBackend->WaitForCount(2, TDuration::Seconds(20)),
            "Acceptor did not retry while the port was occupied");

        occupier.Reset();

        TAutoPtr<NActors::IEventHandle> handle;
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);
        UNIT_ASSERT(handle);

        AssertHttpsRequestSucceeds(runtime, proxyId, port);
    }
}
