#include <library/cpp/testing/unittest/registar.h>

#include "kafka_test_client.h"
#include "test_server.h"

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/kafka_proxy/kafka_connection.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/raw_socket/sock64.h>
#include <ydb/core/raw_socket/sock_listener.h>
#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <atomic>
#include <memory>
#include <optional>
#include <string_view>

using namespace NKafka;

namespace {

using namespace NKikimr;
using namespace NKikimr::NRawSocket;

std::shared_ptr<TInet64SecureStreamSocket::TServerMtlsCreds> MakeMtlsServerCreds(
        const NCertTestUtils::TCertAndKey& serverCert, const NCertTestUtils::TCertAndKey& ca) {
    auto creds = std::make_shared<TInet64SecureStreamSocket::TServerMtlsCreds>();
    {
        TSslHelpers::TSslHolder<BIO> bio(BIO_new_mem_buf(serverCert.Certificate.data(), (int)serverCert.Certificate.size()));
        creds->ServerCert = bio
            ? TSslHelpers::TSslHolder<X509>(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr))
            : TSslHelpers::TSslHolder<X509>();
    }
    {
        TSslHelpers::TSslHolder<BIO> bio(BIO_new_mem_buf(serverCert.PrivateKey.data(), (int)serverCert.PrivateKey.size()));
        creds->ServerPrivateKey = bio
            ? TSslHelpers::TSslHolder<EVP_PKEY>(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr))
            : TSslHelpers::TSslHolder<EVP_PKEY>();
    }
    {
        TSslHelpers::TSslHolder<BIO> bio(BIO_new_mem_buf(ca.Certificate.data(), (int)ca.Certificate.size()));
        creds->CACert = bio
            ? TSslHelpers::TSslHolder<X509>(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr))
            : TSslHelpers::TSslHolder<X509>();
    }
    return creds;
}

class TInstantSuccessTicketParser : public NActors::TActorBootstrapped<TInstantSuccessTicketParser> {
public:
    void Bootstrap() {
        Become(&TInstantSuccessTicketParser::StateWork);
    }

    std::atomic<size_t> Requests{0};

    STRICT_STFUNC(StateWork,
        HFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev, const NActors::TActorContext& ctx) {
        Requests.fetch_add(1);
        ctx.Send(ev->Sender,
                 new TEvTicketParser::TEvAuthorizeTicketResult(
                     ev->Get()->Ticket, TIntrusiveConstPtr<NACLib::TUserToken>()),
                 0, ev->Cookie);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KafkaMtls) {

    // YDBBUGS-940: TKafkaConnection::StateConnected SIGSEGV.
    // The mTLS success continuation consumes the single-slot PollerEventSaved cache.
    // A duplicate (late/retried) successful TEvAuthResult then replays a null TAutoPtr
    // and used to crash the node. The connection must close gracefully instead.
    //
    // The client deliberately sends no Kafka requests on the mTLS connection: the
    // request-processing path of the mTLS flow is racy and out of scope here. The test
    // drives the connection only through the TLS handshake and the auth round trip.
    Y_UNIT_TEST(MtlsAuthAndDuplicateAuthResult) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .EnableMtls = true,
        });
        auto* runtime = testServer.KikimrServer->GetRuntime();

        const auto ca = NCertTestUtils::GenerateCA(NCertTestUtils::TProps::AsCA());
        const auto serverCert = NCertTestUtils::GenerateSignedCert(ca, NCertTestUtils::TProps::AsServer());
        const auto clientCert = NCertTestUtils::GenerateSignedCert(ca, NCertTestUtils::TProps::AsClient());

        THolder<TTempFileHandle> serverCertFile = MakeHolder<TTempFileHandle>();
        THolder<TTempFileHandle> serverKeyFile = MakeHolder<TTempFileHandle>();
        {
            TFileOutput certOut(serverCertFile->Name());
            certOut.Write(serverCert.Certificate.data(), serverCert.Certificate.size());
            TFileOutput keyOut(serverKeyFile->Name());
            keyOut.Write(serverCert.PrivateKey.data(), serverCert.PrivateKey.size());
        }

        TPortManager portManager;
        const ui16 mtlsPort = portManager.GetTcpPort();

        TListenerSettings listenerSettings;
        listenerSettings.Port = mtlsPort;
        listenerSettings.Address = "::";
        listenerSettings.CertificateFile = serverCertFile->Name();
        listenerSettings.PrivateKeyFile = serverKeyFile->Name();
        listenerSettings.TcpNotDelay = true;

        const auto serverCreds = MakeMtlsServerCreds(serverCert, ca);
        NActors::IActor* connectionActor = nullptr;
        const auto& kafkaConfig = runtime->GetAppData().KafkaProxyConfig;
        auto connectionCreator = [&connectionActor, &kafkaConfig, serverCreds](
                                      const NActors::TActorId& listenerActorId,
                                      TIntrusivePtr<TSocketDescriptor> socket,
                                      TNetworkConfig::TSocketAddressType address) -> NActors::IActor* {
            auto* actor = CreateKafkaConnection(listenerActorId, socket, address, kafkaConfig, serverCreds);
            connectionActor = actor;
            return actor;
        };

        runtime->Register(CreateSocketListener(
                              NActors::MakePollerActorId(), listenerSettings, connectionCreator,
                              NKikimrServices::EServiceKikimr::KAFKA_PROXY),
                          0, runtime->GetAppData().UserPoolId);

        auto* ticketParser = new TInstantSuccessTicketParser();
        runtime->RegisterService(MakeTicketParserID(), runtime->Register(ticketParser));

        std::optional<TKafkaTestClient> client;
        for (size_t attempt = 0; attempt < 50 && !client; ++attempt) {
            try {
                client.emplace(mtlsPort, clientCert.Certificate, clientCert.PrivateKey, ca.Certificate);
            } catch (const std::exception&) {
                Sleep(TDuration::MilliSeconds(100));
            }
        }
        UNIT_ASSERT_C(client.has_value(), "failed to connect to the mTLS listener");
        client->SetSocketTimeout(5);

        bool authRequested = false;
        for (size_t attempt = 0; attempt < 100 && !authRequested; ++attempt) {
            authRequested = ticketParser->Requests.load() > 0;
            if (!authRequested) {
                Sleep(TDuration::MilliSeconds(100));
            }
        }
        UNIT_ASSERT_C(authRequested, "mTLS client certificate never reached the ticket parser");
        Sleep(TDuration::MilliSeconds(500));

        UNIT_ASSERT_C(connectionActor != nullptr, "mTLS connection actor was not created");
        const NActors::TActorId connectionId = connectionActor->SelfId();

        auto clientResponse = std::make_shared<TEvKafka::TEvResponse>(
            0, std::make_shared<TSaslAuthenticateResponseData>(), EKafkaErrors::NONE_ERROR);
        runtime->Send(new NActors::IEventHandle(
            connectionId, NActors::TActorId(),
            new TEvKafka::TEvAuthResult(EAuthSteps::SUCCESS, clientResponse)));

        bool closed = false;
        for (size_t attempt = 0; attempt < 50 && !closed; ++attempt) {
            try {
                Sleep(TDuration::MilliSeconds(100));
                client->ApiVersions(true);
            } catch (...) {
                closed = true;
            }
        }
        UNIT_ASSERT_C(closed, "connection should be closed after a duplicate mTLS auth result");
    }
}
