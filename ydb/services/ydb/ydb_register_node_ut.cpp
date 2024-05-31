#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/location.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>
#include <ydb/core/security/certificate_check/dynamic_node_auth_processor.h>
#include <ydb/core/security/certificate_check/cert_auth_utils.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <google/protobuf/any.h>

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include "ydb_common_ut.h"

#include <util/generic/ymath.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

struct TKikimrServerWithOutCertVerification : TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert> {
    using TBase = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

    TKikimrServerWithOutCertVerification()
        : TBase(GetAppConfig())
    {}

    static NKikimrConfig::TAppConfig GetAppConfig() {
        auto config = NKikimrConfig::TAppConfig();
        config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        return config;
    }
};

struct TKikimrServerWithCertVerification: public TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert> {
    using TBase = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

    TKikimrServerWithCertVerification()
        : TBase(GetAppConfig())
    {}

    static NKikimrConfig::TAppConfig GetAppConfig() {
        auto config = NKikimrConfig::TAppConfig();

        auto& securityConfig = *config.MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenRequirement(true);
        // auto& administrationAllowedSids = *securityConfig.MutableAdministrationAllowedSIDs();
        // administrationAllowedSids.Add(BUILTIN_ACL_ROOT);
        // administrationAllowedSids.Add("C=RU,ST=MSK,L=MSK,O=YA,OU=UtTest,CN=localhost@cert");
        config.MutableFeatureFlags()->SetEnableDynamicNodeAuthorization(true);

        auto& dynNodeDefinition = *config.MutableClientCertificateAuthorization()->MutableDynamicNodeAuthorization();
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".yandex.ru"});

        return config;
    }
};

struct TKikimrServerWithCertVerificationAndWrongIdentity : public TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert> {
    using TBase = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

    TKikimrServerWithCertVerificationAndWrongIdentity()
        : TBase(GetAppConfig())
    {}

    static NKikimrConfig::TAppConfig GetAppConfig() {
        auto config = NKikimrConfig::TAppConfig();

        auto& securityConfig = *config.MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenRequirement(true);
        // auto& administrationAllowedSids = *securityConfig.MutableAdministrationAllowedSIDs();
        // administrationAllowedSids.Add(BUILTIN_ACL_ROOT);
        // administrationAllowedSids.Add("C=RU,ST=MSK,L=MSK,O=YA,OU=UtTest,CN=localhost@cert");
        config.MutableFeatureFlags()->SetEnableDynamicNodeAuthorization(true);

        auto& dynNodeDefinition = *config.MutableClientCertificateAuthorization()->MutableDynamicNodeAuthorization();
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("C", {"WRONG"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
        *dynNodeDefinition.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});

        return config;
    }
};

NDiscovery::TNodeRegistrationSettings GetNodeRegistrationSettings() {
    NDiscovery::TNodeRegistrationSettings settings;
    settings.Host("localhost");
    settings.Port(GetRandomPort());
    settings.ResolveHost("localhost");
    settings.Address("localhost");
    settings.DomainPath("Root");
    settings.FixedNodeId(false);

    NYdb::NDiscovery::TNodeLocation loc;
    loc.DataCenterNum = DataCenterFromString("DataCenter");
    loc.RoomNum = 0;
    loc.RackNum = RackFromString("Rack");
    loc.BodyNum = 2;
    loc.DataCenter = "DataCenter";
    loc.Rack = "Rack";
    loc.Unit = "Body";

    settings.Location(loc);
    return settings;
}

} // namespace

Y_UNIT_TEST_SUITE(TRegisterNodeOverDiscoveryService) {

Y_UNIT_TEST(ServerWithCertVerification_ClientWithCorrectCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(!result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(!resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey noCert;

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(noCert.Certificate.c_str(),noCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesCorrectCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey noCert;

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(noCert.Certificate.c_str(),noCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientDoesNotProvideCorrectCerts) {
    TKikimrServerWithCertVerificationAndWrongIdentity server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Cannot create token from certificate. Client certificate failed verification");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Cannot create token from certificate. Client certificate failed verification");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Cannot create token from certificate. Client certificate failed verification");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientDoesNotProvideAnyCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    TDriverConfig config;
    config.SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesServerCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey& serverCert = GenerateSignedCert(caCert, TProps::AsServer()); // client or client-server is allowed, not just server

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(serverCert.Certificate.c_str(),serverCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesCorruptedCert) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    if (clientServerCert.Certificate[50] != 'a') {
        clientServerCert.Certificate[50] = 'a';
    } else {
        clientServerCert.Certificate[50] = 'b';
    }

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "empty address list");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "empty address list");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "empty address list");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesCorruptedPrivatekey) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    if (clientServerCert.PrivateKey[20] != 'a') {
        clientServerCert.PrivateKey[20] = 'a';
    } else {
        clientServerCert.Certificate[20] = 'b';
    }

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "empty address list");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "empty address list");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "empty address list");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesExpiredCert) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer().WithValid(TDuration::Seconds(2)));

    // wait until cert expires
    Sleep(TDuration::Seconds(10));

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "failed to connect to all addresses");
}

Y_UNIT_TEST(ServerWithOutCertVerification_ClientProvidesExpiredCert) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer().WithValid(TDuration::Seconds(2)));

    // wait until cert expires
    Sleep(TDuration::Seconds(10));

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");
}

Y_UNIT_TEST(ServerWithCertVerification_ClientDoesNotProvideClientCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();

    TDriverConfig config;
    config.UseSecureConnection(caCert.Certificate.c_str())
          .SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Access denied without user token");
}

}

namespace {

NClient::TKikimr GetKikimr(const TString& addr, const TCertAndKey& caCert, const TCertAndKey& clientServerCert) {
    NYdbGrpc::TGRpcClientConfig grpcConfig(addr, TDuration::Seconds(15));
    grpcConfig.EnableSsl = true;
    grpcConfig.SslCredentials = {.pem_root_certs = caCert.Certificate.c_str(),
                                 .pem_private_key = clientServerCert.PrivateKey.c_str(),
                                 .pem_cert_chain = clientServerCert.Certificate.c_str()};
    grpcConfig.LoadBalancingPolicy = "round_robin";

    return NClient::TKikimr(grpcConfig);
}

THolder<NClient::TRegistrationResult> TryToRegisterDynamicNode(
        NClient::TKikimr& kikimr,
        const TString &domainName,
        const TString &nodeHost,
        const TString &nodeAddress,
        const TString &nodeResolveHost,
        ui16 interconnectPort)
{
    auto registrant = kikimr.GetNodeRegistrant();

    NActorsInterconnect::TNodeLocation location;
    location.SetDataCenter("DataCenter");
    location.SetRack("Rack");
    location.SetUnit("Body");
    TNodeLocation loc(location);

    NActorsInterconnect::TNodeLocation legacy;
    legacy.SetDataCenterNum(DataCenterFromString("DataCenter"));
    legacy.SetRoomNum(0);
    legacy.SetRackNum(RackFromString("Rack"));
    legacy.SetBodyNum(2);
    loc.InheritLegacyValue(TNodeLocation(legacy));

    return MakeHolder<NClient::TRegistrationResult>
        (registrant.SyncRegisterNode(ToString(domainName),
                                     nodeHost,
                                     interconnectPort,
                                     nodeAddress,
                                     nodeResolveHost,
                                     std::move(loc),
                                     false));
}

} // namespace

Y_UNIT_TEST_SUITE(TRegisterNodeOverLegacyService) {

Y_UNIT_TEST(ServerWithCertVerification_ClientWithCorrectCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(resp->IsSuccess(), resp->GetErrorMessage());

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey noCert;

    NClient::TKikimr kikimr = GetKikimr(location, caCert, noCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(!resp->IsSuccess(), resp->GetErrorMessage());
    UNIT_ASSERT_STRINGS_EQUAL(resp->GetErrorMessage(), "Cannot authorize node. Node has not provided certificate");

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesCorrectCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(resp->IsSuccess(), resp->GetErrorMessage());

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey noCert;

    NClient::TKikimr kikimr = GetKikimr(location, caCert, noCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(resp->IsSuccess(), resp->GetErrorMessage());

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(ServerWithCertVerification_ClientDoesNotProvideCorrectCerts) {
    TKikimrServerWithCertVerificationAndWrongIdentity server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(!resp->IsSuccess(), resp->GetErrorMessage());
    UNIT_ASSERT_STRINGS_EQUAL(resp->GetErrorMessage(), "Cannot authorize node by certificate");

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

}

} // namespace NKikimr
