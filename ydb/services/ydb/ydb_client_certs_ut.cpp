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
#include <ydb/core/grpc_services/auth_processor/dynamic_node_auth_processor.h>

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
#include "cert_gen.h"

#include <util/generic/ymath.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

struct TKikimrTestWithServerCert : TKikimrTestWithAuthAndSsl {
    static constexpr bool SSL = true;

    static const NTest::TCertAndKey& GetCACertAndKey() {
        static const NTest::TCertAndKey ca = NTest::GenerateCA(NTest::TProps::AsCA());
        return ca;
    }

    static const NTest::TCertAndKey& GetServertCert() {
        static const NTest::TCertAndKey server = NTest::GenerateSignedCert(GetCACertAndKey(), NTest::TProps::AsServer());
        return server;
    }

    static TString GetCaCrt() {
        return GetCACertAndKey().Certificate.c_str();
    }

    static TString GetServerCrt() {
        return GetServertCert().Certificate.c_str();
    }

    static TString GetServerKey() {
        return GetServertCert().PrivateKey.c_str();
    }
};

NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm MakeSubjectTerm(TString name, const TVector<TString>& values, const TVector<TString>& suffixes = {}) {
    NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm term;
    term.SetShortName(name);
    for (const auto& val: values) {
        *term.MutableValues()->Add() = val;
    }
    for (const auto& suf: suffixes) {
        *term.MutableSuffixes()->Add() = suf;
    }
    return term;
}

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

        config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
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

struct TKikimrServerWithCertVerificationAndWrongIndentity : public TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert> {
    using TBase = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

    TKikimrServerWithCertVerificationAndWrongIndentity()
        : TBase(GetAppConfig())
    {}

    static NKikimrConfig::TAppConfig GetAppConfig() {
        auto config = NKikimrConfig::TAppConfig();

        config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
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

Y_UNIT_TEST_SUITE(TGRpcClientCerts) {

Y_UNIT_TEST(TestGenerateAndVerify) {
    using namespace NTest;
    TCertAndKey ca = GenerateCA(TProps::AsCA());

    TCertAndKey server = GenerateSignedCert(ca, TProps::AsServer());
    VerifyCert(server.Certificate, ca.Certificate);

    TCertAndKey client = GenerateSignedCert(ca, TProps::AsClient());
    VerifyCert(client.Certificate, ca.Certificate);

    TCertAndKey clientServer = GenerateSignedCert(ca, TProps::AsClientServer());
    VerifyCert(clientServer.Certificate, ca.Certificate);
}

Y_UNIT_TEST(TestClientCertAuthorizationParamsMatch) {
    {
        TDynamicNodeAuthorizationParams authParams;
        TDynamicNodeAuthorizationParams::TDistinguishedName dn;
        dn.AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("C").AddValue("RU"))
          .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("ST").AddValue("MSK"))
          .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("L").AddValue("MSK"))
          .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("O").AddValue("YA"))
          .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("OU").AddValue("UtTest"))
          .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("CN").AddValue("localhost").AddSuffix(".yandex.ru"));
        authParams.AddCertSubjectDescription(dn);

        {
            TMap<TString, TString> subjectTerms;
            subjectTerms["C"] = "RU";
            subjectTerms["ST"] = "MSK";
            subjectTerms["L"] = "MSK";
            subjectTerms["O"] = "YA";
            subjectTerms["OU"] = "UtTest";
            subjectTerms["CN"] = "localhost";

            UNIT_ASSERT(authParams.IsSubjectDescriptionMatched(subjectTerms));
        }

        {
            TMap<TString, TString> subjectTerms;
            subjectTerms["C"] = "RU";
            subjectTerms["ST"] = "MSK";
            subjectTerms["L"] = "MSK";
            subjectTerms["O"] = "YA";
            subjectTerms["OU"] = "UtTest";
            subjectTerms["CN"] = "test.yandex.ru";

            UNIT_ASSERT(authParams.IsSubjectDescriptionMatched(subjectTerms));
        }

        {
            TMap<TString, TString> subjectTerms;
            subjectTerms["C"] = "RU";
            subjectTerms["ST"] = "MSK";
            subjectTerms["L"] = "MSK";
            subjectTerms["O"] = "YA";
            subjectTerms["OU"] = "UtTest";
            subjectTerms["CN"] = "test.yandex.ru";
            subjectTerms["ELSE"] = "WhatEver";

            UNIT_ASSERT(authParams.IsSubjectDescriptionMatched(subjectTerms));
        }

        {
            TMap<TString, TString> subjectTerms;
            subjectTerms["C"] = "WRONG";
            subjectTerms["ST"] = "MSK";
            subjectTerms["L"] = "MSK";
            subjectTerms["O"] = "YA";
            subjectTerms["OU"] = "UtTest";
            subjectTerms["CN"] = "test.yandex.ru";

            UNIT_ASSERT(!authParams.IsSubjectDescriptionMatched(subjectTerms));
        }

        {
            TMap<TString, TString> subjectTerms;
            subjectTerms["C"] = "RU";
            subjectTerms["ST"] = "MSK";
            subjectTerms["L"] = "MSK";
            subjectTerms["O"] = "YA";
            subjectTerms["OU"] = "UtTest";
            subjectTerms["CN"] = "test.not-yandex.ru";

            UNIT_ASSERT(!authParams.IsSubjectDescriptionMatched(subjectTerms));
        }

        {
            TMap<TString, TString> subjectTerms;
            //subjectTerms["C"] = "RU";
            subjectTerms["ST"] = "MSK";
            subjectTerms["L"] = "MSK";
            subjectTerms["O"] = "YA";
            subjectTerms["OU"] = "UtTest";
            subjectTerms["CN"] = "test.yandex.ru";

            UNIT_ASSERT(!authParams.IsSubjectDescriptionMatched(subjectTerms));
        }
    }
}

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

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientWithCorrectCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

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

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey noCert;

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

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRINGS_EQUAL(resultWithToken.GetIssues().ToOneLineString(), "{ <main>: Error: Cannot authorize node. Node has not provided certificate }");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(resultWithWrongToken.GetIssues().ToOneLineString(), "{ <main>: Error: GRpc error: (16): unauthenticated, unauthenticated: { <main>: Error: Could not find correct token validator } }");
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithoutCertVerification_ClientProvidesCorrectCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

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

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(resultWithWrongToken.GetIssues().ToOneLineString(), "{ <main>: Error: GRpc error: (16): unauthenticated, unauthenticated: { <main>: Error: Could not find correct token validator } }");
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithoutCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey noCert;

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

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(resultWithWrongToken.GetIssues().ToOneLineString(), "{ <main>: Error: GRpc error: (16): unauthenticated, unauthenticated: { <main>: Error: Could not find correct token validator } }");
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientDoesNotProvideCorrectCerts) {
    TKikimrServerWithCertVerificationAndWrongIndentity server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

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
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRINGS_EQUAL(result.GetIssues().ToOneLineString(), "{ <main>: Error: Cannot authorize node by certificate }");

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRINGS_EQUAL(resultWithToken.GetIssues().ToOneLineString(), "{ <main>: Error: Cannot authorize node by certificate }");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(!resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRINGS_EQUAL(resultWithWrongToken.GetIssues().ToOneLineString(), "{ <main>: Error: Cannot authorize node by certificate }");
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientDoesNotProvideAnyCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    TDriverConfig config;
    config.SetEndpoint(location);

    // Request with certificate only
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);

    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientProvidesServerCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const NTest::TCertAndKey& serverCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsServer()); // client or client-server is allowed, not just server

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

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientProvidesCorruptedCert) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());
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

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientProvidesCorruptedPrivatekey) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());
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

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientProvidesExpiredCert) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer().WithValid(TDuration::Seconds(2)));

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

    // Request with certificate and correct token
    auto connectionWithToken = NYdb::TDriver(config.SetAuthToken(BUILTIN_ACL_ROOT));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithToken);
    const auto resultWithToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithToken.Stop(true);

    UNIT_ASSERT_C(resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithOutCertVerification_ClientProvidesExpiredCert) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer().WithValid(TDuration::Seconds(2)));

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

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(resultWithWrongToken.GetIssues().ToOneLineString(), "{ <main>: Error: Could not find correct token validator }");
}

Y_UNIT_TEST(TestRegisterNodeViaDiscovery_ServerWithCertVerification_ClientDoesNotProvideClientCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();

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

    UNIT_ASSERT_C(!resultWithToken.IsTransportError(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithToken.IsSuccess(), resultWithToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRINGS_EQUAL(resultWithToken.GetIssues().ToOneLineString(), "{ <main>: Error: Cannot authorize node. Node has not provided certificate }");

    // Request with certificate and wrong token
    auto connectionWithWrongToken = NYdb::TDriver(config.SetAuthToken("wrong_token"));
    discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connectionWithWrongToken);
    const auto resultWithWrongToken = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connectionWithWrongToken.Stop(true);

    UNIT_ASSERT_C(resultWithWrongToken.IsTransportError(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!resultWithWrongToken.IsSuccess(), resultWithWrongToken.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(resultWithWrongToken.GetIssues().ToOneLineString(), "{ <main>: Error: Could not find correct token validator }");
}

NClient::TKikimr GetKikimr(const TString& addr, const NTest::TCertAndKey& caCert, const NTest::TCertAndKey& clientServerCert) {
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

Y_UNIT_TEST(TestRegisterNodeViaLegacy_ServerWithCertVerification_ClientWithCorrectCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(resp->IsSuccess(), resp->GetErrorMessage());

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(TestRegisterNodeViaLegacy_ServerWithCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey noCert;

    NClient::TKikimr kikimr = GetKikimr(location, caCert, noCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(!resp->IsSuccess(), resp->GetErrorMessage());
    UNIT_ASSERT_STRINGS_EQUAL(resp->GetErrorMessage(), "Cannot authorize node. Node has not provided certificate");

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(TestRegisterNodeViaLegacy_ServerWithoutCertVerification_ClientProvidesCorrectCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(resp->IsSuccess(), resp->GetErrorMessage());

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(TestRegisterNodeViaLegacy_ServerWithoutCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey noCert;

    NClient::TKikimr kikimr = GetKikimr(location, caCert, noCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(resp->IsSuccess(), resp->GetErrorMessage());

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(TestRegisterNodeViaLegacy_ServerWithCertVerification_ClientDoesNotProvideCorrectCerts) {
    TKikimrServerWithCertVerificationAndWrongIndentity server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(!resp->IsSuccess(), resp->GetErrorMessage());
    UNIT_ASSERT_STRINGS_EQUAL(resp->GetErrorMessage(), "Cannot authorize node by certificate");

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

}

} // namespace NKikimr
