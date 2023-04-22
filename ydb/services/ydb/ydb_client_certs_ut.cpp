#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>

#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/location.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>
#include <ydb/core/client/server/dynamic_node_auth_processor.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/grpc/client/grpc_client_low.h>

#include <google/protobuf/any.h>

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
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

using TKikimrServerWithOutCertVerification = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

struct TKikimrServerWithCertVerification: public TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert> {
    using TBase = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

    TKikimrServerWithCertVerification()
        : TBase(GetAppConfig())
    {}

    static NKikimrConfig::TAppConfig GetAppConfig() {
        auto config = NKikimrConfig::TAppConfig();

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

Y_UNIT_TEST(TestAllCertIsOk) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const NTest::TCertAndKey& clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(!sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
            UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestWrongCertIndentity) {
    TKikimrServerWithCertVerificationAndWrongIndentity server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const NTest::TCertAndKey& clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer());

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(!sessionValue.IsTransportError(), sessionValue.GetIssues().ToString()); // do not authorize table service through cert
            UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestIncorrectUsageClientCertFails) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const NTest::TCertAndKey& serverCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsServer()); // client or client-server is allowed, not just server

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(serverCert.Certificate.c_str(), serverCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestCorruptedCertFails) {
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
    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestCorruptedKeyFails) {
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
    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestExpiredCertFails) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer().WithValid(TDuration::Seconds(2)));

    // wait intil cert expires
    Sleep(TDuration::Seconds(10));

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestServerWithoutCertVerificationAndExpiredCertWorks) {
    TKikimrServerWithOutCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    NTest::TCertAndKey clientServerCert = NTest::GenerateSignedCert(caCert, NTest::TProps::AsClientServer().WithValid(TDuration::Seconds(2)));

    // wait intil cert expires
    Sleep(TDuration::Seconds(10));

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(!sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
            UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

Y_UNIT_TEST(TestClientWithoutCertPassed) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const NTest::TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .UseSecureConnection(caCert.Certificate.c_str())
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(!sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
            UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
        };

    client.CreateSession().Apply(createSessionHandler).Wait();
    connection.Stop(true);
}

NClient::TKikimr GetKikimr(const TString& addr, const NTest::TCertAndKey& caCert, const NTest::TCertAndKey& clientServerCert) {
    NGrpc::TGRpcClientConfig grpcConfig(addr, TDuration::Seconds(15));
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

Y_UNIT_TEST(TestServerWithCertVerificationClientWithCertCallsRegisterNode) {
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

Y_UNIT_TEST(TestServerWithCertVerificationClientWithoutCertCallsRegisterNodeFails) {
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

Y_UNIT_TEST(TestServerWithoutCertVerificationClientWithCertCallsRegisterNode) {
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

Y_UNIT_TEST(TestServerWithoutCertVerificationClientWithoutCertCallsRegisterNode) {
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

Y_UNIT_TEST(TestServerWithWrongIndentityClientWithCertCallsRegisterNodeFails) {
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

Y_UNIT_TEST(TestInsecureClient) {
    TKikimrServerWithCertVerification server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    auto connection = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("test_user@builtin")
            .SetEndpoint(location));

    auto client = NYdb::NTable::TTableClient(connection);
    std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
        [client] (const TAsyncCreateSessionResult& future) mutable {
            const auto& sessionValue = future.GetValue();
            UNIT_ASSERT_C(sessionValue.IsTransportError(), sessionValue.GetIssues().ToString());
        };

    client.CreateSession().Apply(createSessionHandler).Wait();

    connection.Stop(true);
}

}

} // namespace NKikimr
