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
#include <ydb/core/security/certificate_check/cert_auth_processor.h>
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

struct TKikimrServerForTestNodeRegistration : TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert> {
    using TBase = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithServerCert>;

    struct TServerInitialization {
        bool EnforceUserToken = false;
        bool EnableDynamicNodeAuth = false;
        bool EnableWrongIdentity = false;
        bool SetNodeAuthValues = false;
        std::vector<TString> RegisterNodeAllowedSids = {"DefaultClientAuth@cert", BUILTIN_ACL_ROOT};
    };

    TKikimrServerForTestNodeRegistration(const TServerInitialization& serverInitialization)
        : TBase(GetAppConfig(serverInitialization))
    {}

private:
    static NKikimrConfig::TAppConfig GetAppConfig(const TServerInitialization& serverInitialization) {
        auto config = NKikimrConfig::TAppConfig();

        auto& securityConfig = *config.MutableDomainsConfig()->MutableSecurityConfig();
        if (serverInitialization.EnforceUserToken) {
            securityConfig.SetEnforceUserTokenRequirement(true);
        }
        if (serverInitialization.EnableDynamicNodeAuth) {
            config.MutableClientCertificateAuthorization()->SetRequestClientCertificate(true);
            // config.MutableFeatureFlags()->SetEnableDynamicNodeAuthorization(true);
        }

        std::vector<TString> tmpRegisterNodeAllowedSids(serverInitialization.RegisterNodeAllowedSids);
        for (auto& sid : tmpRegisterNodeAllowedSids) {
            securityConfig.MutableRegisterDynamicNodeAllowedSIDs()->Add(std::move(sid));
        }

        if (serverInitialization.SetNodeAuthValues) {
            auto& clientCertDefinitions = *config.MutableClientCertificateAuthorization()->MutableClientCertificateDefinitions();
            auto& certDef = *clientCertDefinitions.Add();
            if (serverInitialization.EnableWrongIdentity) {
                *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"WRONG"});
            } else {
                *certDef.AddSubjectTerms() = MakeSubjectTerm("C", {"RU"});
            }
            *certDef.AddSubjectTerms() = MakeSubjectTerm("ST", {"MSK"});
            *certDef.AddSubjectTerms() = MakeSubjectTerm("L", {"MSK"});
            *certDef.AddSubjectTerms() = MakeSubjectTerm("O", {"YA"});
            *certDef.AddSubjectTerms() = MakeSubjectTerm("OU", {"UtTest"});
            *certDef.AddSubjectTerms() = MakeSubjectTerm("CN", {"localhost"}, {".yandex.ru"});
        }

        return config;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TRegisterNodeOverDiscoveryService) {

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

void SetLogPriority(TKikimrServerForTestNodeRegistration& server) {
    server.GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    server.GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
}

NDiscovery::TNodeRegistrationResult RegisterNode(const TDriverConfig& config) {
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    const auto result = discoveryClient.NodeRegistration(GetNodeRegistrationSettings()).GetValueSync();
    connection.Stop(true);
    return result;
}

void CheckGood(const NDiscovery::TNodeRegistrationResult& result) {
    UNIT_ASSERT_C(!result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
}

void CheckAccessDenied(const NDiscovery::TNodeRegistrationResult& result, const TString& expectedError) {
    UNIT_ASSERT_C(result.IsTransportError(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), expectedError);
}

void CheckAccessDeniedRegisterNode(const NDiscovery::TNodeRegistrationResult& result, const TString& expectedError) {
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), expectedError);
}

Y_UNIT_TEST(ServerWithCertVerification_ClientWithCorrectCerts_EmptyAllowedSids) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true,
            .RegisterNodeAllowedSids = {}
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true,
            .RegisterNodeAllowedSids = {}
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientWithCorrectCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true,
            .RegisterNodeAllowedSids = {}
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true,
            .RegisterNodeAllowedSids = {}
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientWithCorrectCerts_AllowOnlyDefaultGroup) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true,
            .RegisterNodeAllowedSids = {"DefaultClientAuth@cert"}
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckAccessDeniedRegisterNode(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), "Cannot authorize node. Access denied");
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true,
            .RegisterNodeAllowedSids = {"DefaultClientAuth@cert"}
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckAccessDeniedRegisterNode(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), "Cannot authorize node. Access denied");
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithIssuerVerification_ClientWithSameIssuer) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = false
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = false
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesEmptyClientCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey noCert;
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(noCert.Certificate.c_str(),noCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Access denied without user token");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(noCert.Certificate.c_str(),noCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesCorrectCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Access denied without user token");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesEmptyClientCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey noCert;
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(noCert.Certificate.c_str(),noCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Access denied without user token");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(noCert.Certificate.c_str(),noCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvideIncorrectCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .EnableWrongIdentity = true,
            .SetNodeAuthValues = true,
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Cannot create token from certificate. Client certificate failed verification");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .EnableWrongIdentity = true,
            .SetNodeAuthValues = true,
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(),clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientDoesNotProvideAnyCerts) {
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.SetEndpoint(location);

        const TString expectedError = "failed to connect to all addresses";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.SetEndpoint(location);

        const TString expectedError = "failed to connect to all addresses";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesServerCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey& serverCert = GenerateSignedCert(caCert, TProps::AsServer()); // client or client-server is allowed, not just server
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(serverCert.Certificate.c_str(),serverCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "failed to connect to all addresses";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(serverCert.Certificate.c_str(),serverCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "failed to connect to all addresses";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesCorruptedCert) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    if (clientServerCert.Certificate[50] != 'a') {
        clientServerCert.Certificate[50] = 'a';
    } else {
        clientServerCert.Certificate[50] = 'b';
    }
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "empty address list";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "empty address list";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesCorruptedPrivatekey) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());
    if (clientServerCert.PrivateKey[20] != 'a') {
        clientServerCert.PrivateKey[20] = 'a';
    } else {
        clientServerCert.Certificate[20] = 'b';
    }
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "empty address list";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "empty address list";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesExpiredCert) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer().WithValid(TDuration::Seconds(2)));
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        // wait until cert expires
        Sleep(TDuration::Seconds(10));

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "failed to connect to all addresses";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        // wait until cert expires
        Sleep(TDuration::Seconds(10));

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        const TString expectedError = "failed to connect to all addresses";
        CheckAccessDenied(RegisterNode(config), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)), expectedError);
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), expectedError);
    }
}

Y_UNIT_TEST(ServerWithOutCertVerification_ClientProvidesExpiredCert) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer().WithValid(TDuration::Seconds(2)));
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        // wait until cert expires
        Sleep(TDuration::Seconds(10));

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Access denied without user token");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        // wait until cert expires
        Sleep(TDuration::Seconds(10));

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .UseClientCertificate(clientServerCert.Certificate.c_str(), clientServerCert.PrivateKey.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithCertVerification_ClientDoesNotProvideClientCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Access denied without user token");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
            .EnableDynamicNodeAuth = true,
            .SetNodeAuthValues = true
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
}

Y_UNIT_TEST(ServerWithoutCertVerification_ClientDoesNotProvideClientCerts) {
    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    {
        TKikimrServerForTestNodeRegistration server({
            .EnforceUserToken = true,
        });
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(server);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .SetEndpoint(location);

        CheckAccessDenied(RegisterNode(config), "Access denied without user token");
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckAccessDenied(RegisterNode(config.SetAuthToken("wrong_token")), "Could not find correct token validator");
    }
    {
        TKikimrServerForTestNodeRegistration serverDoesNotRequireToken({
            .EnforceUserToken = false,
        });
        ui16 grpc = serverDoesNotRequireToken.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        SetLogPriority(serverDoesNotRequireToken);

        TDriverConfig config;
        config.UseSecureConnection(caCert.Certificate.c_str())
            .SetEndpoint(location);

        CheckGood(RegisterNode(config));
        CheckGood(RegisterNode(config.SetAuthToken(BUILTIN_ACL_ROOT)));
        CheckGood(RegisterNode(config.SetAuthToken("wrong_token")));
    }
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
    TKikimrServerForTestNodeRegistration server({
        .EnforceUserToken = true,
        .EnableDynamicNodeAuth = true,
        .SetNodeAuthValues = true
    });
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

Y_UNIT_TEST(ServerWithCertVerification_ClientWithCorrectCerts_AccessDenied) {
    TKikimrServerForTestNodeRegistration server({
        .EnforceUserToken = true,
        .EnableDynamicNodeAuth = true,
        .SetNodeAuthValues = true,
        .RegisterNodeAllowedSids = {BUILTIN_ACL_ROOT}
    });
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(!resp->IsSuccess(), resp->GetErrorMessage());
    UNIT_ASSERT_STRINGS_EQUAL(resp->GetErrorMessage(), "Cannot authorize node. Access denied");

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

Y_UNIT_TEST(ServerWithCertVerification_ClientProvidesEmptyClientCerts) {
    TKikimrServerForTestNodeRegistration server({
        .EnforceUserToken = true,
        .EnableDynamicNodeAuth = true,
        .SetNodeAuthValues = true
    });
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

Y_UNIT_TEST(ServerWithoutCertVerification_ClientProvidesCorrectCerts) {
    TKikimrServerForTestNodeRegistration server({
        .EnforceUserToken = true,
    });
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
    TKikimrServerForTestNodeRegistration server({
        .EnforceUserToken = true,
    });
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
    TKikimrServerForTestNodeRegistration server({
        .EnforceUserToken = true,
        .EnableDynamicNodeAuth = true,
        .EnableWrongIdentity = true,
        .SetNodeAuthValues = true
    });
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    const TCertAndKey& caCert = TKikimrTestWithServerCert::GetCACertAndKey();
    const TCertAndKey clientServerCert = GenerateSignedCert(caCert, TProps::AsClientServer());

    NClient::TKikimr kikimr = GetKikimr(location, caCert, clientServerCert);

    Cerr << "Trying to register node" << Endl;

    auto resp = TryToRegisterDynamicNode(kikimr, "Root", "localhost", "localhost", "localhost", GetRandomPort());
    UNIT_ASSERT_C(!resp->IsSuccess(), resp->GetErrorMessage());
    UNIT_ASSERT_STRINGS_EQUAL(resp->GetErrorMessage(), "Cannot create token from certificate. Client certificate failed verification");

    Cerr << "Register node result " << resp->Record().ShortUtf8DebugString() << Endl;
}

}

} // namespace NKikimr
