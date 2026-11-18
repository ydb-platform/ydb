#include <ydb/core/fq/libs/common/external_service.h>

#include <ydb/library/protobuf_printer/security_printer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {
FederatedQuery::Connection TestServiceConnection(const TString& name = "service") {
    FederatedQuery::Connection connection;
    connection.mutable_meta()->set_id(name);
    connection.mutable_content()->set_name(name);
    auto* service = connection.mutable_content()->mutable_setting()->mutable_external_service();
    service->set_protocol(FederatedQuery::ExternalService::HTTP);
    service->set_endpoint("http://unused");
    service->set_insecure(true);
    service->mutable_auth()->mutable_none();
    return connection;
}

}   // namespace

Y_UNIT_TEST_SUITE(TExternalServiceConnection) {
    Y_UNIT_TEST(ExternalServiceValidationAndCredentialIsolation) {
        auto connection = TestServiceConnection("profiles");
        auto& service = *connection.mutable_content()->mutable_setting()->mutable_external_service();
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        service.set_insecure(false);
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.set_endpoint("https://service.test");
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        for (const auto& url : { "https://user:secret@service.test/profile", "https:///profile", "file:///etc/passwd",
                 "https://service.test/#fragment", "https://service.test/profile", "https://service.test/?q=1", "https://service.test/\r\n" }) {
            service.set_endpoint(url);
            UNIT_ASSERT_C(!NFq::ValidateExternalService(service, false).Empty(), url);
        }
        service.set_endpoint("https://service.test");
        service.mutable_auth()->mutable_current_iam();
        UNIT_ASSERT(!NFq::ValidateExternalService(service, true).Empty());
        service.mutable_auth()->mutable_token()->set_token("host-secret");
        (*service.mutable_headers())["X-Api-Key"] = "header-secret";
        const auto printed = NKikimr::SecureDebugString(connection);
        UNIT_ASSERT(!printed.Contains("host-secret"));
        UNIT_ASSERT(!printed.Contains("header-secret"));
        (*service.mutable_headers())["Authorization"] = "override";
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.clear_headers();
        service.mutable_auth()->mutable_service_account()->set_id("service-account");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.mutable_auth()->mutable_none();
        service.set_protocol(FederatedQuery::ExternalService::GRPC);
        service.set_endpoint("localhost:443");
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        service.set_endpoint("dns:///localhost:443");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
    }
}
