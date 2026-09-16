#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NHttpProxy {
    namespace {

        NHttp::THttpIncomingRequestPtr SignedRequest() {
            const TString body = R"({"StreamName":"/alias/Topic","Data":"L2FsaWFzL3ZhbHVl"})";
            const TString bytes = TStringBuilder()
                                  << "POST /alias?name=%2Falias%2Fvalue HTTP/1.1\r\n"
                                  << "Host: example.amazonaws.com\r\n"
                                  << "Content-Type: application/x-amz-json-1.1\r\n"
                                  << "X-Amz-Target: Kinesis_20131202.PutRecord\r\n"
                                  << "X-Amz-Date: 20150830T123600Z\r\n"
                                  << "Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/kinesis/aws4_request, "
                                  << "SignedHeaders=host;x-amz-date, Signature=00000000\r\n"
                                  << "Content-Length: " << body.size() << "\r\n\r\n"
                                  << body;
            auto request = MakeIntrusive<NHttp::THttpIncomingRequest>(bytes,
                                                                      std::make_shared<NHttp::THttpEndpointInfo>(), NHttp::THttpConfig::SocketAddressType{});
            UNIT_ASSERT(request->IsReady());
            return request;
        }

        std::shared_ptr<const NPathAliasing::TPathNormalizer> Rules(const TString& replacement = "/Root") {
            NKikimrConfig::TPathRewriteConfig config;
            auto* alias = config.AddRules();
            alias->SetPattern("^/alias");
            alias->SetReplacement(replacement);
            auto* decoy = config.AddRules();
            decoy->SetPattern("^/Root");
            decoy->SetReplacement("/Decoy");
            return std::make_shared<NPathAliasing::TPathNormalizer>(config);
        }

        struct TRpc {
            using TRequest = Ydb::Scheme::MakeDirectoryRequest;
            using TResponse = Ydb::Scheme::MakeDirectoryResponse;
            static constexpr bool IsOp = true;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(HttpPathAliasingContext) {
        Y_UNIT_TEST(SignaturesAndOriginalHttpBytesAreUnchanged) {
            NKikimrConfig::TServerlessProxyConfig config;
            auto request = SignedRequest();
            THttpRequestContext context(config, request, {}, nullptr, {});
            const TString url(request->URL);
            const TString headers(request->Headers);
            const TString body(request->Body);
            const auto before = context.GetSignature();
            UNIT_ASSERT(before && !before->Empty());
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/Root");
            UNIT_ASSERT_VALUES_EQUAL(*context.PathRewrite.Context->GetLogicalDatabase(), "/alias");
            const auto after = context.GetSignature();
            UNIT_ASSERT(after && !after->Empty());
            UNIT_ASSERT_VALUES_EQUAL(after->GetCanonicalRequest(), before->GetCanonicalRequest());
            UNIT_ASSERT_VALUES_EQUAL(after->GetStringToSign(), before->GetStringToSign());
            UNIT_ASSERT_VALUES_EQUAL(request->URL, url);
            UNIT_ASSERT_VALUES_EQUAL(request->Headers, headers);
            UNIT_ASSERT_VALUES_EQUAL(request->Body, body);
        }

        Y_UNIT_TEST(DisabledRulesPreserveOriginalDatabaseWithoutAllocatingContext) {
            NKikimrConfig::TServerlessProxyConfig config;
            THttpRequestContext context(config, SignedRequest(), {}, nullptr, {});
            context.DatabasePath = "//Root//tenant/";
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "//Root//tenant/");
            UNIT_ASSERT(!context.PathRewrite.Context);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(context.PathRewrite.Database),
                                     static_cast<int>(NGRpcService::EPathInputOrigin::Resolved));
        }

        Y_UNIT_TEST(InvalidDatabaseRemainsInvalidOnRepeatedInitialization) {
            NKikimrConfig::TServerlessProxyConfig config;
            THttpRequestContext context(config, SignedRequest(), {}, nullptr, {});
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules("relative");
            const auto error = context.InitializePathRewriting(app);
            UNIT_ASSERT(!error.empty());
            UNIT_ASSERT_VALUES_EQUAL(context.InitializePathRewriting(app), error);
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/alias");
        }

        Y_UNIT_TEST(TokenDerivedPhysicalDatabaseIsNotRematched) {
            NKikimrConfig::TServerlessProxyConfig config;
            THttpRequestContext context(config, SignedRequest(), {}, nullptr, {});
            context.DatabasePath = "/Root/Topic";
            context.PathRewrite = NGRpcService::TPathRewriteSettings::Internal();
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/Root/Topic");
            UNIT_ASSERT(!context.PathRewrite.Context);
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/Root/Topic");
        }

        Y_UNIT_TEST(LocalInitializationDoesNotInferRouteFromDriverPointer) {
            NKikimrConfig::TServerlessProxyConfig config;
            NYdb::TDriver driver(NYdb::TDriverConfig{});
            THttpRequestContext context(config, SignedRequest(), {}, &driver, {});
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/Root");
            UNIT_ASSERT(context.PathRewrite.Context);
            driver.Stop(true);
        }

        Y_UNIT_TEST(AuthDerivedDatabaseKeepsOriginalResourceNamespaceAtLocalRpcHandoff) {
            NKikimrConfig::TServerlessProxyConfig config;
            THttpRequestContext context(config, SignedRequest(), {}, nullptr, {});
            context.DatabasePath = "/alias/Topic"; // Database inferred from the raw stream operand.
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/Root/Topic");
            context.DatabasePath = "/Root"; // Auth navigation found the owning physical database.
            UNIT_ASSERT(context.InitializePathRewriting(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(context.DatabasePath, "/Root");
            NRpcService::TLocalRpcCtx<TRpc, std::function<void(const TRpc::TResponse&)>> forwarded(
                TRpc::TRequest{}, [](const TRpc::TResponse&) {}, context.DatabasePath, Nothing(), Nothing(), false);
            forwarded.SetPathRewriteSettings(context.PathRewrite);
            UNIT_ASSERT(forwarded.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(*forwarded.GetDatabaseName(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(*forwarded.GetLogicalDatabaseName(), "/alias/Topic");
            const auto resolved = forwarded.NormalizePath("/alias/Topic");
            UNIT_ASSERT(resolved.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(resolved->Path, "/Root/Topic");
        }
    } // Y_UNIT_TEST_SUITE(HttpPathAliasingContext)

} // namespace NKikimr::NHttpProxy
