#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NRpcService {
    namespace {

        using NGRpcService::EPathInputOrigin;
        using NGRpcService::TPathRewriteSettings;
        using NPathAliasing::EPathRewriteOutcome;

        struct TTestRpc {
            using TRequest = Ydb::Scheme::MakeDirectoryRequest;
            using TResponse = Ydb::Scheme::MakeDirectoryResponse;
            static constexpr bool IsOp = true;
        };

        using TResponse = TTestRpc::TResponse;
        using TContext = TLocalRpcCtx<TTestRpc, std::function<void(const TResponse&)>>;

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

        void AssertPath(const NGRpcService::IRequestCtxBaseMtSafe& context, const TString& input,
                        const TString& expected, EPathRewriteOutcome outcome)
        {
            const auto result = context.NormalizePath(input);
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, expected);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(outcome));
        }

        TContext MakeContext(const TString& database, bool internalCall = false) {
            return TContext(TTestRpc::TRequest{}, [](const TResponse&) {}, database, Nothing(), Nothing(), internalCall);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(LocalRpcPathAliasing) {
        Y_UNIT_TEST(DisabledRulesRetainNoAllocatedNamespaceContext) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            auto context = MakeContext("//Root/tenant/");
            context.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            UNIT_ASSERT(context.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT(!context.HasActivePathRewriting());
            UNIT_ASSERT(!context.GetPathRewriteSettings().Context);
            UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabaseName(), "//Root/tenant/");
            UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabaseName(), "//Root/tenant/");
            AssertPath(context, "//Root//Table/", "//Root//Table/", EPathRewriteOutcome::NoMatch);
        }

        Y_UNIT_TEST(DefaultLocalCallHasPhysicalInputsEvenWhenInternalCallIsFalse) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto context = MakeContext("/Root");
            UNIT_ASSERT(!context.IsInternalCall());
            UNIT_ASSERT(context.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabaseName(), "/Root");
            AssertPath(context, "/Root/Table", "/Root/Table", EPathRewriteOutcome::NoMatch);
        }

        Y_UNIT_TEST(ExplicitUserOriginIsIndependentOfInternalCallFlag) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto context = MakeContext("/alias", true);
            context.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            UNIT_ASSERT(context.IsInternalCall());
            UNIT_ASSERT(context.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabaseName(), "/alias");
            UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabaseName(), "/Root");
            UNIT_ASSERT(context.GetPathRewriteSettings().Context);
            AssertPath(context, "/alias/Table", "/Root/Table", EPathRewriteOutcome::Rewritten);
        }

        Y_UNIT_TEST(UnspecifiedInputsAreNotPromotedToUserInputs) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto context = MakeContext("/alias");
            context.SetPathRewriteSettings({});
            UNIT_ASSERT(context.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT(!context.GetPathRewriteSettings().Context);
            UNIT_ASSERT(!context.HasActivePathRewriting());
            UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabaseName(), "/alias");
            AssertPath(context, "/alias/Table", "/alias/Table", EPathRewriteOutcome::NoMatch);
        }

        Y_UNIT_TEST(RepeatedInitializationRetainsCapturedNamespace) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto context = MakeContext("/alias");
            context.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            UNIT_ASSERT(context.InitializePathRewriteContext(app).empty());
            const auto captured = context.GetPathRewriteSettings().Context;
            app.PathNormalizer = Rules("/Different");
            UNIT_ASSERT(context.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT(context.GetPathRewriteSettings().Context == captured);
            UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabaseName(), "/Root");
            AssertPath(context, "/alias/Table", "/Root/Table", EPathRewriteOutcome::Rewritten);
        }

        Y_UNIT_TEST(ForwardedPhysicalDatabaseRetainsLogicalResourceContext) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto source = MakeContext("/alias");
            source.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            UNIT_ASSERT(source.InitializePathRewriteContext(app).empty());
            auto settings = source.GetPathRewriteSettings();
            settings.Database = EPathInputOrigin::Resolved;
            auto forwarded = MakeContext("/Root");
            forwarded.SetPathRewriteSettings(settings);
            UNIT_ASSERT(forwarded.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(*forwarded.GetDatabaseName(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(*forwarded.GetLogicalDatabaseName(), "/alias");
            AssertPath(forwarded, "/alias/Table", "/Root/Table", EPathRewriteOutcome::Rewritten);
            settings.Resources = EPathInputOrigin::Resolved;
            forwarded.SetPathRewriteSettings(settings);
            AssertPath(forwarded, "/Root/Table", "/Root/Table", EPathRewriteOutcome::NoMatch);
        }

        Y_UNIT_TEST(ResolvedForwardingDatabaseIsNotReplacedByCapturedDatabase) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto source = MakeContext("/alias/stream");
            source.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            UNIT_ASSERT(source.InitializePathRewriteContext(app).empty());
            auto settings = source.GetPathRewriteSettings();
            settings.Database = EPathInputOrigin::Resolved;
            auto forwarded = MakeContext("/Root");
            forwarded.SetPathRewriteSettings(settings);
            UNIT_ASSERT(forwarded.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT_VALUES_EQUAL(*forwarded.GetDatabaseName(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(*forwarded.GetLogicalDatabaseName(), "/alias/stream");
            AssertPath(forwarded, "/alias/stream/Table", "/Root/stream/Table", EPathRewriteOutcome::Rewritten);
        }

        Y_UNIT_TEST(StreamingWrapperKeepsInnerNamespaceWithoutRewritingDatabaseAgain) {
            using TStreamRpc = NGRpcService::TGrpcRequestNoOperationCall<
                Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse>;
            using TStreamContext = TLocalRpcCtx<TStreamRpc,
                                                std::function<void(const Ydb::Table::ReadTableResponse&)>>;
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto inner = std::make_shared<TStreamContext>(Ydb::Table::ReadTableRequest{},
                                                          [](const Ydb::Table::ReadTableResponse&) {}, "/alias", Nothing(), Nothing(), false);
            inner->SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            UNIT_ASSERT(inner->InitializePathRewriteContext(app).empty());
            auto stream = MakeIntrusive<TStreamReadProcessor<Ydb::Table::ReadTableResponse>>(inner);
            TStreamRpc outer(stream.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>,
                                              const NGRpcService::IFacilityProvider&) {});
            outer.SetPathRewriteSettings(inner->GetPathRewriteSettings());
            UNIT_ASSERT(outer.InitializePathRewriteContext(app).empty());
            UNIT_ASSERT(outer.GetPathRewriteSettings().Context == inner->GetPathRewriteSettings().Context);
            UNIT_ASSERT_VALUES_EQUAL(*outer.GetDatabaseName(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(*outer.GetLogicalDatabaseName(), "/alias");
            AssertPath(outer, "/alias/Table", "/Root/Table", EPathRewriteOutcome::Rewritten);
        }

        Y_UNIT_TEST(InvalidMixedOriginCannotBecomeValidThroughRetryOrCopy) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules();
            auto settings = TPathRewriteSettings::UserInput();
            settings.Database = EPathInputOrigin::Resolved;
            auto context = MakeContext("/Root");
            context.SetPathRewriteSettings(settings);
            const auto error = context.InitializePathRewriteContext(app);
            UNIT_ASSERT(!error.empty());
            UNIT_ASSERT_VALUES_EQUAL(context.InitializePathRewriteContext(app), error);
            auto forwarded = MakeContext("/Root");
            forwarded.SetPathRewriteSettings(context.GetPathRewriteSettings());
            UNIT_ASSERT_VALUES_EQUAL(forwarded.InitializePathRewriteContext(app), error);
        }

        Y_UNIT_TEST(InvalidDatabaseContextRetainsErrorOnRetryAndCopy) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules("relative");
            auto context = MakeContext("/alias");
            context.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
            const auto error = context.InitializePathRewriteContext(app);
            UNIT_ASSERT(!error.empty());
            UNIT_ASSERT_VALUES_EQUAL(context.InitializePathRewriteContext(app), error);
            auto forwarded = MakeContext("/alias");
            forwarded.SetPathRewriteSettings(context.GetPathRewriteSettings());
            UNIT_ASSERT_VALUES_EQUAL(forwarded.InitializePathRewriteContext(app), error);
        }

        Y_UNIT_TEST(InvalidDatabaseProducesOneOrdinaryResponseBeforeDispatch) {
            TAppData app(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
            app.PathNormalizer = Rules("relative");
            size_t responses = 0;
            {
                TContext context(TTestRpc::TRequest{}, [&](const TResponse& response) {
                    ++responses;
                    UNIT_ASSERT(response.operation().ready());
                    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
                    UNIT_ASSERT(response.operation().issues_size() > 0);
                }, "/alias", Nothing(), Nothing(), false);
                UNIT_ASSERT(!PrepareLocalPathContext(context, app, TPathRewriteSettings::UserInput()));
                UNIT_ASSERT_VALUES_EQUAL(responses, 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(responses, 1);
        }
    } // Y_UNIT_TEST_SUITE(LocalRpcPathAliasing)

} // namespace NKikimr::NRpcService
