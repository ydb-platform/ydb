#include <ydb/services/ydb/ydb_common_ut.h>

#include <ydb/public/sdk/cpp/client/ydb_coordination/coordination.h>
#include <ydb/public/sdk/cpp/client/ydb_rate_limiter/rate_limiter.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <ydb/core/grpc_services/local_rate_limiter.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

namespace NKikimr {

using NYdb::NRateLimiter::TCreateResourceSettings;

namespace {

void SetDuration(const TDuration& duration, google::protobuf::Duration& protoValue) {
    protoValue.set_seconds(duration.Seconds());
    protoValue.set_nanos(duration.NanoSecondsOfSecond());
}

TString PrintStatus(const NYdb::TStatus& status) {
    TStringBuilder builder;
    builder << status.GetStatus();
    const auto& issues = status.GetIssues();
    if (issues) {
        builder << ":\n";
        issues.PrintTo(builder.Out);
    }
    return builder;
}

#define ASSERT_STATUS_C(expr, status, comment) {                                            \
        const auto Y_CAT(resultFuture, __LINE__) = (expr);                                  \
        const auto Y_CAT(result, __LINE__) = Y_CAT(resultFuture, __LINE__).GetValueSync();  \
        UNIT_ASSERT_VALUES_EQUAL_C(Y_CAT(result, __LINE__).GetStatus(), status,             \
                      Y_STRINGIZE(expr) " has status different from expected one. Error description: " << PrintStatus(Y_CAT(result, __LINE__)) \
                      << " " << comment);                                                   \
    }

#define ASSERT_STATUS(expr, status) ASSERT_STATUS_C(expr, status, "")

#define ASSERT_STATUS_SUCCESS_C(expr, comment) ASSERT_STATUS_C(expr, NYdb::EStatus::SUCCESS, comment)

#define ASSERT_STATUS_SUCCESS(expr) ASSERT_STATUS_SUCCESS_C(expr, "")

class TTestSetup {
public:
    TTestSetup()
        : Driver(MakeDriverConfig())
        , CoordinationClient(Driver)
        , RateLimiterClient(Driver)
    {
        CreateCoordinationNode();
    }

    virtual ~TTestSetup() = default;

    NYdb::TDriverConfig MakeDriverConfig() {
        return NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << Server.GetPort());
    }

    void CreateCoordinationNode(const TString& path = CoordinationNodePath) {
        ASSERT_STATUS_SUCCESS_C(CoordinationClient.CreateNode(path), "\nPath: " << path);
    }

    void virtual CheckAcquireResource(const TString& coordinationNodePath, const TString& resourcePath, const NYdb::NRateLimiter::TAcquireResourceSettings& settings, NYdb::EStatus expected) {
        const auto acquireResultFuture = RateLimiterClient.AcquireResource(coordinationNodePath, resourcePath, settings);
        ASSERT_STATUS(acquireResultFuture, expected);
    }

    static TString CoordinationNodePath;

    NYdb::TKikimrWithGrpcAndRootSchema Server;
    NYdb::TDriver Driver;
    NYdb::NCoordination::TClient CoordinationClient;
    NYdb::NRateLimiter::TRateLimiterClient RateLimiterClient;
};

class TTestSetupAcquireActor : public TTestSetup {
private:
    class TAcquireActor : public TActorBootstrapped<TAcquireActor> {
    public:
        TAcquireActor(
            const TString& coordinationNodePath,
            const TString& resourcePath,
            const NYdb::NRateLimiter::TAcquireResourceSettings& settings,
            NThreading::TPromise<NYdb::TStatus> promise)
        : CoordinationNodePath(coordinationNodePath)
        , ResourcePath(resourcePath)
        , Settings(settings)
        , Promise(promise)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateWork);
            Ydb::RateLimiter::AcquireResourceRequest request;
            request.set_coordination_node_path(CoordinationNodePath);
            request.set_resource_path(ResourcePath);

            SetDuration(Settings.OperationTimeout_, *request.mutable_operation_params()->mutable_operation_timeout());
            if (Settings.CancelAfter_) {
                SetDuration(Settings.CancelAfter_, *request.mutable_operation_params()->mutable_cancel_after());
            }

            if (Settings.IsUsedAmount_) {
                request.set_used(Settings.Amount_.GetRef());
            } else {
                request.set_required(Settings.Amount_.GetRef());
            }

            auto id = SelfId();

            auto cb = [this, id](Ydb::RateLimiter::AcquireResourceResponse resp) {
                NYql::TIssues opIssues;
                NYql::IssuesFromMessage(resp.operation().issues(), opIssues);
                NYdb::TStatus status(static_cast<NYdb::EStatus>(resp.operation().status()), std::move(opIssues));
                Promise.SetValue(status);
                Send(id, new TEvents::TEvPoisonPill);
            };

            NKikimr::NRpcService::RateLimiterAcquireUseSameMailbox(std::move(request), "", "", std::move(cb), ctx);
        }

        STATEFN(StateWork) {
            Y_UNUSED(ev);
        }

    private:
        const TString CoordinationNodePath;
        const TString ResourcePath;
        const NYdb::NRateLimiter::TAcquireResourceSettings Settings;
        NThreading::TPromise<NYdb::TStatus> Promise;
    };
public:
    void virtual CheckAcquireResource(const TString& coordinationNodePath, const TString& resourcePath, const NYdb::NRateLimiter::TAcquireResourceSettings& settings, NYdb::EStatus expected) {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        auto actor = new TAcquireActor(coordinationNodePath, resourcePath, settings, promise);
        Server.GetRuntime()->GetAnyNodeActorSystem()->Register(actor);
        ASSERT_STATUS(promise.GetFuture(), expected);
    }
};

TString TTestSetup::CoordinationNodePath = "/Root/CoordinationNode";

} // namespace

Y_UNIT_TEST_SUITE(TGRpcRateLimiterTest) {
    Y_UNIT_TEST(CreateResource) {
        TTestSetup setup;
        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "res",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(1).MaxBurstSizeCoefficient(42)));

        {
            const auto describeResultFuture = setup.RateLimiterClient.DescribeResource(TTestSetup::CoordinationNodePath, "res");
            ASSERT_STATUS_SUCCESS(describeResultFuture);
            const auto describeResult = describeResultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResourcePath(), "res");
            UNIT_ASSERT_VALUES_EQUAL(*describeResult.GetHierarchicalDrrProps().GetMaxUnitsPerSecond(), 1);
            UNIT_ASSERT_VALUES_EQUAL(*describeResult.GetHierarchicalDrrProps().GetMaxBurstSizeCoefficient(), 42);
        }

        // Fail - not canonized path
        ASSERT_STATUS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "/res/res2"), NYdb::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(UpdateResource) {
        using NYdb::NRateLimiter::TAlterResourceSettings;

        TTestSetup setup;
        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "res",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(100500).MaxBurstSizeCoefficient(2)));

        {
            const auto describeResultFuture = setup.RateLimiterClient.DescribeResource(TTestSetup::CoordinationNodePath, "res");
            ASSERT_STATUS_SUCCESS(describeResultFuture);
            const auto describeResult = describeResultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResourcePath(), "res");
            UNIT_ASSERT_VALUES_EQUAL(*describeResult.GetHierarchicalDrrProps().GetMaxUnitsPerSecond(), 100500); // previous value
            UNIT_ASSERT_VALUES_EQUAL(*describeResult.GetHierarchicalDrrProps().GetMaxBurstSizeCoefficient(), 2); // previous value
        }

        // Fail - negative max units per second
        ASSERT_STATUS(setup.RateLimiterClient.AlterResource(TTestSetup::CoordinationNodePath, "res",
                                                            TAlterResourceSettings().MaxUnitsPerSecond(-1)), NYdb::EStatus::BAD_REQUEST);

        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.AlterResource(TTestSetup::CoordinationNodePath, "res",
                                                                    TAlterResourceSettings().MaxUnitsPerSecond(100)));

        {
            const auto describeResultFuture = setup.RateLimiterClient.DescribeResource(TTestSetup::CoordinationNodePath, "res");
            ASSERT_STATUS_SUCCESS(describeResultFuture);
            const auto describeResult = describeResultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResourcePath(), "res");
            UNIT_ASSERT_VALUES_EQUAL(*describeResult.GetHierarchicalDrrProps().GetMaxUnitsPerSecond(), 100); // applied
            UNIT_ASSERT(!describeResult.GetHierarchicalDrrProps().GetMaxBurstSizeCoefficient());
        }
    }

    Y_UNIT_TEST(DropResource) {
        TTestSetup setup;
        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "res",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(100500)));

        // Fail - not found
        ASSERT_STATUS(setup.RateLimiterClient.DropResource(TTestSetup::CoordinationNodePath, "non_existent_resource"), NYdb::EStatus::NOT_FOUND);

        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.DropResource(TTestSetup::CoordinationNodePath, "res"));

        {
            const auto listResultFuture = setup.RateLimiterClient.ListResources(TTestSetup::CoordinationNodePath, "");
            ASSERT_STATUS_SUCCESS(listResultFuture);
            const auto listResult = listResultFuture.GetValueSync();
            UNIT_ASSERT(listResult.GetResourcePaths().empty());
        }
    }

    Y_UNIT_TEST(DescribeResource) {
        TTestSetup setup;
        // Fail - not found
        ASSERT_STATUS(setup.RateLimiterClient.DropResource(TTestSetup::CoordinationNodePath, "no_resource"), NYdb::EStatus::NOT_FOUND);

        ASSERT_STATUS(setup.RateLimiterClient.DropResource(TTestSetup::CoordinationNodePath, "not//canonized/resource/path"), NYdb::EStatus::BAD_REQUEST);

        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "resource",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(100500)));
        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "resource/child"));

        {
            const auto describeResultFuture = setup.RateLimiterClient.DescribeResource(TTestSetup::CoordinationNodePath, "resource");
            ASSERT_STATUS_SUCCESS(describeResultFuture);
            const auto describeResult = describeResultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResourcePath(), "resource");
            UNIT_ASSERT_VALUES_EQUAL(*describeResult.GetHierarchicalDrrProps().GetMaxUnitsPerSecond(), 100500);
            UNIT_ASSERT(!describeResult.GetHierarchicalDrrProps().GetMaxBurstSizeCoefficient());
        }

        {
            const auto describeResultFuture = setup.RateLimiterClient.DescribeResource(TTestSetup::CoordinationNodePath, "resource/child");
            ASSERT_STATUS_SUCCESS(describeResultFuture);
            const auto describeResult = describeResultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResourcePath(), "resource/child");
            UNIT_ASSERT(!describeResult.GetHierarchicalDrrProps().GetMaxUnitsPerSecond());
            UNIT_ASSERT(!describeResult.GetHierarchicalDrrProps().GetMaxBurstSizeCoefficient());
        }
    }

    Y_UNIT_TEST(ListResources) {
        using NYdb::NRateLimiter::TListResourcesSettings;

        TTestSetup setup;
        // Fail - not found
        ASSERT_STATUS(setup.RateLimiterClient.ListResources(TTestSetup::CoordinationNodePath, "no_resource"), NYdb::EStatus::NOT_FOUND);
        ASSERT_STATUS(setup.RateLimiterClient.ListResources(TTestSetup::CoordinationNodePath, "no_resource", TListResourcesSettings().Recursive()), NYdb::EStatus::NOT_FOUND);

        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "parent1",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(100500)));

        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "parent2",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(100500)));

        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "parent1/child1"));
        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "parent1/child2"));
        ASSERT_STATUS_SUCCESS(setup.RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "parent2/child"));

        // All
        {
            const auto listResultFuture = setup.RateLimiterClient.ListResources(TTestSetup::CoordinationNodePath, "", TListResourcesSettings().Recursive(true));
            ASSERT_STATUS_SUCCESS(listResultFuture);
            const auto listResult = listResultFuture.GetValueSync();
            TVector<TString> paths = listResult.GetResourcePaths();
            std::sort(paths.begin(), paths.end());
            UNIT_ASSERT_VALUES_EQUAL(paths.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(paths[0], "parent1");
            UNIT_ASSERT_VALUES_EQUAL(paths[1], "parent1/child1");
            UNIT_ASSERT_VALUES_EQUAL(paths[2], "parent1/child2");
            UNIT_ASSERT_VALUES_EQUAL(paths[3], "parent2");
            UNIT_ASSERT_VALUES_EQUAL(paths[4], "parent2/child");
        }

        // All roots
        {
            const auto listResultFuture = setup.RateLimiterClient.ListResources(TTestSetup::CoordinationNodePath, "", TListResourcesSettings().Recursive(false));
            ASSERT_STATUS_SUCCESS(listResultFuture);
            const auto listResult = listResultFuture.GetValueSync();
            TVector<TString> paths = listResult.GetResourcePaths();
            std::sort(paths.begin(), paths.end());
            UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(paths[0], "parent1");
            UNIT_ASSERT_VALUES_EQUAL(paths[1], "parent2");
        }

        // All children of parent1
        {
            const auto listResultFuture = setup.RateLimiterClient.ListResources(TTestSetup::CoordinationNodePath, "parent1", TListResourcesSettings().Recursive());
            ASSERT_STATUS_SUCCESS(listResultFuture);
            const auto listResult = listResultFuture.GetValueSync();
            TVector<TString> paths = listResult.GetResourcePaths();
            std::sort(paths.begin(), paths.end());
            UNIT_ASSERT_VALUES_EQUAL(paths.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(paths[0], "parent1");
            UNIT_ASSERT_VALUES_EQUAL(paths[1], "parent1/child1");
            UNIT_ASSERT_VALUES_EQUAL(paths[2], "parent1/child2");
        }
    }

    std::unique_ptr<TTestSetup> MakeTestSetup(bool useActorApi) {
        if (useActorApi) {
            return std::make_unique<TTestSetupAcquireActor>();
        }
        return std::make_unique<TTestSetup>();
    }

    void AcquireResourceManyRequired(bool useActorApi, bool useCancelAfter) {
        const TDuration operationTimeout = useCancelAfter ? TDuration::Hours(1) : TDuration::MilliSeconds(200);
        const TDuration cancelAfter = useCancelAfter ? TDuration::MilliSeconds(200) : TDuration::Zero(); // 0 means that parameter is not set

        using NYdb::NRateLimiter::TAcquireResourceSettings;

        auto setup = MakeTestSetup(useActorApi);

        ASSERT_STATUS_SUCCESS(setup->RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "res",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(1).MaxBurstSizeCoefficient(42)));

        setup->CheckAcquireResource(TTestSetup::CoordinationNodePath, "res", TAcquireResourceSettings().Amount(10000).OperationTimeout(operationTimeout).CancelAfter(cancelAfter), NYdb::EStatus::SUCCESS);

        for (int i = 0; i < 3; ++i) {
            setup->CheckAcquireResource(TTestSetup::CoordinationNodePath, "res", TAcquireResourceSettings().Amount(1).OperationTimeout(operationTimeout).CancelAfter(cancelAfter), useCancelAfter ? NYdb::EStatus::CANCELLED : NYdb::EStatus::TIMEOUT);
            setup->CheckAcquireResource(TTestSetup::CoordinationNodePath, "res", TAcquireResourceSettings().Amount(1).IsUsedAmount(true).OperationTimeout(operationTimeout).CancelAfter(cancelAfter), NYdb::EStatus::SUCCESS);
        }
    }

    void AcquireResourceManyUsed(bool useActorApi, bool useCancelAfter) {
        const TDuration operationTimeout = useCancelAfter ? TDuration::Hours(1) : TDuration::MilliSeconds(200);
        const TDuration cancelAfter = useCancelAfter ? TDuration::MilliSeconds(200) : TDuration::Zero(); // 0 means that parameter is not set

        using NYdb::NRateLimiter::TAcquireResourceSettings;

        auto setup = MakeTestSetup(useActorApi);
        ASSERT_STATUS_SUCCESS(setup->RateLimiterClient.CreateResource(TTestSetup::CoordinationNodePath, "res",
                                                                     TCreateResourceSettings().MaxUnitsPerSecond(1).MaxBurstSizeCoefficient(42)));

        setup->CheckAcquireResource(TTestSetup::CoordinationNodePath, "res", TAcquireResourceSettings().Amount(10000).IsUsedAmount(true).OperationTimeout(operationTimeout).CancelAfter(cancelAfter), NYdb::EStatus::SUCCESS);
        for (int i = 0; i < 3; ++i) {
            setup->CheckAcquireResource(TTestSetup::CoordinationNodePath, "res", TAcquireResourceSettings().Amount(1).OperationTimeout(operationTimeout).CancelAfter(cancelAfter), useCancelAfter ? NYdb::EStatus::CANCELLED : NYdb::EStatus::TIMEOUT);
            setup->CheckAcquireResource(TTestSetup::CoordinationNodePath, "res", TAcquireResourceSettings().Amount(1).IsUsedAmount(true).OperationTimeout(operationTimeout).CancelAfter(cancelAfter), NYdb::EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(AcquireResourceManyRequiredGrpcApi) {
        AcquireResourceManyRequired(false, false);
    }

    Y_UNIT_TEST(AcquireResourceManyRequiredActorApi) {
        AcquireResourceManyRequired(true, false);
    }

    Y_UNIT_TEST(AcquireResourceManyRequiredGrpcApiWithCancelAfter) {
        AcquireResourceManyRequired(false, true);
    }

    Y_UNIT_TEST(AcquireResourceManyRequiredActorApiWithCancelAfter) {
        AcquireResourceManyRequired(true, true);
    }

    Y_UNIT_TEST(AcquireResourceManyUsedGrpcApi) {
        AcquireResourceManyUsed(false, false);
    }

    Y_UNIT_TEST(AcquireResourceManyUsedActorApi) {
        AcquireResourceManyUsed(true, false);
    }

    Y_UNIT_TEST(AcquireResourceManyUsedGrpcApiWithCancelAfter) {
        AcquireResourceManyUsed(false, true);
    }

    Y_UNIT_TEST(AcquireResourceManyUsedActorApiWithCancelAfter) {
        AcquireResourceManyUsed(true, true);
    }
}

} // namespace NKikimr
